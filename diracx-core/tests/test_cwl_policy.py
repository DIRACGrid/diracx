from __future__ import annotations

import pytest

from diracx.core.cwl import PolicyViolationError, check_policy, decompose


def tool(**extra) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": ["echo"],
        "inputs": {},
        "outputs": {},
        **extra,
    }


def test_supported_requirements_and_any_hint_are_accepted():
    check_policy(
        decompose(
            tool(
                requirements={"InlineJavascriptRequirement": {}},
                hints={"DockerRequirement": {"dockerPull": "alpine"}},
            )
        )[1]
    )


def test_unsupported_requirements_are_refused_wherever_they_are():
    workflow = {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "inputs": {},
        "outputs": {},
        "steps": {
            "s0": {
                "run": tool(requirements=[{"class": "InplaceUpdateRequirement"}]),
                "requirements": {"SoftwareRequirement": {"packages": []}},
                "in": {},
                "out": [],
            }
        },
    }
    with pytest.raises(PolicyViolationError) as e:
        check_policy(decompose(workflow)[1])
    assert "InplaceUpdateRequirement" in str(e.value)
    assert "SoftwareRequirement" in str(e.value)


def test_oversized_processes_are_refused():
    with pytest.raises(PolicyViolationError, match="sandbox"):
        check_policy(decompose(tool(arguments=["x" * 1000]))[1], max_process_bytes=500)


def test_dirac_requirements_are_refused_but_dirac_hints_are_not():
    check_policy(decompose(tool(hints={"dirac:Transformation": {"group_size": 4}}))[1])
    with pytest.raises(PolicyViolationError, match="dirac:Unsupported"):
        check_policy(decompose(tool(requirements={"dirac:Unsupported": {}}))[1])


def test_abstract_operations_are_refused_wherever_they_are():
    operation = {
        "cwlVersion": "v1.2",
        "class": "Operation",
        "inputs": {"x": "File"},
        "outputs": {"y": "File"},
    }
    workflow = {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "inputs": {"x": "File"},
        "outputs": {"y": {"type": "File", "outputSource": "s0/y"}},
        "steps": {"s0": {"run": operation, "in": {"x": "x"}, "out": ["y"]}},
    }
    for process in (operation, workflow):
        with pytest.raises(PolicyViolationError, match="is an Operation") as e:
            check_policy(decompose(process)[1])
        assert "CommandLineTool, ExpressionTool or Workflow" in str(e.value)


def test_docker_images_are_accepted_when_pulled():
    for image in (
        "harbor.cta-observatory.org/ctapipe:0.24",
        "alpine@sha256:" + "a" * 64,
    ):
        check_policy(
            decompose(tool(requirements={"DockerRequirement": {"dockerPull": image}}))[
                1
            ]
        )
    check_policy(
        decompose(
            tool(requirements={"DockerRequirement": {"dockerImageId": "ctapipe"}})
        )[1]
    )


@pytest.mark.parametrize("field", ["dockerFile", "dockerImport", "dockerLoad"])
@pytest.mark.parametrize("where", ["requirements", "hints"])
def test_docker_images_built_or_fetched_on_the_worker_are_refused(field, where):
    docker = {"DockerRequirement": {"dockerImageId": "x", field: "x"}}
    with pytest.raises(PolicyViolationError, match=f"DockerRequirement {field}") as e:
        check_policy(decompose(tool(**{where: docker}))[1])
    assert "dockerPull" in str(e.value)
