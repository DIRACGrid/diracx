from __future__ import annotations

import json
from pathlib import Path

import pytest

from diracx.core.cwl import PolicyViolationError, check_policy
from diracx.core.cwl.policy import MAX_DOCUMENT_BYTES, MAX_PROCESS_BYTES

FIXTURE = Path(__file__).parent / "fixtures" / "mc-simulation.packed.json"


def tool(**extra) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "id": "#main",
        "baseCommand": ["echo"],
        "inputs": [],
        "outputs": [],
        **extra,
    }


def workflow(run: dict, **step) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "id": "#main",
        "inputs": [],
        "outputs": [],
        "steps": [{"id": "#main/s0", "run": run, "in": [], "out": [], **step}],
    }


def test_the_mc_simulation_workgraph_is_accepted():
    check_policy(json.loads(FIXTURE.read_text()))


def test_supported_requirements_and_hints_are_accepted_in_either_form():
    check_policy(
        tool(
            requirements=[{"class": "InlineJavascriptRequirement"}],
            hints={"DockerRequirement": {"dockerPull": "alpine"}},
        )
    )


def test_unsupported_requirements_are_refused_wherever_they_are():
    document = workflow(
        tool(requirements=[{"class": "InplaceUpdateRequirement"}]),
        requirements=[{"class": "SoftwareRequirement", "packages": []}],
    )
    with pytest.raises(PolicyViolationError) as e:
        check_policy(document)
    assert "inline process of step #main/s0 requires InplaceUpdateRequirement" in str(
        e.value
    )
    assert "#main requires SoftwareRequirement" in str(e.value)


def test_unknown_hints_are_refused():
    check_policy(
        tool(
            hints=[
                {"class": "SoftwareRequirement", "packages": []},
                {"class": "InplaceUpdateRequirement", "inplaceUpdate": True},
            ]
        )
    )
    with pytest.raises(PolicyViolationError, match="http://example.org/cwl#Thing"):
        check_policy(tool(hints=[{"class": "http://example.org/cwl#Thing"}]))


def test_dirac_classes_are_hints_only():
    cls = "https://diracgrid.org/cwl#Transformation"
    check_policy(tool(hints=[{"class": cls, "group_size": 4}]))
    with pytest.raises(PolicyViolationError, match="requires https://diracgrid.org"):
        check_policy(tool(requirements=[{"class": cls}]))


def test_oversized_processes_are_refused():
    with pytest.raises(PolicyViolationError, match="#main is .* bytes.*sandbox"):
        check_policy(tool(arguments=["x" * MAX_PROCESS_BYTES]))


def test_oversized_documents_are_refused():
    # Each entry is within its own limit, but not all of them together
    entries = [
        tool(id=f"#t{i}.cwl", arguments=["x" * (MAX_PROCESS_BYTES // 2)])
        for i in range(2 * MAX_DOCUMENT_BYTES // MAX_PROCESS_BYTES + 1)
    ]
    for entry in entries:
        del entry["cwlVersion"]
    document = {"cwlVersion": "v1.2", "$graph": entries}
    with pytest.raises(PolicyViolationError) as e:
        check_policy(document)
    assert str(e.value).startswith("the document is")
    assert "#t0.cwl is" not in str(e.value)


@pytest.mark.parametrize(
    "value",
    [
        ["DockerRequirement"],
        [None],
        5,
        {"DockerRequirement": "alpine"},
        {"ResourceRequirement": None},
    ],
)
@pytest.mark.parametrize("where", ["requirements", "hints"])
def test_requirements_and_hints_that_are_not_objects_are_refused(value, where):
    with pytest.raises(
        PolicyViolationError, match=f"#main has {where} that are not a list or map"
    ):
        check_policy(tool(**{where: value}))


def test_abstract_operations_are_refused_wherever_they_are():
    operation = {
        "cwlVersion": "v1.2",
        "class": "Operation",
        "id": "#main",
        "inputs": [{"id": "#main/x", "type": "File"}],
        "outputs": [{"id": "#main/y", "type": "File"}],
    }
    for document in (operation, workflow({**operation, "id": "#op"})):
        with pytest.raises(PolicyViolationError, match="is an Operation") as e:
            check_policy(document)
        assert "CommandLineTool, ExpressionTool or Workflow" in str(e.value)


def test_docker_images_are_accepted_when_pulled_or_named():
    for image in (
        "harbor.cta-observatory.org/ctapipe:0.24",
        "alpine@sha256:" + "a" * 64,
        # An unpacked image on CVMFS
        "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/library/alpine:3",
    ):
        check_policy(
            tool(requirements=[{"class": "DockerRequirement", "dockerPull": image}])
        )
    check_policy(
        tool(requirements=[{"class": "DockerRequirement", "dockerImageId": "ctapipe"}])
    )


@pytest.mark.parametrize("field", ["dockerFile", "dockerImport", "dockerLoad"])
@pytest.mark.parametrize("where", ["requirements", "hints"])
def test_docker_images_built_or_fetched_on_the_worker_are_refused(field, where):
    docker = [{"class": "DockerRequirement", "dockerImageId": "x", field: "x"}]
    with pytest.raises(PolicyViolationError, match=f"DockerRequirement {field}") as e:
        check_policy(tool(**{where: docker}))
    assert "dockerPull" in str(e.value)


@pytest.mark.parametrize("where", ["requirements", "hints"])
def test_every_docker_requirement_given_is_checked(where):
    built = {"class": "DockerRequirement", "dockerImageId": "x", "dockerFile": "x"}
    pulled = {"class": "DockerRequirement", "dockerPull": "alpine"}
    for docker in ([built, pulled], [pulled, built]):
        with pytest.raises(PolicyViolationError, match="DockerRequirement dockerFile"):
            check_policy(tool(**{where: docker}))
