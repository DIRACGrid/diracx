"""Taking DiracX's own hints out of a submitted document."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from diracx.core.cwl import (
    DIRAC_NAMESPACE,
    check_self_contained,
    entry_identity,
    split_dirac_hints,
)

FIXTURE = Path(__file__).parent / "fixtures" / "mc-simulation.packed.json"


def submitted() -> dict:
    return json.loads(FIXTURE.read_text())


def main(document: dict) -> dict:
    return next(e for e in document["$graph"] if e["id"] == "#main")


def test_every_dirac_hint_and_field_is_taken_out():
    document, hints = split_dirac_hints(submitted())
    assert DIRAC_NAMESPACE not in json.dumps(document["$graph"])
    assert set(hints) == {
        "#main",
        "#main/events",
        "#main/MCSimulation",
        "#main/MCReconstruction",
        "#main/MCMerge",
    }
    assert hints["#main/MCMerge"]["Transformation"]["packer"]["name"] == (
        "ByGroupSizeRun"
    )
    check_self_contained(document)


def test_the_submitted_document_is_left_as_it_is():
    document = submitted()
    split_dirac_hints(document)
    assert document == submitted()


def test_scheduling_edits_do_not_change_an_identity():
    edited = submitted()
    for step in main(edited)["steps"]:
        for hint in step.get("hints", []):
            if hint["class"] == DIRAC_NAMESPACE + "Transformation":
                hint["packer"] = {"name": "PerInput"}
    before, _ = split_dirac_hints(submitted())
    after, hints = split_dirac_hints(edited)
    assert entry_identity(after, "#main") == entry_identity(before, "#main")
    assert hints["#main/MCMerge"]["Transformation"]["packer"] == {"name": "PerInput"}


def test_hints_in_map_form_are_taken_out_too():
    tool = {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "id": "#main",
        "baseCommand": "true",
        "inputs": [],
        "outputs": [],
        "hints": {
            DIRAC_NAMESPACE + "Job": {"priority": 3},
            "ResourceRequirement": {"coresMin": 2},
        },
    }
    document, hints = split_dirac_hints(tool)
    assert document["hints"] == {"ResourceRequirement": {"coresMin": 2}}
    assert hints == {"#main": {"Job": {"priority": 3}}}
    only_dirac = copy.deepcopy(tool)
    del only_dirac["hints"]["ResourceRequirement"]
    document, _ = split_dirac_hints(only_dirac)
    assert "hints" not in document


def test_a_document_without_dirac_hints_is_unchanged():
    document, _ = split_dirac_hints(submitted())
    again, hints = split_dirac_hints(document)
    assert (again, hints) == (document, {})
