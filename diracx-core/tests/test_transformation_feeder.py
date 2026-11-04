"""Tests for feeder proposal models (DX-ADR-006)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from diracx.core.models.transformation import (
    FeederInput,
    NoMoreFeederInputs,
    SetFeederState,
)


def test_descriptor_defaults_to_empty_dict():
    assert FeederInput().descriptor == {}


def test_input_seed_and_file_shapes():
    seed = FeederInput(descriptor={"seed": 42})
    assert seed.descriptor == {"seed": 42}
    assert seed.lfn is None

    delayed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    file_input = FeederInput(
        lfn="/lhcb/a.dst",
        lfn_size=1024,
        descriptor={"Mask": "1"},
        delayed_until=delayed,
    )
    assert file_input.lfn == "/lhcb/a.dst"
    assert file_input.lfn_size == 1024
    assert file_input.delayed_until == delayed


def test_lfn_and_lfn_size_must_both_be_set_or_both_omitted():
    with pytest.raises(ValueError, match="both be set or both be omitted"):
        FeederInput(lfn_size=1024)
    with pytest.raises(ValueError, match="both be set or both be omitted"):
        FeederInput(lfn="/lhcb/a.dst")
    both_omitted = FeederInput(descriptor={"seed": 1})
    assert both_omitted.lfn is None and both_omitted.lfn_size is None
    both_set = FeederInput(lfn="/lhcb/a.dst", lfn_size=1024)
    assert both_set.lfn == "/lhcb/a.dst" and both_set.lfn_size == 1024


def test_lfn_constraints():
    with pytest.raises(ValidationError):
        FeederInput(lfn="", lfn_size=0)
    with pytest.raises(ValidationError):
        FeederInput(lfn="x" * 513, lfn_size=0)
    assert FeederInput(lfn="/a", lfn_size=1).lfn == "/a"
    assert FeederInput(lfn="x" * 512, lfn_size=0).lfn == "x" * 512


def test_lfn_size_non_negative():
    with pytest.raises(ValidationError):
        FeederInput(lfn="/a", lfn_size=-1)
    assert FeederInput(lfn="/a", lfn_size=0).lfn_size == 0


def test_feeder_control_proposals_construct():
    assert SetFeederState(state={"next_seed": 7}).state == {"next_seed": 7}
    assert NoMoreFeederInputs() is not None
