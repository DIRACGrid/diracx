"""Tests for feeder proposal models (DX-ADR-006)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from diracx.core.models.feeder import (
    Input,
    NoMoreInputs,
    SetFeederState,
)


def test_input_seed_and_file_shapes():
    seed = Input(descriptor={"seed": 42})
    assert seed.descriptor == {"seed": 42}
    assert seed.lfn is None

    delayed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    file_input = Input(
        lfn="/lhcb/a.dst",
        lfn_size=1024,
        descriptor={"Mask": "1"},
        delayed_until=delayed,
    )
    assert file_input.lfn == "/lhcb/a.dst"
    assert file_input.lfn_size == 1024
    assert file_input.delayed_until == delayed


def test_input_lfn_size_requires_lfn():
    with pytest.raises(ValueError, match="lfn_size requires lfn"):
        Input(lfn_size=1024)


def test_feeder_control_proposals_construct():
    assert SetFeederState(state={"next_seed": 7}).state == {"next_seed": 7}
    assert NoMoreInputs() is not None
