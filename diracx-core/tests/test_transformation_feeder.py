"""Tests for feeder proposal models (DX-ADR-006)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from diracx.core.models.transformation import FeederInput, SetFeederState


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


@pytest.mark.parametrize("kwargs", [{"lfn": "/a"}, {"lfn_size": 1}])
def test_lfn_and_lfn_size_must_be_paired(kwargs):
    with pytest.raises(ValidationError, match="both be set or both be omitted"):
        FeederInput(**kwargs)


@pytest.mark.parametrize(
    "lfn, ok", [("", False), ("/a", True), ("x" * 512, True), ("x" * 513, False)]
)
def test_lfn_length_bounds(lfn, ok):
    if ok:
        assert FeederInput(lfn=lfn, lfn_size=0).lfn == lfn
    else:
        with pytest.raises(ValidationError):
            FeederInput(lfn=lfn, lfn_size=0)


@pytest.mark.parametrize("lfn_size, ok", [(-1, False), (0, True), (2**40, True)])
def test_lfn_size_non_negative(lfn_size, ok):
    if ok:
        assert FeederInput(lfn="/a", lfn_size=lfn_size).lfn_size == lfn_size
    else:
        with pytest.raises(ValidationError):
            FeederInput(lfn="/a", lfn_size=lfn_size)


@pytest.mark.parametrize("bad", [datetime.now(UTC), {1, 2}, object()])
def test_descriptor_rejects_non_json_values(bad):
    with pytest.raises(ValidationError):
        FeederInput(descriptor={"x": bad})


@pytest.mark.parametrize(
    "delayed_until",
    [
        datetime(2026, 1, 1),  # noqa: DTZ001
        datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1))),
    ],
    ids=["naive", "non-utc"],
)
def test_delayed_until_must_be_aware_utc(delayed_until):
    with pytest.raises(ValidationError):
        FeederInput(delayed_until=delayed_until)


def test_feeder_input_json_round_trip():
    original = FeederInput(
        lfn="/lhcb/a.dst",
        lfn_size=1024,
        descriptor={"Mask": [1, 2], "nested": {"k": None}},
        delayed_until=datetime(2026, 1, 2, tzinfo=UTC),
    )
    assert FeederInput.model_validate_json(original.model_dump_json()) == original


def test_set_feeder_state_requires_a_dict():
    assert SetFeederState(state={"next_seed": 7}).state == {"next_seed": 7}
    with pytest.raises(ValidationError):
        SetFeederState()
    with pytest.raises(ValidationError):
        SetFeederState(state=[1, 2])
