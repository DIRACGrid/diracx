"""Tests for the feeder/packer plugin contracts."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import NonNegativeInt, PositiveInt, ValidationError

from diracx.core.exceptions import ConfigurationError
from diracx.core.models.transformation import ComputeParcel, NoMoreFeederInputs
from diracx.logic.transformations import (
    FEEDER_KIND,
    PACKER_KIND,
    PluginArgs,
    load_plugin,
    validate_plugin_args,
)
from diracx.logic.transformations import plugins as plugins_module


class CountArgs(PluginArgs):
    max_count: NonNegativeInt = 0


class GroupArgs(PluginArgs):
    group_size: PositiveInt = 1


async def good_feeder(*, feeder_args: CountArgs, feeder_state, scouting: bool):
    yield NoMoreFeederInputs()


def sync_feeder(*, feeder_args: CountArgs, feeder_state, scouting: bool):
    yield NoMoreFeederInputs()


async def good_packer(
    *,
    packer_args: GroupArgs,
    inputs,
    flush: bool,
    feeder_active: bool,
    input_overview,
):
    async for packer_input in inputs:
        yield ComputeParcel(input_ids=[packer_input.input_id])


async def packer_missing_flush(
    *, packer_args: GroupArgs, inputs, feeder_active: bool, input_overview
):
    yield ComputeParcel(input_ids=[])


async def packer_with_kwargs(*, packer_args: GroupArgs, **kwargs):
    yield ComputeParcel(input_ids=[])


async def packer_untyped_args(
    *, packer_args: dict, inputs, flush, feeder_active, input_overview
):
    yield ComputeParcel(input_ids=[])


@pytest.fixture
def register(monkeypatch):
    """Make ``select_from_extension`` return the given plugin for any name."""

    def _register(plugin):
        entry_point = SimpleNamespace(load=lambda: plugin)
        monkeypatch.setattr(
            plugins_module, "select_from_extension", lambda **_: [entry_point]
        )

    return _register


@pytest.mark.parametrize("kind", [FEEDER_KIND, PACKER_KIND], ids=["feeder", "packer"])
def test_load_unknown_name(kind):
    with pytest.raises(ConfigurationError, match=f"No {kind.name} named"):
        load_plugin(kind, "DoesNotExist")


def test_load_feeder(register):
    register(good_feeder)
    assert load_plugin(FEEDER_KIND, "Count") == (good_feeder, CountArgs)


def test_load_packer(register):
    register(good_packer)
    assert load_plugin(PACKER_KIND, "Group") == (good_packer, GroupArgs)


def test_load_rejects_sync_generator(register):
    register(sync_feeder)
    with pytest.raises(ConfigurationError, match="async generator function"):
        load_plugin(FEEDER_KIND, "Sync")


def test_load_rejects_missing_contract_param(register):
    register(packer_missing_flush)
    with pytest.raises(ConfigurationError, match="flush"):
        load_plugin(PACKER_KIND, "Broken")


def test_load_accepts_var_keyword(register):
    register(packer_with_kwargs)
    assert load_plugin(PACKER_KIND, "Kwargs")[1] is GroupArgs


def test_load_requires_plugin_args_annotation(register):
    register(packer_untyped_args)
    with pytest.raises(ConfigurationError, match="PluginArgs subclass"):
        load_plugin(PACKER_KIND, "Untyped")


def test_validate_plugin_args(register):
    register(good_feeder)
    assert validate_plugin_args(FEEDER_KIND, "Count", {"max_count": 5}) == CountArgs(
        max_count=5
    )
    assert validate_plugin_args(FEEDER_KIND, "Count", None) == CountArgs()


def test_plugin_args_reject_unknown_keys(register):
    register(good_feeder)
    with pytest.raises(ValidationError, match="maxcount"):
        validate_plugin_args(FEEDER_KIND, "Count", {"maxcount": 5})


def test_plugin_args_are_frozen():
    args = CountArgs(max_count=5)
    with pytest.raises(ValidationError):
        args.max_count = 6  # type: ignore[misc]
