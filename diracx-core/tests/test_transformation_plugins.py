"""Tests for the arguments model shared by the transformation plugins."""

from __future__ import annotations

import pytest
from pydantic import NonNegativeInt, ValidationError

from diracx.core.models.transformation import PluginArgs


class CountArgs(PluginArgs):
    max_count: NonNegativeInt = 0


def test_plugin_args_reject_unknown_keys():
    with pytest.raises(ValidationError, match="maxcount"):
        CountArgs.model_validate({"maxcount": 5})


def test_plugin_args_are_frozen():
    args = CountArgs(max_count=5)
    with pytest.raises(ValidationError):
        args.max_count = 6  # type: ignore[misc]
