"""Shared validation and documentation of transformation plugin models."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError
from uuid_utils.compat import uuid7

from diracx.core.models import TransformationInput
from diracx.core.models.transformation import (
    FeederInput,
    PackerInput,
)


@pytest.fixture(params=[TransformationInput, FeederInput, PackerInput])
def input_model(request):
    model = request.param
    required = {"input_id": uuid7()} if model is PackerInput else {}
    return model, required


def test_shared_input_fields_and_round_trip(input_model):
    model, required = input_model
    fields = {
        "descriptor": {"selection": [1, 1.5, None]},
        "lfn": "/vo/a",
        "lfn_size": 0,
    }
    instance = model(**required, **fields)
    assert isinstance(instance, TransformationInput)
    assert instance.model_dump(include=set(fields)) == fields
    assert model.model_validate_json(instance.model_dump_json()) == instance


@pytest.mark.parametrize(
    "fields",
    [
        {"lfn": "", "lfn_size": 0},
        {"lfn": "x" * 513, "lfn_size": 0},
        {"lfn": "/vo/a", "lfn_size": -1},
        {"descriptor": {"invalid": object()}},
    ],
)
def test_shared_input_constraints(input_model, fields):
    model, required = input_model
    with pytest.raises(ValidationError):
        model(**required, **fields)


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), float("-inf")], ids=["nan", "inf", "-inf"]
)
@pytest.mark.parametrize("nested", [False, True], ids=["direct", "nested"])
def test_shared_inputs_reject_non_finite_descriptor_values(input_model, value, nested):
    model, required = input_model
    descriptor = {"value": [{"nested": value}] if nested else value}

    with pytest.raises(ValidationError) as exc_info:
        model(**required, descriptor=descriptor)
    assert "finite_number" in {error["type"] for error in exc_info.value.errors()}

    payload = json.dumps({**required, "descriptor": descriptor}, default=str)
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate_json(payload)
    assert exc_info.value.errors()[0]["loc"] == ("descriptor",)
    assert exc_info.value.errors()[0]["type"] == "value_error"


def test_shared_field_documentation(input_model):
    model, _ = input_model
    properties = model.model_json_schema()["properties"]
    for field in ("descriptor", "lfn", "lfn_size"):
        assert properties[field]["description"]


def test_only_feeder_requires_lfn_and_size_together():
    assert PackerInput(input_id=uuid7(), lfn="/vo/a").lfn_size is None
    with pytest.raises(ValidationError, match="both be set or both be omitted"):
        FeederInput(lfn="/vo/a")


@pytest.mark.parametrize(
    "field, replacement",
    [
        ("descriptor", {"seed": 42}),
        ("lfn", "/vo/new"),
        ("lfn_size", 1),
    ],
)
def test_shared_inputs_are_frozen(input_model, field, replacement):
    model, required = input_model
    instance = model(**required)
    with pytest.raises(ValidationError) as exc_info:
        setattr(instance, field, replacement)
    assert exc_info.value.errors()[0]["type"] == "frozen_instance"
