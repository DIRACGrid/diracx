"""Test the legacy_adaptor functionality in BaseModel for CS conversion."""

from __future__ import annotations

import os

import pytest
from pydantic import Field, ValidationError

from diracx.core.config import SerializableSet
from diracx.core.config.schema import BaseModel
from diracx.core.properties import NORMAL_USER, PRODUCTION_MANAGEMENT, SecurityProperty


# Note: In the following tests we use BaseModel coming from the config schema module!
class SimpleModel(BaseModel):
    """Test model with various field types that should be handled by legacy_adaptor."""

    # Non-optional list
    required_list: list[str] = Field(alias="RequiredList")

    # Optional list
    optional_list: list[str] | None = Field(None, alias="OptionalList")

    # Non-optional SerializableSet
    required_set: SerializableSet[str] = Field(alias="RequiredSet")

    # Optional SerializableSet
    optional_set: SerializableSet[str] | None = Field(None, alias="OptionalSet")

    # Optional SerializableSet with SecurityProperty
    optional_security_set: SerializableSet[SecurityProperty] | None = Field(
        None, alias="OptionalSecuritySet"
    )


def test_legacy_adaptor_without_env_var():
    """Test that legacy_adaptor does nothing when DIRAC_COMPAT_ENABLE_CS_CONVERSION is not set."""
    # Ensure the env var is not set
    os.environ.pop("DIRAC_COMPAT_ENABLE_CS_CONVERSION", None)

    with pytest.raises(ValidationError) as exc_info:
        SimpleModel.model_validate(
            {
                "RequiredList": "item1, item2",
                "RequiredSet": "item3, item4",
            }
        )

    # Verify the error is about type validation
    errors = exc_info.value.errors()
    assert any(error["type"] == "list_type" for error in errors)


def test_legacy_adaptor_required_list(monkeypatch):
    """Test that required list[str] fields are converted from comma-separated strings."""
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": "xroot, root",
            "RequiredSet": "item1, item2",
        }
    )

    assert model.required_list == ["xroot", "root"]
    assert model.required_set == {"item1", "item2"}


def test_legacy_adaptor_optional_list_with_value(monkeypatch):
    """Test that optional list[str] | None fields are converted from comma-separated strings.

    This is the failing test case that reproduces the issue from the certification environment.
    """
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": "item1",
            "RequiredSet": "item2",
            "OptionalList": "xroot, root",
        }
    )

    assert model.optional_list == ["xroot", "root"]


def test_legacy_adaptor_optional_list_with_none(monkeypatch):
    """Test that optional list fields can be None."""
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": "item1",
            "RequiredSet": "item2",
            "OptionalList": None,
        }
    )

    assert model.optional_list is None


def test_legacy_adaptor_optional_set_with_value(monkeypatch):
    """Test that optional SerializableSet[str] | None fields are converted."""
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": "item1",
            "RequiredSet": "item2",
            "OptionalSet": "value1, value2, value3",
        }
    )

    assert model.optional_set == {"value1", "value2", "value3"}


def test_legacy_adaptor_optional_security_property_set(monkeypatch):
    """Test that optional SerializableSet[SecurityProperty] | None fields are converted."""
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": "item1",
            "RequiredSet": "item2",
            "OptionalSecuritySet": "NormalUser, ProductionManagement",
        }
    )

    assert model.optional_security_set == {
        NORMAL_USER,
        PRODUCTION_MANAGEMENT,
    }


def test_legacy_adaptor_whitespace_handling(monkeypatch):
    """Test that whitespace is properly stripped from comma-separated values."""
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": "  item1  ,  item2  ,  item3  ",
            "RequiredSet": "value1,value2,  value3  ",
            "OptionalList": "xroot,   root,   gsiftp",
        }
    )

    assert model.required_list == ["item1", "item2", "item3"]
    assert model.required_set == {"value1", "value2", "value3"}
    assert model.optional_list == ["xroot", "root", "gsiftp"]


def test_legacy_adaptor_empty_values(monkeypatch):
    """Test that empty values are properly filtered out."""
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": "item1,,item2,,,item3",  # Empty values between commas
            "RequiredSet": "value1, , value2",  # Empty value with spaces
        }
    )

    assert model.required_list == ["item1", "item2", "item3"]
    assert model.required_set == {"value1", "value2"}


def test_legacy_adaptor_already_list(monkeypatch):
    """Test that fields that are already lists are not modified."""
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "yes")

    model = SimpleModel.model_validate(
        {
            "RequiredList": ["already", "a", "list"],
            "RequiredSet": ["already", "a", "list"],
            "OptionalList": ["also", "a", "list"],
        }
    )

    assert model.required_list == ["already", "a", "list"]
    assert model.required_set == {"already", "a", "list"}
    assert model.optional_list == ["also", "a", "list"]
