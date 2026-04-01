"""Tests for UTCDatetime pydantic type validation."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import BaseModel, ValidationError

import diracx.core.models
from diracx.core.models.types import UTCDatetime, _validate_utc


class SampleModel(BaseModel):
    ts: UTCDatetime
    optional_ts: UTCDatetime | None = None


class TestUTCDatetimeAcceptsUTC:
    def test_utc_timezone(self):
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        m = SampleModel(ts=dt)
        assert m.ts == dt
        assert m.ts.tzinfo is UTC

    def test_timezone_utc(self):
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        m = SampleModel(ts=dt)
        assert m.ts.utcoffset() == timedelta(0)

    def test_iso_string_utc(self):
        m = SampleModel(ts="2024-01-01T12:00:00Z")
        assert m.ts.tzinfo is UTC

    def test_iso_string_plus_zero(self):
        m = SampleModel(ts="2024-01-01T12:00:00+00:00")
        assert m.ts.utcoffset() == timedelta(0)

    def test_optional_none(self):
        m = SampleModel(ts="2024-01-01T12:00:00Z", optional_ts=None)
        assert m.optional_ts is None


class TestUTCDatetimeRejectsNonUTC:
    def test_naive_datetime(self):
        dt = datetime(2024, 1, 1, 12, 0, 0)  # noqa: DTZ001
        with pytest.raises(ValidationError, match="timezone"):
            SampleModel(ts=dt)

    def test_non_utc_timezone(self):
        cet = timezone(timedelta(hours=1))
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=cet)
        with pytest.raises(ValidationError, match="must be in UTC"):
            SampleModel(ts=dt)

    def test_iso_string_non_utc(self):
        with pytest.raises(ValidationError, match="must be in UTC"):
            SampleModel(ts="2024-01-01T12:00:00+05:30")

    def test_naive_iso_string(self):
        with pytest.raises(ValidationError):
            SampleModel(ts="2024-01-01T12:00:00")


def _is_datetime_type(annotation: type) -> bool:
    """Check if an annotation is datetime or a subclass of datetime."""
    try:
        return isinstance(annotation, type) and issubclass(annotation, datetime)
    except TypeError:
        return False


def _collect_model_classes() -> list[type[BaseModel]]:
    """Discover all BaseModel subclasses in diracx.core.models."""
    models = []
    package = diracx.core.models
    for _importer, modname, _ispkg in pkgutil.walk_packages(
        package.__path__, prefix=package.__name__ + "."
    ):
        if modname.endswith(".types"):
            continue
        module = importlib.import_module(modname)
        for _name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, BaseModel)
                and obj is not BaseModel
                and obj.__module__ == modname
            ):
                models.append(obj)
    return models


def _check_field_uses_utc_validator(model: type[BaseModel], field_name: str) -> bool:
    """Check that a datetime field has the _validate_utc AfterValidator."""
    field_info = model.model_fields[field_name]
    return any(getattr(m, "func", None) is _validate_utc for m in field_info.metadata)


def test_all_datetime_fields_use_utc_datetime():
    """Ensure no pydantic model in diracx.core.models uses bare datetime.

    Every datetime field must use UTCDatetime to enforce UTC validation.
    """
    violations = []
    for model in _collect_model_classes():
        for field_name, field_info in model.model_fields.items():
            if not _is_datetime_type(field_info.annotation):
                continue
            if not _check_field_uses_utc_validator(model, field_name):
                violations.append(f"{model.__name__}.{field_name}")

    assert not violations, (
        "The following fields use bare datetime instead of UTCDatetime:\n"
        + "\n".join(f"  - {v}" for v in violations)
    )
