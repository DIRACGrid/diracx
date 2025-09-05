from __future__ import annotations

import pytest

from diracx.core.extensions import (
    extensions_by_priority,
    select_from_extension,
    supports_extending,
)


class MockEntryPoint:
    def __init__(self, value, load_func):
        self.value = value
        self._load_func = load_func

    def load(self):
        return self._load_func


def test_extensions_by_priority():
    # The case with extension installed is tested in the gubbins tests
    assert list(extensions_by_priority()) == ["diracx"]


def test_select_from_extension():
    """Test the select_from_extension function."""
    # Test with existing group
    result = select_from_extension(group="diracx", name="extension")
    assert len(result) >= 1
    assert all(ep.group == "diracx" for ep in result)
    assert all(ep.name == "extension" for ep in result)

    # Test with non-existent group
    result = select_from_extension(group="nonexistent", name="test")
    assert result == []


def test_supports_extending_decorator(monkeypatch):
    """Test the supports_extending decorator functionality."""

    # Create a mock function to decorate
    def original_function(x: int) -> str:
        return f"original: {x}"

    # Test with no extensions (should use original function)
    def mock_select_from_extension(group: str, name: str):
        # Mock to return only the original function's entry point
        mock_entry_point = MockEntryPoint(
            f"{original_function.__module__}:{original_function.__qualname__}",
            original_function,
        )
        return [mock_entry_point]

    monkeypatch.setattr(
        "diracx.core.extensions.select_from_extension", mock_select_from_extension
    )

    decorated_func = supports_extending("test.group", "test.name")(original_function)

    # Should return the original function
    assert decorated_func is original_function
    result = decorated_func(42)
    assert result == "original: 42"


def test_supports_extending_with_extension(monkeypatch):
    """Test the supports_extending decorator with an extension."""

    def original_function(x: int) -> str:
        return f"original: {x}"

    def extension_function(x: int) -> str:
        return f"extension: {x}"

    def mock_select_from_extension(group: str, name: str):
        # Mock to return extension first, then original
        mock_extension_ep = MockEntryPoint(
            "extension.module:extension_function", extension_function
        )
        mock_original_ep = MockEntryPoint(
            f"{original_function.__module__}:{original_function.__qualname__}",
            original_function,
        )
        return [mock_extension_ep, mock_original_ep]

    monkeypatch.setattr(
        "diracx.core.extensions.select_from_extension", mock_select_from_extension
    )

    decorated_func = supports_extending("test.group", "test.name")(original_function)

    # Should return the extension function
    assert decorated_func is extension_function
    result = decorated_func(42)
    assert result == "extension: 42"


def test_supports_extending_no_candidates(monkeypatch):
    """Test the supports_extending decorator with no candidates."""

    def original_function(x: int) -> str:
        return f"original: {x}"

    def mock_select_from_extension(group: str, name: str):
        # Mock to return empty list
        return []

    monkeypatch.setattr(
        "diracx.core.extensions.select_from_extension", mock_select_from_extension
    )

    with pytest.raises(
        AssertionError,
        match="No extension found for group='test.group' name='test.name'",
    ):
        supports_extending("test.group", "test.name")(original_function)


def test_supports_extending_wrong_original(monkeypatch):
    """Test the supports_extending decorator with wrong original function."""

    def original_function(x: int) -> str:
        return f"original: {x}"

    def mock_select_from_extension(group: str, name: str):
        # Mock to return entry point with wrong value
        mock_entry_point = MockEntryPoint(
            "wrong.module:wrong.function", original_function
        )
        return [mock_entry_point]

    monkeypatch.setattr(
        "diracx.core.extensions.select_from_extension", mock_select_from_extension
    )

    with pytest.raises(AssertionError):
        supports_extending("test.group", "test.name")(original_function)
