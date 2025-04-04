from __future__ import annotations

from diracx.core.extensions import extensions_by_priority


def test_extensions_by_priority():
    # The case with extension installed is tested in the gubbins tests
    assert list(extensions_by_priority()) == ["diracx"]
