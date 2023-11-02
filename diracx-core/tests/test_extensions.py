import pytest

from diracx.core.extensions import extensions_by_priority


def test_extensions_by_priority(monkeypatch):
    monkeypatch.setenv("DIRACX_EXTENSIONS", "diracx")
    assert list(extensions_by_priority()) == ["diracx"]

    monkeypatch.setenv("DIRACX_EXTENSIONS", "os")
    assert list(extensions_by_priority()) == ["os"]

    monkeypatch.setenv("DIRACX_EXTENSIONS", "os,diracx")
    assert list(extensions_by_priority()) == ["os", "diracx"]

    monkeypatch.setenv("DIRACX_EXTENSIONS", "diracx,os")
    assert list(extensions_by_priority()) == ["diracx", "os"]

    monkeypatch.delenv("DIRACX_EXTENSIONS")
    assert list(extensions_by_priority()) == ["diracx"]

    monkeypatch.setenv("DIRACX_EXTENSIONS", "missingdiracx")
    with pytest.raises(RuntimeError, match="Could not find extension module"):
        list(extensions_by_priority())
