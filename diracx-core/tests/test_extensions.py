import pytest

from diracx.core.extensions import extensions_by_priority


@pytest.fixture
def clean_cache():
    extensions_by_priority.cache_clear()
    yield
    extensions_by_priority.cache_clear()


@pytest.mark.parametrize(
    "env,expected",
    [
        ("diracx", ["diracx"]),
        ("os", ["os"]),
        ("os,diracx", ["os", "diracx"]),
        ("diracx,os", ["diracx", "os"]),
    ],
)
def test_extensions_by_priority(monkeypatch, env, expected, clean_cache):
    monkeypatch.setenv("DIRACX_EXTENSIONS", env)
    assert list(extensions_by_priority()) == expected


def test_extensions_by_priority_error(monkeypatch, clean_cache):
    monkeypatch.setenv("DIRACX_EXTENSIONS", "missingdiracx")
    with pytest.raises(RuntimeError, match="Could not find extension module"):
        list(extensions_by_priority())
