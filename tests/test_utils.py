from __future__ import annotations

from diracx.core.utils import dotenv_files_from_environment


def test_dotenv_files_from_environment(monkeypatch):
    monkeypatch.setattr("os.environ", {})
    assert dotenv_files_from_environment("TEST_PREFIX") == []

    monkeypatch.setattr("os.environ", {"TEST_PREFIX": "/a"})
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a"]

    monkeypatch.setattr("os.environ", {"TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"})
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b"]

    monkeypatch.setattr(
        "os.environ",
        {"TEST_PREFIX_2": "/c", "TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"},
    )
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b", "/c"]

    monkeypatch.setattr(
        "os.environ",
        {"TEST_PREFIX_2a": "/c", "TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"},
    )
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b"]
