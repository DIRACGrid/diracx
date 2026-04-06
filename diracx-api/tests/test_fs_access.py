"""Tests for DiracReplicaMapFsAccess SB: path resolution.

cwltool is an optional runtime dependency (not installed in test env),
so we mock cwltool.stdfsaccess.StdFsAccess and load fs_access directly.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Provide a minimal cwltool.stdfsaccess.StdFsAccess mock so fs_access.py
# can be imported without the real cwltool package.
# ---------------------------------------------------------------------------


class _StdFsAccess:
    """Minimal stub of cwltool.stdfsaccess.StdFsAccess for testing."""

    def __init__(self, basedir: str):
        self.basedir = basedir

    def _abs(self, p: str) -> str:
        if os.path.isabs(p):
            return p
        return os.path.join(self.basedir, p)

    def glob(self, pattern: str) -> list[str]:
        import glob as _glob

        return _glob.glob(self._abs(pattern))

    def open(self, fn: str, mode: str):
        return open(self._abs(fn), mode)

    def exists(self, fn: str) -> bool:
        return os.path.exists(self._abs(fn))

    def isfile(self, fn: str) -> bool:
        return os.path.isfile(self._abs(fn))

    def isdir(self, fn: str) -> bool:
        return os.path.isdir(self._abs(fn))

    def size(self, fn: str) -> int:
        return os.stat(self._abs(fn)).st_size


def _ensure_mock_module(name: str) -> types.ModuleType:
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)
    return sys.modules[name]


# Only install mocks if cwltool is not genuinely available
try:
    import cwltool.stdfsaccess  # noqa: F401
except ImportError:
    _ensure_mock_module("cwltool")
    _mod = _ensure_mock_module("cwltool.stdfsaccess")
    _mod.StdFsAccess = _StdFsAccess  # type: ignore[attr-defined]


def _load_fs_access_module():
    """Load fs_access.py directly, bypassing the executor __init__.py.

    The executor __init__.py imports cwltool-heavy modules (executor.py etc.)
    that we don't need and can't mock easily. Loading the module file directly
    avoids that chain.
    """
    fs_access_path = (
        Path(__file__).resolve().parent.parent
        / "src"
        / "diracx"
        / "api"
        / "executor"
        / "fs_access.py"
    )
    spec = importlib.util.spec_from_file_location(
        "diracx.api.executor.fs_access", fs_access_path
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_fs_mod = _load_fs_access_module()
DiracReplicaMapFsAccess = _fs_mod.DiracReplicaMapFsAccess

from diracx.core.models.replica_map import ReplicaMap  # noqa: E402


@pytest.fixture
def sandbox_file(tmp_path: Path) -> Path:
    """Create a local file simulating an extracted sandbox file."""
    f = tmp_path / "helper.sh"
    f.write_text("#!/bin/bash\necho hello")
    return f


@pytest.fixture
def fs_access_with_sb(tmp_path: Path, sandbox_file: Path) -> DiracReplicaMapFsAccess:
    """FsAccess with a sandbox entry in the replica map."""
    sb_key = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
    replica_map = ReplicaMap(
        root={
            sb_key: {
                "replicas": [{"url": f"file://{sandbox_file}", "se": "local"}],
            }
        }
    )
    return DiracReplicaMapFsAccess(str(tmp_path), replica_map=replica_map)


class TestSBResolution:
    def test_resolve_path_sb(self, fs_access_with_sb, sandbox_file):
        """SB: path should resolve to local file path via replica map."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        resolved, is_remote = fs_access_with_sb._resolve_path(sb_path)
        assert resolved == str(sandbox_file)
        assert is_remote is False

    def test_resolve_path_lfn_still_works(self, tmp_path):
        """LFN: resolution should still work after rename."""
        lfn_file = tmp_path / "data.dst"
        lfn_file.write_text("data")
        replica_map = ReplicaMap(
            root={
                "/lhcb/data/file.dst": {
                    "replicas": [{"url": f"file://{lfn_file}", "se": "CERN-DST"}],
                }
            }
        )
        fs = DiracReplicaMapFsAccess(str(tmp_path), replica_map=replica_map)
        resolved, is_remote = fs._resolve_path("LFN:/lhcb/data/file.dst")
        assert resolved == str(lfn_file)
        assert is_remote is False

    def test_abs_resolves_sb(self, fs_access_with_sb, sandbox_file):
        """_abs should resolve SB: paths through replica map."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        result = fs_access_with_sb._abs(sb_path)
        assert result == str(sandbox_file)

    def test_exists_sb(self, fs_access_with_sb):
        """exists() should return True for SB: path with local file."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert fs_access_with_sb.exists(sb_path) is True

    def test_isfile_sb(self, fs_access_with_sb):
        """isfile() should return True for SB: path."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert fs_access_with_sb.isfile(sb_path) is True

    def test_isdir_sb(self, fs_access_with_sb):
        """isdir() should return False for SB: paths (always files)."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert fs_access_with_sb.isdir(sb_path) is False

    def test_open_sb(self, fs_access_with_sb):
        """open() should work for SB: path resolved to local file."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        with fs_access_with_sb.open(sb_path, "r") as f:
            content = f.read()
        assert "echo hello" in content

    def test_size_sb(self, fs_access_with_sb, sandbox_file):
        """size() should return file size for SB: path."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert fs_access_with_sb.size(sb_path) == sandbox_file.stat().st_size


@pytest.fixture
def fs_access_with_remote_lfn(tmp_path: Path) -> DiracReplicaMapFsAccess:
    """FsAccess with an LFN that resolves to a remote (root://) URL."""
    replica_map = ReplicaMap(
        root={
            "/lhcb/data/remote.dst": {
                "replicas": [
                    {
                        "url": "root://eoslhcb.cern.ch//eos/lhcb/data/remote.dst",
                        "se": "CERN-EOS",
                    }
                ],
                "size_bytes": 2048,
            }
        }
    )
    return DiracReplicaMapFsAccess(str(tmp_path), replica_map=replica_map)


class TestLFNResolution:
    def test_resolve_lfn_remote_url(self, fs_access_with_remote_lfn):
        """LFN resolving to a root:// URL should be marked as remote."""
        resolved, is_remote = fs_access_with_remote_lfn._resolve_path(
            "LFN:/lhcb/data/remote.dst"
        )
        assert is_remote is True
        assert "root://" in resolved

    def test_exists_remote_lfn_returns_true(self, fs_access_with_remote_lfn):
        """exists() should return True for a remote LFN without touching the filesystem."""
        assert fs_access_with_remote_lfn.exists("LFN:/lhcb/data/remote.dst") is True

    def test_size_from_replica_map(self, fs_access_with_remote_lfn):
        """size() should return size_bytes from the replica map entry."""
        assert fs_access_with_remote_lfn.size("LFN:/lhcb/data/remote.dst") == 2048

    def test_lfn_not_in_map_returns_cleaned_path(self, tmp_path: Path):
        """Missing LFN should resolve to path without the LFN: prefix."""
        fs = DiracReplicaMapFsAccess(str(tmp_path), replica_map=ReplicaMap(root={}))
        resolved, is_remote = fs._resolve_path("LFN:/lhcb/data/missing.dst")
        assert resolved == "/lhcb/data/missing.dst"
        assert is_remote is False

    def test_glob_remote_lfn_returns_original(self, fs_access_with_remote_lfn):
        """glob() should return the original LFN: path for remote LFNs."""
        lfn_path = "LFN:/lhcb/data/remote.dst"
        result = fs_access_with_remote_lfn.glob(lfn_path)
        assert result == [lfn_path]

    def test_open_remote_lfn_raises(self, fs_access_with_remote_lfn):
        """open() should raise ValueError for a remote LFN."""
        with pytest.raises(ValueError, match="Cannot open remote file"):
            fs_access_with_remote_lfn.open("LFN:/lhcb/data/remote.dst", "r")
