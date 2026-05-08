"""Tests for DiracReplicaMapFsAccess SB: path resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from diracx.cli.executor.fs_access import DiracReplicaMapFsAccess
from diracx.core.models.replica_map import ReplicaMap


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
