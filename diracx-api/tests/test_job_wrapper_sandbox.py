"""Tests for JobWrapper SB: path parsing and replica map injection.

job_wrapper.py has heavy runtime dependencies (cwl_utils, DIRACCommon,
ruamel.yaml, diracx.client) that are not installed in the test environment.
We mock the unavailable modules at import time, then load job_wrapper.py
directly so we can test the pure-logic methods (parse_sb_path,
_add_sandbox_entries_to_replica_map) without those dependencies.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

from diracx.core.models.replica_map import ReplicaMap

# ---------------------------------------------------------------------------
# Mock unavailable modules so job_wrapper.py can be imported
# ---------------------------------------------------------------------------


def _ensure_mock(name: str) -> types.ModuleType:
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)
    return sys.modules[name]


_MOCK_MODULES = [
    "cwl_utils",
    "cwl_utils.parser",
    "cwl_utils.parser.cwl_v1_2",
    "DIRACCommon",
    "DIRACCommon.Core",
    "DIRACCommon.Core.Utilities",
    "DIRACCommon.Core.Utilities.ReturnValues",
    "ruamel",
    "ruamel.yaml",
    "diracx.api.job_report",
    "diracx.api.jobs",
    "diracx.client",
    "diracx.client.aio",
]

for _name in _MOCK_MODULES:
    _mod = _ensure_mock(_name)
    # Provide stubs for names imported by job_wrapper.py
    if _name == "cwl_utils.parser":
        _mod.save = None  # type: ignore[attr-defined]
    elif _name == "cwl_utils.parser.cwl_v1_2":
        for _attr in (
            "CommandLineTool",
            "ExpressionTool",
            "File",
            "Saveable",
            "Workflow",
        ):
            setattr(_mod, _attr, type(_attr, (), {}))
    elif _name == "DIRACCommon.Core.Utilities.ReturnValues":
        _mod.returnValueOrRaise = None  # type: ignore[attr-defined]
    elif _name == "ruamel.yaml":
        _mod.YAML = None  # type: ignore[attr-defined]
    elif _name == "diracx.api.job_report":
        _mod.JobReport = None  # type: ignore[attr-defined]
    elif _name == "diracx.api.jobs":
        _mod.create_sandbox = None  # type: ignore[attr-defined]
        _mod.download_sandbox = None  # type: ignore[attr-defined]
    elif _name == "diracx.client.aio":
        _mod.AsyncDiracClient = None  # type: ignore[attr-defined]


def _load_job_wrapper():
    """Load job_wrapper.py directly, bypassing package __init__ chains."""
    path = (
        Path(__file__).resolve().parent.parent
        / "src"
        / "diracx"
        / "api"
        / "job_wrapper.py"
    )
    spec = importlib.util.spec_from_file_location("diracx.api.job_wrapper", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_jw_mod = _load_job_wrapper()
JobWrapper = _jw_mod.JobWrapper


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParseSBPath:
    """Test the SB: path parsing logic."""

    def test_parse_sb_path(self):
        """Parse SB: path into PFN and relative path."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:abc123.tar.zst#helper.sh"
        pfn, rel_path = JobWrapper.parse_sb_path(sb_path)
        assert pfn == "SandboxSE|/S3/store/sha256:abc123.tar.zst"
        assert rel_path == "helper.sh"

    def test_parse_sb_path_nested(self):
        """Parse SB: path with nested relative path."""
        sb_path = "SB:SandboxSE|/S3/store/sha256:def456.tar.zst#config/db.yaml"
        pfn, rel_path = JobWrapper.parse_sb_path(sb_path)
        assert pfn == "SandboxSE|/S3/store/sha256:def456.tar.zst"
        assert rel_path == "config/db.yaml"

    def test_parse_sb_path_no_fragment(self):
        """SB: path without # fragment should raise ValueError."""
        with pytest.raises(ValueError, match="missing '#' fragment"):
            JobWrapper.parse_sb_path("SB:SandboxSE|/S3/store/sha256:abc.tar.zst")

    def test_parse_sb_path_no_prefix(self):
        """Non-SB: path should raise ValueError."""
        with pytest.raises(ValueError, match="Not an SB: path"):
            JobWrapper.parse_sb_path("/some/local/file.txt")


class TestAddSandboxEntriesToReplicaMap:
    """Test injecting sandbox entries into the replica map JSON."""

    def test_creates_new_replica_map(self, tmp_path):
        """When no replica map exists, create one with sandbox entries."""
        sandbox_mappings = {
            "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh": tmp_path
            / "helper.sh",
        }
        (tmp_path / "helper.sh").write_text("#!/bin/bash")

        wrapper = JobWrapper.__new__(JobWrapper)
        wrapper._replica_map_path = None
        wrapper._add_sandbox_entries_to_replica_map(sandbox_mappings, tmp_path)

        assert wrapper._replica_map_path is not None
        replica_map = ReplicaMap.model_validate_json(
            wrapper._replica_map_path.read_text()
        )
        key = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert key in replica_map.root
        entry = replica_map[key]
        assert len(entry.replicas) == 1
        assert entry.replicas[0].se == "local"
        assert "helper.sh" in str(entry.replicas[0].url)

    def test_extends_existing_replica_map(self, tmp_path):
        """When a replica map already exists (from LFN resolution), add sandbox entries."""
        existing = {
            "/lhcb/data/file.dst": {
                "replicas": [
                    {"url": "root://eoslhcb.cern.ch//data/file.dst", "se": "CERN-DST"}
                ],
            }
        }
        map_path = tmp_path / "replica_map.json"
        map_path.write_text(json.dumps(existing))

        sandbox_mappings = {
            "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#script.py": tmp_path
            / "script.py",
        }
        (tmp_path / "script.py").write_text("print('hello')")

        wrapper = JobWrapper.__new__(JobWrapper)
        wrapper._replica_map_path = map_path
        wrapper._add_sandbox_entries_to_replica_map(sandbox_mappings, tmp_path)

        replica_map = ReplicaMap.model_validate_json(map_path.read_text())
        assert "/lhcb/data/file.dst" in replica_map.root
        assert "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#script.py" in replica_map.root
