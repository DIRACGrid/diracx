"""Unit tests for DiracExecutor, DiracPathMapper, and DiracCommandLineTool."""
# ruff: noqa: N803, N818  # Stub classes mirror cwltool's camelCase API

from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import diracx.cli.executor.tool as _tool_mod
from diracx.cli.executor.executor import DiracExecutor
from diracx.cli.executor.fs_access import DiracReplicaMapFsAccess  # noqa: F401
from diracx.cli.executor.pathmapper import DiracPathMapper
from diracx.cli.executor.tool import DiracCommandLineTool, dirac_make_tool
from diracx.core.models.replica_map import ReplicaMap

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_replica_map(*lfns_with_urls) -> ReplicaMap:
    """Create a ReplicaMap from (lfn, url) pairs."""
    root = {}
    for lfn, url in lfns_with_urls:
        root[lfn] = {"replicas": [{"url": url, "se": "TEST-SE"}]}
    return ReplicaMap(root=root)


def _make_replica_map_with_meta(lfn, url, size_bytes=None, adler32=None) -> ReplicaMap:
    entry = {"replicas": [{"url": url, "se": "TEST-SE"}]}
    if size_bytes is not None:
        entry["size_bytes"] = size_bytes
    if adler32 is not None:
        entry["checksum"] = {"adler32": adler32}
    return ReplicaMap(root={lfn: entry})


# ---------------------------------------------------------------------------
# Tests: DiracExecutor._extract_lfns_from_inputs
# ---------------------------------------------------------------------------


class TestExtractLfnsFromInputs:
    """Test the static method that finds LFNs in CWL job inputs."""

    def test_single_file_with_lfn_location(self):
        """A File object with LFN: location should yield its LFN (without prefix)."""
        executor = DiracExecutor.__new__(DiracExecutor)
        inputs = {
            "input_file": {
                "class": "File",
                "location": "LFN:/lhcb/data/2024/file.dst",
            }
        }
        result = executor._extract_lfns_from_inputs(inputs)
        assert result == ["/lhcb/data/2024/file.dst"]

    def test_single_file_with_lfn_path(self):
        """A File with LFN: in the 'path' field should also be extracted."""
        executor = DiracExecutor.__new__(DiracExecutor)
        inputs = {
            "f": {
                "class": "File",
                "path": "LFN:/lhcb/data/2024/other.dst",
            }
        }
        result = executor._extract_lfns_from_inputs(inputs)
        assert result == ["/lhcb/data/2024/other.dst"]

    def test_array_of_files(self):
        """An array of File objects should yield all their LFNs."""
        executor = DiracExecutor.__new__(DiracExecutor)
        inputs = {
            "files": [
                {"class": "File", "location": "LFN:/lhcb/data/a.dst"},
                {"class": "File", "location": "LFN:/lhcb/data/b.dst"},
                {"class": "File", "location": "LFN:/lhcb/data/c.dst"},
            ]
        }
        result = executor._extract_lfns_from_inputs(inputs)
        assert sorted(result) == [
            "/lhcb/data/a.dst",
            "/lhcb/data/b.dst",
            "/lhcb/data/c.dst",
        ]

    def test_no_lfn_inputs_returns_empty(self):
        """Inputs without LFN: paths should return an empty list."""
        executor = DiracExecutor.__new__(DiracExecutor)
        inputs = {
            "param": "hello",
            "count": 42,
        }
        result = executor._extract_lfns_from_inputs(inputs)
        assert result == []

    def test_sb_references_not_extracted(self):
        """SB: references are sandbox files, not LFNs — must not appear in result."""
        executor = DiracExecutor.__new__(DiracExecutor)
        inputs = {
            "script": {
                "class": "File",
                "location": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#run.sh",
            }
        }
        result = executor._extract_lfns_from_inputs(inputs)
        assert result == []

    def test_mixed_lfn_and_non_lfn(self):
        """Only LFN: files are extracted; other inputs are ignored."""
        executor = DiracExecutor.__new__(DiracExecutor)
        inputs = {
            "data": {"class": "File", "location": "LFN:/lhcb/data/file.dst"},
            "script": {
                "class": "File",
                "location": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#run.sh",
            },
            "config": {"class": "File", "location": "file:///local/config.yaml"},
            "count": 5,
        }
        result = executor._extract_lfns_from_inputs(inputs)
        assert result == ["/lhcb/data/file.dst"]

    def test_deduplication(self):
        """The same LFN appearing twice should be returned only once."""
        executor = DiracExecutor.__new__(DiracExecutor)
        inputs = {
            "a": {"class": "File", "location": "LFN:/lhcb/data/file.dst"},
            "b": {"class": "File", "location": "LFN:/lhcb/data/file.dst"},
        }
        result = executor._extract_lfns_from_inputs(inputs)
        assert result == ["/lhcb/data/file.dst"]


# ---------------------------------------------------------------------------
# Tests: DiracExecutor._prepare_job_replica_map
# ---------------------------------------------------------------------------


class TestPrepareJobReplicaMap:
    """Test filtering the global replica map for a single job step."""

    def _make_executor_with_map(self, replica_map: ReplicaMap) -> DiracExecutor:
        executor = DiracExecutor.__new__(DiracExecutor)
        executor.global_map = replica_map
        executor.global_map_path = None
        executor.output_dirs = set()
        return executor

    def _make_job(self, inputs: dict, outdir: str) -> object:
        """Create a minimal CommandLineJob-like object."""
        job = types.SimpleNamespace(
            name="test_job",
            outdir=outdir,
            builder=types.SimpleNamespace(job=inputs),
        )
        return job

    def test_filters_global_map_for_step(self, tmp_path):
        """Only the LFNs referenced by a step should appear in its replica map."""
        global_map = _make_replica_map(
            ("/lhcb/data/a.dst", "file:///storage/a.dst"),
            ("/lhcb/data/b.dst", "file:///storage/b.dst"),
            ("/lhcb/data/c.dst", "file:///storage/c.dst"),
        )
        executor = self._make_executor_with_map(global_map)
        job = self._make_job(
            {"f": {"class": "File", "location": "LFN:/lhcb/data/a.dst"}},
            str(tmp_path),
        )
        runtime_context = types.SimpleNamespace()
        executor._prepare_job_replica_map(job, runtime_context)

        step_map = ReplicaMap.model_validate_json(
            (tmp_path / "replica_map.json").read_text()
        )
        assert "/lhcb/data/a.dst" in step_map.root
        assert "/lhcb/data/b.dst" not in step_map.root
        assert "/lhcb/data/c.dst" not in step_map.root

    def test_empty_lfns_produces_empty_map(self, tmp_path):
        """A step with no LFN inputs should get an empty replica map written."""
        global_map = _make_replica_map(
            ("/lhcb/data/a.dst", "file:///storage/a.dst"),
        )
        executor = self._make_executor_with_map(global_map)
        job = self._make_job({"param": "hello"}, str(tmp_path))
        executor._prepare_job_replica_map(job, types.SimpleNamespace())

        step_map = ReplicaMap.model_validate_json(
            (tmp_path / "replica_map.json").read_text()
        )
        assert step_map.root == {}

    def test_missing_lfn_silently_skipped(self, tmp_path):
        """LFNs requested by a step but absent from the global map are silently skipped."""
        global_map = _make_replica_map(
            ("/lhcb/data/a.dst", "file:///storage/a.dst"),
        )
        executor = self._make_executor_with_map(global_map)
        job = self._make_job(
            {
                "f1": {"class": "File", "location": "LFN:/lhcb/data/a.dst"},
                "f2": {"class": "File", "location": "LFN:/lhcb/data/MISSING.dst"},
            },
            str(tmp_path),
        )
        executor._prepare_job_replica_map(job, types.SimpleNamespace())

        step_map = ReplicaMap.model_validate_json(
            (tmp_path / "replica_map.json").read_text()
        )
        assert "/lhcb/data/a.dst" in step_map.root
        assert "/lhcb/data/MISSING.dst" not in step_map.root


# ---------------------------------------------------------------------------
# Tests: DiracExecutor._update_replica_map_from_job
# ---------------------------------------------------------------------------


class TestUpdateReplicaMapFromJob:
    """Test merging step output back into the global replica map."""

    def _make_executor(self, initial_map: ReplicaMap | None = None) -> DiracExecutor:
        executor = DiracExecutor.__new__(DiracExecutor)
        executor.global_map = initial_map or ReplicaMap(root={})
        executor.global_map_path = None
        executor.output_dirs = set()
        return executor

    def test_new_entries_added_to_global_map(self, tmp_path):
        """New LFNs written by a job must appear in the global replica map."""
        executor = self._make_executor()

        step_map = _make_replica_map(("/lhcb/out/result.dst", "file:///out/result.dst"))
        (tmp_path / "replica_map.json").write_text(step_map.model_dump_json())

        job = types.SimpleNamespace(name="step1", outdir=str(tmp_path))
        executor._update_replica_map_from_job(job, types.SimpleNamespace())

        assert "/lhcb/out/result.dst" in executor.global_map.root

    def test_existing_entries_preserved(self, tmp_path):
        """Pre-existing entries in the global map must not be overwritten with identical data."""
        initial = _make_replica_map(
            ("/lhcb/data/input.dst", "file:///storage/input.dst")
        )
        executor = self._make_executor(initial)

        # Step map contains the same input entry (unchanged) plus a new output
        step_map = _make_replica_map(
            ("/lhcb/data/input.dst", "file:///storage/input.dst"),
            ("/lhcb/out/output.dst", "file:///out/output.dst"),
        )
        (tmp_path / "replica_map.json").write_text(step_map.model_dump_json())

        job = types.SimpleNamespace(name="step1", outdir=str(tmp_path))
        executor._update_replica_map_from_job(job, types.SimpleNamespace())

        assert "/lhcb/data/input.dst" in executor.global_map.root
        assert "/lhcb/out/output.dst" in executor.global_map.root

    def test_no_replica_map_file_is_safe(self, tmp_path):
        """If no replica_map.json exists in outdir, the update is a safe no-op."""
        executor = self._make_executor()
        job = types.SimpleNamespace(name="step_no_map", outdir=str(tmp_path))
        # Should not raise
        executor._update_replica_map_from_job(job, types.SimpleNamespace())
        assert executor.global_map.root == {}

    def test_no_outdir_is_safe(self):
        """If job.outdir is None, the update is a safe no-op."""
        executor = self._make_executor()
        job = types.SimpleNamespace(name="step_no_outdir", outdir=None)
        executor._update_replica_map_from_job(job, types.SimpleNamespace())
        assert executor.global_map.root == {}


# ---------------------------------------------------------------------------
# Tests: DiracPathMapper.visit
# ---------------------------------------------------------------------------


class TestDiracPathMapper:
    """Test LFN resolution logic in DiracPathMapper.visit."""

    def _make_mapper(self, replica_map: ReplicaMap) -> DiracPathMapper:
        """Instantiate DiracPathMapper without calling super().__init__ (avoids file scanning)."""
        mapper = DiracPathMapper.__new__(DiracPathMapper)
        mapper._pathmap = {}
        mapper.replica_map = replica_map
        return mapper

    def test_lfn_resolved_to_pfn(self):
        """LFN: location should be mapped to its PFN in _pathmap."""
        replica_map = _make_replica_map(
            ("/lhcb/data/file.dst", "file:///storage/file.dst")
        )
        mapper = self._make_mapper(replica_map)
        obj = {"class": "File", "location": "LFN:/lhcb/data/file.dst"}
        mapper.visit(obj, "/stagedir", "/basedir")
        assert "LFN:/lhcb/data/file.dst" in mapper._pathmap
        entry = mapper._pathmap["LFN:/lhcb/data/file.dst"]
        assert "/storage/file.dst" in entry.resolved

    def test_multiple_replicas_picks_first(self):
        """When multiple replicas exist, the first one is used."""
        root = {
            "/lhcb/data/file.dst": {
                "replicas": [
                    {"url": "file:///first/file.dst", "se": "SE-1"},
                    {"url": "file:///second/file.dst", "se": "SE-2"},
                ]
            }
        }
        replica_map = ReplicaMap(root=root)
        mapper = self._make_mapper(replica_map)
        obj = {"class": "File", "location": "LFN:/lhcb/data/file.dst"}
        mapper.visit(obj, "/stagedir", "/basedir")
        entry = mapper._pathmap["LFN:/lhcb/data/file.dst"]
        assert "first" in entry.resolved

    def test_size_set_from_replica_map(self):
        """visit() should annotate obj with size_bytes from the replica map."""
        replica_map = _make_replica_map_with_meta(
            "/lhcb/data/file.dst",
            "file:///storage/file.dst",
            size_bytes=1048576,
        )
        mapper = self._make_mapper(replica_map)
        obj = {"class": "File", "location": "LFN:/lhcb/data/file.dst"}
        mapper.visit(obj, "/stagedir", "/basedir")
        assert obj.get("size") == 1048576

    def test_checksum_set_from_replica_map(self):
        """visit() should annotate obj with checksum from the replica map."""
        replica_map = _make_replica_map_with_meta(
            "/lhcb/data/file.dst",
            "file:///storage/file.dst",
            adler32="788c5caa",
        )
        mapper = self._make_mapper(replica_map)
        obj = {"class": "File", "location": "LFN:/lhcb/data/file.dst"}
        mapper.visit(obj, "/stagedir", "/basedir")
        assert obj.get("checksum") == "adler32$788c5caa"

    def test_existing_size_not_overwritten(self):
        """visit() should not overwrite size if already present on obj."""
        replica_map = _make_replica_map_with_meta(
            "/lhcb/data/file.dst",
            "file:///storage/file.dst",
            size_bytes=1048576,
        )
        mapper = self._make_mapper(replica_map)
        obj = {"class": "File", "location": "LFN:/lhcb/data/file.dst", "size": 9999}
        mapper.visit(obj, "/stagedir", "/basedir")
        # Existing size is preserved
        assert obj["size"] == 9999

    def test_remote_url_mapped_directly(self):
        """root:// URLs should be mapped directly without staging."""
        mapper = self._make_mapper(ReplicaMap(root={}))
        obj = {
            "class": "File",
            "location": "root://eoslhcb.cern.ch//eos/lhcb/data/file.dst",
        }
        mapper.visit(obj, "/stagedir", "/basedir")
        loc = "root://eoslhcb.cern.ch//eos/lhcb/data/file.dst"
        assert loc in mapper._pathmap
        entry = mapper._pathmap[loc]
        assert entry.resolved == loc
        assert entry.staged is False

    def test_missing_lfn_does_not_add_to_pathmap(self):
        """LFN missing from replica map should not be added to _pathmap by DiracPathMapper.

        When the LFN is absent, DiracPathMapper falls through to the parent PathMapper.visit().
        The key invariant is that DiracPathMapper itself did NOT add the LFN key to _pathmap
        before delegating to the parent. We verify this by checking the _pathmap state after
        the visit call (ignoring any error from the parent's attempt to handle the unknown path).
        """
        mapper = self._make_mapper(ReplicaMap(root={}))
        obj = {"class": "File", "location": "LFN:/lhcb/data/MISSING.dst"}
        try:
            mapper.visit(obj, "/stagedir", "/basedir")
        except Exception:
            pass  # Parent may raise for unknown paths — that's OK
        # DiracPathMapper must NOT have added the LFN key itself
        assert "LFN:/lhcb/data/MISSING.dst" not in mapper._pathmap

    def test_staged_false_for_lfn(self):
        """LFN-resolved entries should have staged=False (no local copy needed)."""
        replica_map = _make_replica_map(
            ("/lhcb/data/file.dst", "file:///storage/file.dst")
        )
        mapper = self._make_mapper(replica_map)
        obj = {"class": "File", "location": "LFN:/lhcb/data/file.dst"}
        mapper.visit(obj, "/stagedir", "/basedir")
        entry = mapper._pathmap["LFN:/lhcb/data/file.dst"]
        assert entry.staged is False


# ---------------------------------------------------------------------------
# Tests: DiracCommandLineTool.make_path_mapper
# ---------------------------------------------------------------------------


class TestDiracCommandLineTool:
    """Test DiracCommandLineTool.make_path_mapper and dirac_make_tool."""

    def _make_runtime_context(self, replica_map=None):
        ctx = types.SimpleNamespace(basedir="/base", replica_map=replica_map)
        return ctx

    def test_make_path_mapper_with_replica_map_returns_dirac_mapper(self):
        """When replica_map is set on context, make_path_mapper returns DiracPathMapper."""
        replica_map = _make_replica_map(
            ("/lhcb/data/file.dst", "file:///storage/file.dst")
        )
        ctx = self._make_runtime_context(replica_map=replica_map)
        mapper = DiracCommandLineTool.make_path_mapper([], "/stagedir", ctx, True)
        assert isinstance(mapper, DiracPathMapper)

    def test_make_path_mapper_without_replica_map_returns_default(self):
        """When no replica_map is on context, make_path_mapper returns base PathMapper."""
        ctx = types.SimpleNamespace(basedir="/base")  # no replica_map attr
        mapper = DiracCommandLineTool.make_path_mapper([], "/stagedir", ctx, True)
        # Should NOT be a DiracPathMapper — should be the plain PathMapper
        assert not isinstance(mapper, DiracPathMapper)

    def test_make_path_mapper_replica_map_none_returns_default(self):
        """replica_map=None on context falls through to plain PathMapper."""
        ctx = self._make_runtime_context(replica_map=None)
        mapper = DiracCommandLineTool.make_path_mapper([], "/stagedir", ctx, True)
        assert not isinstance(mapper, DiracPathMapper)

    def test_dirac_make_tool_commandlinetool_returns_dirac_instance(self):
        """dirac_make_tool should return DiracCommandLineTool for CommandLineTool class.

        We mock DiracCommandLineTool's __init__ so we don't need a full cwltool toolpath.
        The test verifies routing: the factory creates a DiracCommandLineTool for
        toolpath_objects whose class is 'CommandLineTool'.
        """
        toolpath = {"class": "CommandLineTool", "baseCommand": "echo"}
        loading_ctx = types.SimpleNamespace()
        sentinel = MagicMock(spec=DiracCommandLineTool)
        with patch.object(
            _tool_mod, "DiracCommandLineTool", return_value=sentinel
        ) as mock_cls:
            result = dirac_make_tool(toolpath, loading_ctx)
        mock_cls.assert_called_once_with(toolpath, loading_ctx)
        assert result is sentinel

    def test_dirac_make_tool_delegates_workflow(self):
        """dirac_make_tool should delegate non-CommandLineTool classes to default_make_tool."""
        toolpath = {"class": "Workflow", "steps": []}
        loading_ctx = types.SimpleNamespace()
        sentinel = MagicMock()
        with patch.object(
            _tool_mod, "default_make_tool", return_value=sentinel
        ) as mock_fn:
            result = dirac_make_tool(toolpath, loading_ctx)
        mock_fn.assert_called_once_with(toolpath, loading_ctx)
        assert result is sentinel

    def test_dirac_make_tool_delegates_expression_tool(self):
        """dirac_make_tool delegates ExpressionTool to default_make_tool."""
        toolpath = {"class": "ExpressionTool"}
        loading_ctx = types.SimpleNamespace()
        sentinel = MagicMock()
        with patch.object(
            _tool_mod, "default_make_tool", return_value=sentinel
        ) as mock_fn:
            result = dirac_make_tool(toolpath, loading_ctx)
        mock_fn.assert_called_once_with(toolpath, loading_ctx)
        assert result is sentinel
