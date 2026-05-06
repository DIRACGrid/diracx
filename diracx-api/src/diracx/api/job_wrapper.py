#!/usr/bin/env python
"""Job wrapper for executing CWL workflows with DIRAC."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import shutil
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, Sequence

from DIRACCommon.Core.Utilities.ReturnValues import (  # type: ignore[import-untyped]
    returnValueOrRaise,
)
from ruamel.yaml import YAML

from diracx.api.job_monitor import JobMonitor, KillCommandReceived, send_final_heartbeat
from diracx.api.job_report import JobReport
from diracx.api.jobs import create_sandbox, download_sandbox
from diracx.api.prmon_reader import PrmonFifoReader
from diracx.client.aio import AsyncDiracClient  # type: ignore[attr-defined]
from diracx.core.exceptions import WorkflowProcessingError
from diracx.core.models.commands import (
    PostProcessCommand,
    PreProcessCommand,
    StoreOutputDataCommand,
)
from diracx.core.models.cwl import JobHint
from diracx.core.models.job import JobMinorStatus, JobStatus

# cwltool lifecycle patterns worth reporting as ApplicationStatus.
# Anchored to a log-level prefix to avoid false positives from user output.
# Group 1 captures from the bracket onward, stripping the prefix.
_CWLTOOL_STATUS_RE = re.compile(
    r"(?:INFO|WARNING|ERROR) "
    r"(\[(?:job |step )?[^\]]+\]"
    r" (?:completed \w+|start(?:ing \S+)?|will be skipped"
    r"|exited with status: \d+|was terminated by signal: \w+"
    r"|Iteration \d+ completed \w+))"
)

# -----------------------------------------------------------------------------
# JobWrapper
# -----------------------------------------------------------------------------

logger = logging.getLogger(__name__)


class JobWrapper:
    """Job Wrapper for the execution hook."""

    def __init__(self, job_id: int) -> None:
        """Initialize the job wrapper."""
        self._preprocess_commands: list[PreProcessCommand] = []
        self._postprocess_commands: list[PostProcessCommand] = []
        self._output_sandbox: list[str] = []
        self._input_data_sources: list[str] = []
        self._input_sandbox_sources: list[str] = []
        self._replica_map_path: Path | None = None
        self._job_path: Path = Path()
        self._job_id = job_id
        src = "JobWrapper"
        self._diracx_client: AsyncDiracClient = AsyncDiracClient()
        self._job_report: JobReport = JobReport(self._job_id, src, self._diracx_client)
        self._job_report.set_job_status(
            JobStatus.RUNNING, JobMinorStatus.JOB_INITIALIZATION
        )

    async def __download_input_sandbox(
        self, inputs: dict[str, Any], job_hint: JobHint, job_path: Path
    ) -> dict[str, Path]:
        """Download input sandbox files and return SB: → local path mappings.

        Parses SB: prefixed paths from CWL input values (identified via the
        hint's input_sandbox source references), downloads and extracts sandbox
        tars (cached per unique SB: reference), and returns a mapping of full
        SB: URIs to their local extracted file paths for replica map injection.

        :param inputs: The job input model containing CWL input values.
        :param job_hint: The dirac:Job hint with input_sandbox config.
        :param job_path: Path to the job working directory.
        :return: Dict mapping SB: URI strings to local file Paths.
        """
        sandbox_mappings: dict[str, Path] = {}
        if not job_hint.input_sandbox:
            return sandbox_mappings

        self._job_report.set_job_status(
            minor_status=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX
        )

        # Cache: download each sandbox tar only once
        downloaded_sb_refs: set[str] = set()

        for ref in job_hint.input_sandbox:
            cwl_value = inputs.get(ref.source)
            if cwl_value is None:
                continue

            # Extract file paths from CWL value
            file_paths = self.__extract_file_paths_from_cwl_value(cwl_value)
            for file_path in file_paths:
                if not file_path.startswith("SB:"):
                    logger.warning(
                        "Skipping non-SB: path in input_sandbox: %s", file_path
                    )
                    continue
                sb_ref, rel_path = self.parse_sb_path(file_path)
                # Download + extract once per unique sandbox reference
                if sb_ref not in downloaded_sb_refs:
                    await download_sandbox(sb_ref, job_path)
                    downloaded_sb_refs.add(sb_ref)
                # Map the full SB: URI to the local extracted file
                sandbox_mappings[file_path] = job_path / rel_path

        logger.info("Input sandbox files downloaded successfully")
        return sandbox_mappings

    @staticmethod
    def __extract_file_paths_from_cwl_value(cwl_value: Any) -> list[str]:
        """Extract file paths/sandbox IDs from a CWL input value."""
        paths: list[str] = []
        if not isinstance(cwl_value, list):
            cwl_value = [cwl_value]
        for item in cwl_value:
            path = None
            if isinstance(item, dict):
                path = item.get("location") or item.get("path")
            elif hasattr(item, "location"):
                path = item.location or getattr(item, "path", None)
            elif hasattr(item, "path"):
                path = item.path
            if path and isinstance(path, str):
                paths.append(path)
        return paths

    @staticmethod
    def parse_sb_path(path: str) -> tuple[str, str]:
        """Parse an SB: URI into sandbox reference and relative path.

        Format: SB:<se_name>|<s3_path>#<relative_path_inside_tar>

        The SB: prefix is preserved in the returned reference — it is
        the canonical form used by the DiracX API for sandbox operations.

        :param path: Full SB: URI string (e.g. ``SB:SandboxSE|/S3/...#file.sh``)
        :return: Tuple of (sb_ref, relative_path) where sb_ref includes ``SB:``
        :raises ValueError: If path is not a valid SB: reference
        """
        if not path.startswith("SB:"):
            raise ValueError(f"Not an SB: path: {path}")
        if "#" not in path:
            raise ValueError(f"SB: path missing '#' fragment separator: {path}")
        sb_ref, rel_path = path.split("#", 1)
        return sb_ref, rel_path

    async def __upload_output_sandbox(
        self,
        outputs: dict[str, str | Path | Sequence[str | Path]],
    ):
        logger.info(
            "Output sandbox upload: declared sources=%s, available output keys=%s",
            self._output_sandbox,
            list(outputs.keys()),
        )
        outputs_to_sandbox = []
        for output_name, src_path in outputs.items():
            in_sandbox = bool(
                self._output_sandbox and output_name in self._output_sandbox
            )
            logger.info(
                "  output %r (in_sandbox=%s) -> %r",
                output_name,
                in_sandbox,
                src_path,
            )
            if in_sandbox:
                if isinstance(src_path, (Path, str)):
                    src_path = [Path(src_path)]
                for path in src_path:
                    outputs_to_sandbox.append(Path(path))
        if outputs_to_sandbox:
            logger.info(
                "Uploading %d file(s) to output sandbox: %s",
                len(outputs_to_sandbox),
                outputs_to_sandbox,
            )
            self._job_report.set_job_status(
                JobStatus.COMPLETING,
                minor_status=JobMinorStatus.UPLOADING_OUTPUT_SANDBOX,
            )
            sb_ref = await create_sandbox(outputs_to_sandbox)
            logger.info(
                "Successfully stored output %s in Sandbox %s",
                self._output_sandbox,
                sb_ref,
            )
            await self._diracx_client.jobs.assign_sandbox_to_job(
                self._job_id, f'"{sb_ref}"'
            )
            self._job_report.set_job_status(
                JobStatus.COMPLETING,
                minor_status=JobMinorStatus.OUTPUT_SANDBOX_UPLOADED,
            )
        else:
            logger.warning(
                "Output sandbox upload skipped: no files matched declared sources %s",
                self._output_sandbox,
            )

    async def __download_input_data(
        self, inputs: dict[str, Any], job_path: Path
    ) -> dict[str, Path | list[Path]]:
        """Download LFNs into the job working directory and build a replica map.

        Uses ``self._input_data_sources`` (from the dirac:Job hint) to identify
        which CWL inputs contain LFN references, then resolves the actual LFN
        paths from the CWL input values.

        :param inputs: The job input model containing CWL input values.
        :param job_path: Path to the job working directory.
        :return: Mapping of input names to downloaded local file paths.
        """
        from DIRAC.DataManagementSystem.Client.DataManager import (
            DataManager,  # type: ignore[import-untyped]
        )

        if not self._input_data_sources:
            return {}

        new_paths: dict[str, Path | list[Path]] = {}
        self._job_report.set_job_status(
            minor_status=JobMinorStatus.INPUT_DATA_RESOLUTION
        )

        datamanager = DataManager()

        # Extract LFNs from CWL inputs using the hint's source references
        lfns_by_input: dict[str, list[str]] = {}
        for source_id in self._input_data_sources:
            cwl_value = inputs.get(source_id)
            if cwl_value is None:
                continue
            lfns = self.__extract_lfns_from_cwl_value(cwl_value)
            if lfns:
                lfns_by_input[source_id] = lfns

        if not lfns_by_input:
            return {}

        # Collect all LFNs for replica map generation
        all_lfns = [lfn for lfns in lfns_by_input.values() for lfn in lfns]
        if all_lfns:
            self.__build_replica_map(datamanager, all_lfns, job_path)

        # Download files
        for input_name, lfns in lfns_by_input.items():
            res = returnValueOrRaise(datamanager.getFile(lfns, str(job_path)))
            if res["Failed"]:
                raise RuntimeError(f"Could not get files: {res['Failed']}")
            paths = res["Successful"]
            if paths:
                downloaded = [
                    Path(paths[lfn]).relative_to(job_path.resolve())
                    for lfn in lfns
                    if lfn in paths
                ]
                if len(downloaded) == 1:
                    new_paths[input_name] = downloaded[0]
                else:
                    new_paths[input_name] = downloaded

        return new_paths

    @staticmethod
    def __extract_lfns_from_cwl_value(cwl_value: Any) -> list[str]:
        """Extract LFN paths from a CWL input value.

        CWL File values can be:
        - {"class": "File", "location": "LFN:/path/to/file"}
        - [{"class": "File", "location": "LFN:/path/to/file"}, ...]
        - A cwl_utils File object with a .location or .path attribute
        """
        lfns: list[str] = []
        if not isinstance(cwl_value, list):
            cwl_value = [cwl_value]
        for item in cwl_value:
            path = None
            if isinstance(item, dict):
                path = item.get("location") or item.get("path")
            elif hasattr(item, "location"):
                path = item.location or getattr(item, "path", None)
            elif hasattr(item, "path"):
                path = item.path
            if path and isinstance(path, str):
                if not path.startswith("LFN:"):
                    logger.warning("Skipping non-LFN path in input_data: %s", path)
                    continue
                lfns.append(path.removeprefix("LFN:"))
        return lfns

    def __build_replica_map(self, datamanager, lfns: list[str], job_path: Path) -> None:
        """Query replica info and write a replica_map.json for dirac-cwl-runner.

        Uses DataManager's FileCatalog to get replicas and file metadata,
        then builds a ReplicaMap model matching dirac-cwl's expected format.
        """
        from diracx.core.models.replica_map import ReplicaMap

        # Get active replicas with URLs: {lfn: {se: pfn_url}}
        replica_result = returnValueOrRaise(
            datamanager.getActiveReplicas(lfns, getUrl=True)
        )
        successful_replicas = replica_result.get("Successful", {})

        if not successful_replicas:
            return

        # Get file metadata (size, checksum, GUID)
        metadata_result = datamanager.fileCatalog.getFileMetadata(
            list(successful_replicas.keys())
        )
        metadata = {}
        if metadata_result["OK"]:
            metadata = metadata_result["Value"].get("Successful", {})

        entries: dict[str, dict] = {}
        for lfn, se_pfn_map in successful_replicas.items():
            replicas = [{"url": pfn, "se": se} for se, pfn in se_pfn_map.items()]
            if not replicas:
                continue

            entry: dict[str, Any] = {"replicas": replicas}

            lfn_meta = metadata.get(lfn, {})
            if "Size" in lfn_meta:
                entry["size_bytes"] = lfn_meta["Size"]
            checksum: dict[str, str] = {}
            if "Checksum" in lfn_meta:
                checksum["adler32"] = lfn_meta["Checksum"]
            if "GUID" in lfn_meta:
                checksum["guid"] = lfn_meta["GUID"]
            if checksum:
                entry["checksum"] = checksum

            entries[lfn] = entry

        if entries:
            replica_map = ReplicaMap.model_validate(entries)
            replica_map_path = job_path / "replica_map.json"
            with open(replica_map_path, "w") as f:
                f.write(replica_map.model_dump_json(indent=2))
            self._replica_map_path = replica_map_path
            logger.info("Built replica map with %d entries", len(entries))

    def _add_sandbox_entries_to_replica_map(
        self, sandbox_mappings: dict[str, Path], job_path: Path
    ) -> None:
        """Inject sandbox file mappings into the replica map JSON.

        Each entry maps an SB: path to a local file:// URL so the CWL executor
        can resolve sandbox files through the same replica map as LFN files.

        :param sandbox_mappings: Dict of SB: path → local extracted file Path
        :param job_path: Job working directory
        """
        from diracx.core.models.replica_map import ReplicaMap

        # Load existing replica map or start fresh
        entries: dict[str, dict] = {}
        if self._replica_map_path and self._replica_map_path.exists():
            existing = ReplicaMap.model_validate_json(
                self._replica_map_path.read_text()
            )
            entries = {
                k: json.loads(v.model_dump_json()) for k, v in existing.root.items()
            }

        # Add sandbox entries
        for sb_path, local_path in sandbox_mappings.items():
            entries[sb_path] = {
                "replicas": [{"url": local_path.resolve().as_uri(), "se": "local"}],
            }

        if entries:
            replica_map = ReplicaMap.model_validate(entries)
            replica_map_path = job_path / "replica_map.json"
            with open(replica_map_path, "w") as f:
                f.write(replica_map.model_dump_json(indent=2))
            self._replica_map_path = replica_map_path
            logger.info(
                "Added %d sandbox entries to replica map", len(sandbox_mappings)
            )

    def __update_inputs(
        self, inputs: dict[str, Any], updates: dict[str, Path | list[Path]]
    ):
        """Replace File entries in the inputs dict with downloaded local paths.

        Existing File entries have their ``path`` flattened to the basename
        (matching cwltool's working-dir staging convention). Each entry in
        *updates* is then written as a fresh ``{class: File, path: ...}``
        dict (or list thereof) under its input id.
        """
        for value in inputs.values():
            files = value if isinstance(value, list) else [value]
            for file in files:
                if (
                    isinstance(file, dict)
                    and file.get("class") == "File"
                    and file.get("path")
                ):
                    file["path"] = Path(file["path"]).name
        for input_name, path in updates.items():
            if isinstance(path, Path):
                inputs[input_name] = {"class": "File", "path": str(path)}
            else:
                inputs[input_name] = [{"class": "File", "path": str(p)} for p in path]

    def __parse_output_filepaths(
        self, stdout: str
    ) -> dict[str, str | Path | Sequence[str | Path]]:
        """Get the outputted filepaths per output.

        :param str stdout:
            The console output of the the job

        :return dict[str, list[str]]:
            The dict of the list of filepaths for each output
        """
        outputted_files: dict[str, str | Path | Sequence[str | Path]] = {}
        # Use raw_decode to tolerate trailing non-JSON data (e.g. prmon
        # warnings written to stdout after the CWL JSON output).
        outputs, _ = json.JSONDecoder().raw_decode(stdout.lstrip())
        for output, files in outputs.items():
            if not files:
                continue
            if not isinstance(files, list):
                files = [files]
            file_paths = []
            for file in files:
                if file:
                    file_paths.append(str(file["path"]))
            outputted_files[output] = file_paths
        return outputted_files

    async def pre_process(
        self,
        params: dict[str, Any] | None,
        job_hint: JobHint,
    ) -> Path | None:
        """Download input sandbox/data and (re)write the parameters file.

        Returns the path to the rewritten parameters file (or None if no
        params were supplied). The workflow YAML is already on disk —
        dirac-cwl-runner is pointed at it directly.
        """
        logger = logging.getLogger("JobWrapper - Pre-process")
        logger.info("Preparing the task...")

        params_path: Path | None = None
        if params is not None:
            # Download input sandbox and collect SB: → local path mappings
            sandbox_mappings: dict[str, Path] = {}
            if job_hint.input_sandbox:
                logger.info("Downloading input sandbox files...")
                sandbox_mappings = await self.__download_input_sandbox(
                    params, job_hint, self._job_path
                )

            # Download input data (LFNs) using hint source references
            if job_hint.input_data:
                updates = await self.__download_input_data(params, self._job_path)
                self.__update_inputs(params, updates)

            # Inject sandbox entries into replica map
            if sandbox_mappings:
                self._add_sandbox_entries_to_replica_map(
                    sandbox_mappings, self._job_path
                )

            # Write the (possibly mutated) parameters file for dirac-cwl-runner.
            logger.info("Preparing the parameters...")
            params_path = self._job_path / "parameter.yaml"
            with open(params_path, "w") as f:
                YAML().dump(params, f)

        if self._preprocess_commands:
            await self.__run_preprocess_commands(self._job_path)

        await self._job_report.commit()
        return params_path

    async def post_process(
        self,
        status: int,
        stdout: str,
        stderr: str,
    ) -> bool:
        """Post-process the job after execution.

        Runs for both success and failure so cwltool's output JSON is always
        logged and the output sandbox is attempted even on permanentFail —
        the latter is essential when cwltool emits partial outputs (e.g. via
        ``pickValue: all_non_null`` on diagnostic outputs) so users can see
        *why* a job failed.

        Reports its own infrastructure outcome (parsing/upload/postprocess
        commands) via the return value. Does **not** treat cwltool's
        non-zero exit as an exception — that's the caller's policy
        decision based on the status they passed in.

        :return: True if post-processing infrastructure ran cleanly,
            False if a postprocess command or upload step failed.
        """
        logger = logging.getLogger("JobWrapper - Post-process")
        logger.info("cwltool exit status: %d", status)
        logger.info(
            "---- cwltool output JSON ----\n%s\n---- end ----", stdout or "<empty>"
        )

        outputs: dict = {}
        try:
            outputs = self.__parse_output_filepaths(stdout)
        except (ValueError, json.JSONDecodeError) as e:
            logger.warning("Could not parse cwltool output JSON: %s", e)
        logger.info("Parsed output structure: %s", outputs)

        success = True
        if status == 0 and self._postprocess_commands:
            success = await self.__run_postprocess_commands(
                self._job_path, outputs=outputs
            )

        if outputs:
            try:
                await self.__upload_output_sandbox(outputs=outputs)
            except Exception:
                logger.exception("Output sandbox upload failed")
                success = False

        await self._job_report.commit()
        return success

    async def __run_preprocess_commands(self, job_path: Path, **kwargs: Any) -> None:
        """Run all pre-process commands."""
        for cmd in self._preprocess_commands:
            try:
                await cmd.execute(job_path, **kwargs)
            except Exception as e:
                msg = f"Command '{type(cmd).__name__}' failed during the pre-process stage: {e}"
                logger.exception(msg)
                raise WorkflowProcessingError(msg) from e

    async def __run_postprocess_commands(
        self,
        job_path: Path,
        outputs: dict[str, str | Path | Sequence[str | Path]] = {},
        **kwargs: Any,
    ) -> bool:
        """Run all post-process commands."""
        for cmd in self._postprocess_commands:
            try:
                await cmd.execute(job_path, outputs=outputs, **kwargs)
            except Exception as e:
                msg = f"Command '{type(cmd).__name__}' failed during the post-process stage: {e}"
                logger.exception(msg)
                raise WorkflowProcessingError(msg) from e
        return True

    def _build_commands_from_hint(self, job_hint: JobHint) -> None:
        """Build pre/post-process commands from the dirac:Job hint.

        The ``type`` field determines which commands are attached.
        I/O config from the hint is used to configure commands.
        """
        # Extract input I/O config from the hint
        self._input_data_sources = [ref.source for ref in job_hint.input_data]
        self._input_sandbox_sources = [ref.source for ref in job_hint.input_sandbox]
        self._output_sandbox = [ref.source for ref in job_hint.output_sandbox]

        # Extract output I/O config from the hint
        output_paths = {
            entry.source: entry.output_path for entry in job_hint.output_data
        }
        output_se = []
        for entry in job_hint.output_data:
            output_se.extend(entry.output_se)
        output_se = list(set(output_se))

        # Build post-process commands — output storage
        if output_paths:
            self._postprocess_commands.append(
                StoreOutputDataCommand(output_paths=output_paths, output_se=output_se)
            )

    async def run_job(
        self, workflow_path: Path, params_path: Path | None = None
    ) -> bool:
        """Execute a CWL workflow via dirac-cwl-runner.

        :param workflow_path: Path to the CWL workflow file on disk.
        :param params_path: Path to the CWL parameters file on disk
            (None if the workflow takes no inputs).
        :return: True on success, False on failure.
        """
        logger = logging.getLogger("JobWrapper")

        # Parse the workflow once for hint extraction and stdout/stderr
        # auto-collection. After that the dict is dropped — dirac-cwl-runner
        # reads the workflow file directly.
        workflow = YAML().load(workflow_path.read_text())
        job_hint = JobHint.from_cwl(workflow)
        self._build_commands_from_hint(job_hint)

        for out in workflow.get("outputs") or []:
            if not isinstance(out, dict):
                continue
            if out.get("type") in ("stdout", "stderr"):
                out_id = (out.get("id") or "").rsplit("#", 1)[-1]
                if out_id and out_id not in self._output_sandbox:
                    self._output_sandbox.append(out_id)

        # Read the input parameters (if any) so pre_process can mutate them
        # with downloaded sandbox/LFN paths before writing them back out.
        params: dict[str, Any] | None = None
        if params_path is not None:
            params = YAML().load(params_path.read_text())

        # Isolate the job in a specific directory
        self._job_path = Path(".") / "workernode" / f"{random.randint(1000, 9999)}"  # noqa: S311
        self._job_path.mkdir(parents=True, exist_ok=True)

        try:
            logger.info("Pre-processing Task...")
            updated_params_path = await self.pre_process(params, job_hint)
            logger.info("Task pre-processed successfully!")

            cwl_command = [
                "dirac-cwl-runner",
                str(workflow_path.resolve()),
                "--tmpdir-prefix",
                str(self._job_path.resolve()) + "/",
            ]
            if updated_params_path is not None:
                cwl_command.append(str(updated_params_path.name))
            if self._replica_map_path and self._replica_map_path.exists():
                cwl_command.extend(["--replica-map", str(self._replica_map_path.name)])

            # Wrap with prmon for resource monitoring
            # TODO: replace with CS config options
            heartbeat_interval = 60.0
            prmon_interval = 1
            stall_window = 1800.0  # seconds (30 minutes)

            if shutil.which("prmon") is None:
                raise RuntimeError(
                    "prmon not found in PATH -- required for job monitoring (part of DIRACOS2)"
                )

            # Create FIFO for streaming prmon output (no large TSV on disk)
            prmon_fifo = self._job_path / "prmon_fifo"
            os.mkfifo(prmon_fifo)
            fifo_reader = PrmonFifoReader(prmon_fifo)
            reader_task = asyncio.create_task(fifo_reader.run())

            # Use just filenames -- subprocess CWD is already job_path
            command = [
                "prmon",
                "--interval",
                str(prmon_interval),
                "--fast-memmon",
                "--units",
                "--filename",
                "prmon_fifo",
                "--json-summary",
                "prmon.json",
                "--",
                *cwl_command,
            ]

            # Execute the task
            logger.info("Executing Task: %s", command)
            self._job_report.set_job_status(minor_status=JobMinorStatus.APPLICATION)
            await self._job_report.commit()
            # HACK: PATH augmentation for the cwltool subprocess only.
            #
            #  1. CVMFS-bundled node, because cwl_utils.sandboxjs hardcodes
            #     a PATH lookup for `nodejs`/`node` to evaluate $(inputs[...])
            #     JS expressions and DIRACOS doesn't ship nodejs. Remove
            #     when nodejs lands in DIRACOS proper.
            #
            #  2. HACK ALERT: the job working directory itself, to make up
            #     for lb-prod-run-rs not being installed on the worker.
            #     The binary arrives via dirac:Job.input_sandbox and lands
            #     at `<job_path>/lb-prod-run-rs` after extraction, but each
            #     CommandLineTool's `baseCommand: [lb-prod-run-rs]` resolves
            #     against PATH, not the job dir. Remove when the binary is
            #     either (a) installed on workers via DIRACOS / CVMFS, or
            #     (b) declared as a typed File input on each tool and staged
            #     via InitialWorkDirRequirement.
            proc_env = os.environ.copy()
            proc_env["PATH"] = (
                "/cvmfs/lhcb.cern.ch/lib/var/lib/LbEnv/3886/stable/linux-64/bin:"
                + str(self._job_path.resolve())
                + ":"
                + proc_env.get("PATH", "")
            )
            proc = await asyncio.create_subprocess_exec(  # noqa: S603
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._job_path,
                env=proc_env,
            )
            assert proc.stderr is not None  # guaranteed by stderr=PIPE
            assert proc.stdout is not None  # guaranteed by stdout=PIPE

            # Shared deque for cwltool stderr -- monitor reads it for peek content
            cwltool_stderr: deque[str] = deque(maxlen=100)

            # Start the job monitor as a concurrent task
            monitor = JobMonitor(
                pid=proc.pid,
                job_path=self._job_path,
                job_report=self._job_report,
                cwltool_stderr=cwltool_stderr,
                heartbeat_interval=heartbeat_interval,
                fifo_reader=fifo_reader,
                stall_window=stall_window,
            )
            monitor_task = asyncio.create_task(monitor.run())

            # Stream stderr line-by-line while collecting stdout concurrently
            async def _collect_stdout() -> bytes:
                assert proc.stdout is not None
                return await proc.stdout.read()

            stdout_task = asyncio.create_task(_collect_stdout())

            stderr_lines: list[str] = []
            last_commit = time.monotonic()
            async for raw in proc.stderr:
                line = raw.decode().rstrip("\n")
                stderr_lines.append(line)
                cwltool_stderr.append(line)  # feed monitor's peek buffer
                # Always re-emit to stderr for Watchdog peek
                print(line, file=sys.stderr, flush=True)
                # Only report lifecycle transitions as ApplicationStatus
                match = _CWLTOOL_STATUS_RE.search(line)
                if match:
                    self._job_report.set_job_status(application_status=match.group(1))
                    now = time.monotonic()
                    if now - last_commit >= 2.0:
                        try:
                            await self._job_report.commit()
                        except Exception:
                            logger.warning(
                                "Failed to commit status update",
                                exc_info=True,
                            )
                        last_commit = now

            # Flush any remaining status updates
            try:
                await self._job_report.commit()
            except Exception:
                logger.warning("Failed to commit final status update", exc_info=True)

            stdout_bytes = await stdout_task
            stdout_text = stdout_bytes.decode()
            await proc.wait()

            # Stop the monitor
            monitor_task.cancel()
            try:
                await monitor_task
            except (asyncio.CancelledError, KillCommandReceived):
                pass

            # Wait for FIFO reader to finish (EOF from prmon exit)
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

            # Send final heartbeat with exit metrics
            await send_final_heartbeat(
                job_path=self._job_path,
                job_report=self._job_report,
                cwltool_stderr=cwltool_stderr,
                fifo_reader=fifo_reader,
            )

            # Write compressed time-series for output sandbox
            fifo_reader.write_compressed(self._job_path / "prmon_compressed.txt")

            if proc.returncode != 0:
                logger.error(
                    "Error in executing workflow:\n%s", "\n".join(stderr_lines)
                )
                self._job_report.set_job_status(
                    JobStatus.COMPLETING,
                    minor_status=JobMinorStatus.APP_ERRORS,
                    application_status=f"failed (exit {proc.returncode})",
                )
            else:
                logger.info("Task executed successfully!")
                self._job_report.set_job_status(
                    JobStatus.COMPLETING,
                    minor_status=JobMinorStatus.APP_SUCCESS,
                )

            # Post-process always runs — even on cwltool failure, so we log
            # the output JSON and attempt the output-sandbox upload (so users
            # can see *why* a job failed via partial outputs from
            # `pickValue: all_non_null`).
            logger.info("Post-processing Task...")
            returncode = proc.returncode if proc.returncode is not None else -1
            post_ok = await self.post_process(
                returncode,
                stdout_text,
                "\n".join(stderr_lines),
            )

            if returncode != 0:
                # cwltool failed — job is FAILED regardless of post-process
                # outcome. Diagnostic upload (if any) already happened inside
                # post_process.
                self._job_report.set_job_status(
                    JobStatus.FAILED, JobMinorStatus.APP_ERRORS
                )
                return False
            if not post_ok:
                logger.error("Post-processing infrastructure failed")
                self._job_report.set_job_status(JobStatus.FAILED)
                return False
            logger.info("Task post-processed successfully!")
            self._job_report.set_job_status(
                JobStatus.DONE, JobMinorStatus.EXEC_COMPLETE
            )
            return True

        except Exception:
            logger.exception("JobWrapper: Failed to execute workflow")
            self._job_report.set_job_status(JobStatus.FAILED)
            if "monitor_task" in locals():
                monitor_task.cancel()
                try:
                    await monitor_task
                except (asyncio.CancelledError, KillCommandReceived):
                    pass
                except Exception:
                    logger.warning("Error stopping monitor task", exc_info=True)
            if "reader_task" in locals():
                reader_task.cancel()
                try:
                    await reader_task
                except asyncio.CancelledError:
                    pass
            return False
        finally:
            # Commit all stored job reports
            await self._job_report.commit()
            # Clean up FIFO
            if "prmon_fifo" in locals():
                try:
                    prmon_fifo.unlink(missing_ok=True)
                except OSError:
                    pass
            # Clean up job directory
            if self._job_path.exists():
                shutil.rmtree(self._job_path)
