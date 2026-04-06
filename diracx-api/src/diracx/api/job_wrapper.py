#!/usr/bin/env python
"""Job wrapper for executing CWL workflows with DIRAC.

WARNING: Do not import cwltool in this module.
cwltool is mypyc-compiled and must be patched before first import.
CWL execution happens via dirac-cwl-run subprocess.
"""

from __future__ import annotations

import json
import logging
import random
import shutil
import subprocess
from pathlib import Path
from typing import Any, Sequence, cast

from cwl_utils.parser import (
    save,
)
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    File,
    Saveable,
    Workflow,
)
from DIRACCommon.Core.Utilities.ReturnValues import (  # type: ignore[import-untyped]
    returnValueOrRaise,
)
from ruamel.yaml import YAML

from diracx.api.job_report import JobReport
from diracx.api.jobs import create_sandbox, download_sandbox
from diracx.client.aio import AsyncDiracClient  # type: ignore[attr-defined]
from diracx.core.exceptions import WorkflowProcessingError
from diracx.core.models.commands import (
    PostProcessCommand,
    PreProcessCommand,
    StoreOutputDataCommand,
)
from diracx.core.models.cwl import JobHint
from diracx.core.models.cwl_submission import JobInputModel, JobModel
from diracx.core.models.job import JobMinorStatus, JobStatus

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
        self, inputs: JobInputModel, job_hint: JobHint, job_path: Path
    ) -> None:
        """Download input sandbox files using the dirac:Job hint's source references.

        Each ``input_sandbox`` entry in the hint references a CWL input by ID.
        The files are downloaded from the sandbox store and extracted into
        the job working directory.

        :param inputs: The job input model containing CWL input values.
        :param job_hint: The dirac:Job hint with input_sandbox config.
        :param job_path: Path to the job working directory.
        """
        if not job_hint.input_sandbox:
            return

        self._job_report.set_job_status(
            minor_status=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX
        )

        for ref in job_hint.input_sandbox:
            cwl_value = inputs.cwl.get(ref.source)
            if cwl_value is None:
                continue

            # Extract file paths from CWL value
            file_paths = self.__extract_file_paths_from_cwl_value(cwl_value)
            for file_path in file_paths:
                await download_sandbox(file_path, job_path)

        logger.info("Input sandbox files downloaded successfully")

    @staticmethod
    def __extract_file_paths_from_cwl_value(cwl_value: Any) -> list[str]:
        """Extract file paths/sandbox IDs from a CWL input value."""
        paths: list[str] = []
        if not isinstance(cwl_value, list):
            cwl_value = [cwl_value]
        for item in cwl_value:
            path = None
            if isinstance(item, dict):
                path = item.get("path") or item.get("location")
            elif hasattr(item, "path"):
                path = item.path
            if path and isinstance(path, str):
                paths.append(path)
        return paths

    async def __upload_output_sandbox(
        self,
        outputs: dict[str, str | Path | Sequence[str | Path]],
    ):
        outputs_to_sandbox = []
        for output_name, src_path in outputs.items():
            if self._output_sandbox and output_name in self._output_sandbox:
                if isinstance(src_path, (Path, str)):
                    src_path = [Path(src_path)]
                for path in src_path:
                    outputs_to_sandbox.append(Path(path))
        if outputs_to_sandbox:
            self._job_report.set_job_status(
                JobStatus.COMPLETING,
                minor_status=JobMinorStatus.UPLOADING_OUTPUT_SANDBOX,
            )
            sb_path = Path(await create_sandbox(outputs_to_sandbox))
            logger.info(
                "Successfully stored output %s in Sandbox %s",
                self._output_sandbox,
                sb_path,
            )
            await self._diracx_client.jobs.assign_sandbox_to_job(
                self._job_id, f'"{sb_path}"'
            )
            self._job_report.set_job_status(
                JobStatus.COMPLETING,
                minor_status=JobMinorStatus.OUTPUT_SANDBOX_UPLOADED,
            )

    async def __download_input_data(
        self, inputs: JobInputModel, job_path: Path
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
            cwl_value = inputs.cwl.get(source_id)
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
        - {"class": "File", "path": "LFN:/path/to/file"}
        - [{"class": "File", "path": "LFN:/path/to/file"}, ...]
        - A cwl_utils File object with a .path attribute
        """
        lfns: list[str] = []
        if not isinstance(cwl_value, list):
            cwl_value = [cwl_value]
        for item in cwl_value:
            path = None
            if isinstance(item, dict):
                path = item.get("path") or item.get("location")
            elif hasattr(item, "path"):
                path = item.path
            if path and isinstance(path, str):
                # Normalize LFN: prefix
                lfns.append(
                    path.removeprefix("LFN:") if path.startswith("LFN:") else path
                )
        return lfns

    def __build_replica_map(self, datamanager, lfns: list[str], job_path: Path) -> None:
        """Query replica info and write a replica_map.json for dirac-cwl-run.

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

    def __update_inputs(
        self, inputs: JobInputModel, updates: dict[str, Path | list[Path]]
    ):
        """Update CWL job inputs with new file paths.

        This method updates the `inputs.cwl` object by replacing or adding
        file paths for each input specified in `updates`. It supports both
        single files and lists of files.

        :param inputs: The job input model whose `cwl` dictionary will be updated.
        :type inputs: JobInputModel
        :param updates: Dictionary mapping input names to their corresponding local file
            paths. Each value can be a single `Path` or a list of `Path` objects.
        :type updates: dict[str, Path | list[Path]]

        .. note::
           This method is typically called after downloading LFNs
           using `download_lfns` to ensure that the CWL job inputs reference
           the correct local files.
        """
        for _, value in inputs.cwl.items():
            files = value if isinstance(value, list) else [value]
            for file in files:
                if isinstance(file, File) and file.path:
                    file.path = Path(file.path).name
        for input_name, path in updates.items():
            if isinstance(path, Path):
                inputs.cwl[input_name] = File(path=str(path))
            else:
                inputs.cwl[input_name] = []
                for p in path:
                    inputs.cwl[input_name].append(File(path=str(p)))

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
        outputs = json.loads(stdout)
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
        executable: CommandLineTool | Workflow | ExpressionTool,
        arguments: JobInputModel | None,
        job_hint: JobHint,
    ) -> None:
        """Pre-process the job before execution.

        Writes the CWL task and parameters to disk, downloads input sandbox
        and input data as declared in the dirac:Job hint.
        """
        logger = logging.getLogger("JobWrapper - Pre-process")

        # Write CWL task to file
        logger.info("Preparing the task...")
        task_dict = save(executable)
        task_path = self._job_path / "task.cwl"
        with open(task_path, "w") as task_file:
            YAML().dump(task_dict, task_file)

        if arguments:
            # Download input sandbox using hint source references
            if job_hint.input_sandbox:
                logger.info("Downloading input sandbox files...")
                await self.__download_input_sandbox(arguments, job_hint, self._job_path)

            # Download input data (LFNs) using hint source references
            if job_hint.input_data:
                updates = await self.__download_input_data(arguments, self._job_path)
                self.__update_inputs(arguments, updates)

            # Write input parameters to file
            logger.info("Preparing the parameters...")
            parameter_dict = save(cast(Saveable, arguments.cwl))
            parameter_path = self._job_path / "parameter.cwl"
            with open(parameter_path, "w") as parameter_file:
                YAML().dump(parameter_dict, parameter_file)

        if self._preprocess_commands:
            await self.__run_preprocess_commands(self._job_path)

        await self._job_report.commit()

    async def post_process(
        self,
        status: int,
        stdout: str,
        stderr: str,
    ):
        """Post-process the job after execution.

        :return: True if the job is post-processed successfully, False otherwise
        """
        logger = logging.getLogger("JobWrapper - Post-process")
        if status != 0:
            raise RuntimeError(f"Error {status} during the task execution.")

        logger.info(stdout)
        logger.info(stderr)

        outputs = self.__parse_output_filepaths(stdout)

        success = True

        if self._postprocess_commands:
            success = await self.__run_postprocess_commands(
                self._job_path, outputs=outputs
            )

        await self.__upload_output_sandbox(outputs=outputs)
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

    async def run_job(self, job: JobModel) -> bool:
        """Execute a given CWL workflow using dirac-cwl run via subprocess.

        This is the equivalent of the DIRAC JobWrapper.

        :param job: The job model containing workflow and inputs.
        :return: True if the job is executed successfully, False otherwise.
        """
        logger = logging.getLogger("JobWrapper")

        # Extract dirac:Job hint and build commands from type + I/O config
        job_hint = JobHint.from_cwl(job.task)
        self._build_commands_from_hint(job_hint)

        # Isolate the job in a specific directory
        self._job_path = Path(".") / "workernode" / f"{random.randint(1000, 9999)}"  # noqa: S311
        self._job_path.mkdir(parents=True, exist_ok=True)

        try:
            # Pre-process the job
            logger.info("Pre-processing Task...")
            await self.pre_process(job.task, job.input, job_hint)
            logger.info("Task pre-processed successfully!")

            # Build dirac-cwl-run command (different interface from cwltool)
            task_file = self._job_path / "task.cwl"
            param_file = self._job_path / "parameter.cwl"
            command = ["dirac-cwl-run", str(task_file.name)]
            if param_file.exists():
                command.append(str(param_file.name))
            if self._replica_map_path and self._replica_map_path.exists():
                command.extend(["--replica-map", str(self._replica_map_path.name)])

            # Execute the task
            logger.info("Executing Task: %s", command)
            self._job_report.set_job_status(minor_status=JobMinorStatus.APPLICATION)
            await self._job_report.commit()
            result = subprocess.run(  # noqa: S603
                command, capture_output=True, text=True, cwd=self._job_path
            )

            if result.returncode != 0:
                logger.error("Error in executing workflow:\n%s", result.stderr)
                self._job_report.set_job_status(
                    JobStatus.COMPLETING, minor_status=JobMinorStatus.APP_ERRORS
                )
                self._job_report.set_job_status(JobStatus.FAILED)
                return False
            logger.info("Task executed successfully!")
            self._job_report.set_job_status(
                JobStatus.COMPLETING, minor_status=JobMinorStatus.APP_SUCCESS
            )
            # Post-process the job
            logger.info("Post-processing Task...")
            if await self.post_process(
                result.returncode,
                result.stdout,
                result.stderr,
            ):
                logger.info("Task post-processed successfully!")
                self._job_report.set_job_status(
                    JobStatus.DONE, JobMinorStatus.EXEC_COMPLETE
                )
                return True
            logger.error("Failed to post-process Task")
            self._job_report.set_job_status(JobStatus.FAILED)
            return False

        except Exception:
            logger.exception("JobWrapper: Failed to execute workflow")
            self._job_report.set_job_status(JobStatus.FAILED)
            return False
        finally:
            # Commit all stored job reports
            await self._job_report.commit()
            # Clean up
            if self._job_path.exists():
                shutil.rmtree(self._job_path)
