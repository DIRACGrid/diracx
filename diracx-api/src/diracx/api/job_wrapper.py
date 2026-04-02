#!/usr/bin/env python
"""Job wrapper for executing CWL workflows with DIRAC."""

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
from rich.text import Text
from ruamel.yaml import YAML

from diracx.api.cwl_utility import get_lfns
from diracx.api.job_report import JobMinorStatus, JobReport, JobStatus
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
        self._job_path: Path = Path()
        self._job_id = job_id
        src = "JobWrapper"
        self._diracx_client: AsyncDiracClient = AsyncDiracClient()
        self._job_report: JobReport = JobReport(self._job_id, src, self._diracx_client)
        self._job_report.set_job_status(
            JobStatus.RUNNING, JobMinorStatus.JOB_INITIALIZATION
        )

    async def __download_input_sandbox(
        self, arguments: JobInputModel, job_path: Path
    ) -> None:
        """Download the files from the sandbox store.

        :param arguments: Job input model containing sandbox information.
        :param job_path: Path to the job working directory.
        """
        assert arguments.sandbox is not None
        self._job_report.set_job_status(
            minor_status=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX
        )
        for sandbox in arguments.sandbox:
            await download_sandbox(sandbox, job_path)

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
        """Download LFNs into the job working directory.

        :param JobInputModel inputs:
            The job input model containing ``lfns_input``, a mapping from input names to one or more LFN paths.
        :param Path job_path:
            Path to the job working directory where files will be copied.

        :return dict[str, Path | list[Path]]:
            A dictionary mapping each input name to the corresponding downloaded
            file path(s) located in the working directory.
        """
        from DIRAC.DataManagementSystem.Client.DataManager import (
            DataManager,  # type: ignore[import-untyped]
        )

        new_paths: dict[str, Path | list[Path]] = {}
        self._job_report.set_job_status(
            minor_status=JobMinorStatus.INPUT_DATA_RESOLUTION
        )

        datamanager = DataManager()

        lfns_inputs = get_lfns(inputs.cwl)

        if lfns_inputs:
            for input_name, lfns in lfns_inputs.items():
                res = returnValueOrRaise(datamanager.getFile(lfns, str(job_path)))
                if res["Failed"]:
                    raise RuntimeError(f"Could not get files : {res['Failed']}")
                paths = res["Successful"]
                if paths and isinstance(lfns, list):
                    new_paths[input_name] = [
                        Path(paths[lfn]).relative_to(job_path.resolve())
                        for lfn in paths
                    ]
                elif paths and isinstance(lfns, str):
                    new_paths[input_name] = Path(paths[lfns]).relative_to(
                        job_path.resolve()
                    )
        return new_paths

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
    ) -> list[str]:
        """Pre-process the job before execution.

        :return: True if the job is pre-processed successfully, False otherwise
        """
        logger = logging.getLogger("JobWrapper - Pre-process")

        # Prepare the task for cwltool
        logger.info("Preparing the task for cwltool...")
        command = ["cwltool", "--parallel"]

        task_dict = save(executable)
        task_path = self._job_path / "task.cwl"
        with open(task_path, "w") as task_file:
            YAML().dump(task_dict, task_file)
        command.append(str(task_path.name))

        if arguments:
            if arguments.sandbox:
                # Download the files from the sandbox store
                logger.info("Downloading the files from the sandbox store...")
                await self.__download_input_sandbox(arguments, self._job_path)
                logger.info("Files downloaded successfully!")

            updates = await self.__download_input_data(arguments, self._job_path)
            self.__update_inputs(arguments, updates)

            logger.info("Preparing the parameters for cwltool...")
            parameter_dict = save(cast(Saveable, arguments.cwl))
            parameter_path = self._job_path / "parameter.cwl"
            with open(parameter_path, "w") as parameter_file:
                YAML().dump(parameter_dict, parameter_file)
            command.append(str(parameter_path.name))

        if self._preprocess_commands:
            await self.__run_preprocess_commands(self._job_path)

        await self._job_report.commit()
        return command

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
        # Extract I/O config from the hint
        output_paths = {
            entry.source: entry.output_path for entry in job_hint.output_data
        }
        output_se = []
        for entry in job_hint.output_data:
            output_se.extend(entry.output_se)
        output_se = list(set(output_se))

        self._output_sandbox = [ref.source for ref in job_hint.output_sandbox]

        # Build post-process commands — output storage
        if output_paths:
            self._postprocess_commands.append(
                StoreOutputDataCommand(output_paths=output_paths, output_se=output_se)
            )

    async def run_job(self, job: JobModel) -> bool:
        """Execute a given CWL workflow using cwltool.

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
            command = await self.pre_process(job.task, job.input)
            logger.info("Task pre-processed successfully!")

            # Execute the task
            logger.info("Executing Task: %s", command)
            self._job_report.set_job_status(minor_status=JobMinorStatus.APPLICATION)
            await self._job_report.commit()
            result = subprocess.run(  # noqa: S603
                command, capture_output=True, text=True, cwd=self._job_path
            )

            if result.returncode != 0:
                logger.error(
                    "Error in executing workflow:\n%s", Text.from_ansi(result.stderr)
                )
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
