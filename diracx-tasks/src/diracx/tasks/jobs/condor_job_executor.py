"""Tasks that hand submitted jobs to an HTCondor scheduler."""

from __future__ import annotations

import dataclasses
import logging
import tempfile
from datetime import UTC, datetime

import htcondor2
from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import extractJDL
from pydantic import PositiveInt

from diracx.core.models import (
    JobLoggingRecord,
    JobStatus,
    JobStatusUpdate,
    ScalarSearchOperator,
    ScalarSearchSpec,
)
from diracx.core.settings import ServiceSettingsBase
from diracx.db.os import JobParametersDB
from diracx.db.sql import JobDB, JobLoggingDB, TaskQueueDB

# from diracx.logic.jobs import set_job_statuses
from diracx.tasks.plumbing.base_task import BaseTask, PeriodicBaseTask
from diracx.tasks.plumbing.depends import Config
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import JOB
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import IntervalSeconds

logger = logging.getLogger(__name__)

MINOR_STATUS = "CondorExecutor"

DEFAULT_DESIRED_SITES = (
    "T1_DE_KIT",
    "T1_ES_PIC",
    "T1_FR_CCIN2P3",
    "T1_IT_CNAF",
    "T1_RU_JINR",
    "T1_UK_RAL",
    "T1_US_FNAL",
    "T2_CH_CERN",
    "T2_CH_CERN_P2",
    "T2_CH_CSCS",
    "T2_DE_DESY",
    "T2_DE_RWTH",
    "T2_ES_CIEMAT",
    "T2_ES_IFCA",
    "T2_FR_GRIF",
    "T2_FR_IPHC",
    "T2_IT_Bari",
    "T2_IT_Legnaro",
    "T2_IT_Rome",
    "T2_UK_London_Brunel",
    "T2_UK_London_IC",
    "T2_UK_SGrid_Bristol",
    "T2_UK_SGrid_RALPP",
    "T2_US_Caltech",
    "T2_US_MIT",
    "T2_US_Nebraska",
    "T2_US_Purdue",
    "T2_US_UCSD",
    "T2_US_Vanderbilt",
    "T2_US_Wisconsin",
)


class CondorJobExecutorSettings(ServiceSettingsBase):
    """Settings controlling automatic Condor job submission."""

    model_config = ServiceSettingsBase.model_config | {
        "env_prefix": "DIRACX_TASKS_CONDOR_JOB_EXECUTOR_",
        "use_attribute_docstrings": True,
    }

    enabled: bool = False
    """Whether the monitor is scheduled automatically."""

    interval_seconds: PositiveInt = 10
    """How often the enabled monitor searches for received jobs."""

    schedd_name: str = "crab3@vocms059.cern.ch"
    """Logical name of the target HTCondor schedd."""

    collector_host: str | None = "vocms4100.cern.ch"
    """Optional collector host used to resolve the target schedd."""


_settings = CondorJobExecutorSettings()


def _jdl_to_key_value_pairs(jdl: str) -> dict[str, str]:
    """Return a debugger-friendly dictionary extracted from a JDL.

    The DIRAC JDL comes as a classad-like string such as
    ``[ Executable = "/bin/echo"; Arguments = "hello"; ]``.  Converting it to a
    dictionary makes it easier to inspect in a debugger while still allowing the
    same data to be turned back into HTCondor submit text.
    """
    stripped = jdl.strip()
    if not stripped:
        raise ValueError("JDL is empty")

    if stripped.startswith("[") and stripped.endswith("]"):
        stripped = stripped[1:-1].strip()

    pairs: dict[str, str] = {}
    for chunk in stripped.split(";"):
        piece = chunk.strip()
        if not piece or piece.lower().startswith("queue"):
            continue
        if "=" not in piece:
            continue
        key, value = piece.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith("(") and value.endswith(")"):
            value = value[1:-1].strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        pairs[key] = value

    if not pairs:
        raise ValueError("JDL did not contain any key/value entries")

    return pairs


def _clean_jdl_value(value: str) -> str:
    """Normalize DIRAC JDL representations into plain Python strings."""
    stripped = str(value).strip()
    if not stripped:
        return ""

    while (
        len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {'"', "'"}
    ):
        stripped = stripped[1:-1].strip()

    if stripped.startswith("[") and stripped.endswith("]"):
        stripped = stripped[1:-1].strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        stripped = stripped[1:-1].strip()
    if stripped.startswith("(") and stripped.endswith(")"):
        stripped = stripped[1:-1].strip()

    if "](" in stripped and stripped.endswith(")"):
        stripped = stripped.split("](", 1)[0].strip("[]")

    return stripped


def _jdl_dict_to_submit_description(jdl: dict[str, str]) -> str:
    """Render a DIRAC JDL dictionary into a realistic HTCondor submit stanza."""
    values = {key: _clean_jdl_value(value) for key, value in jdl.items()}

    executable = values.get("Executable") or "/bin/sh"
    arguments = values.get("Arguments") or ""
    job_name = values.get("JobName") or "dirac_job"
    owner = values.get("Owner") or "localuser"
    owner_group = "analysis"
    cpu_time = int(values.get("CPUTime") or 86400)
    request_cpus = values.get("RequestCpus") or values.get("CPUs") or "1"
    request_memory = values.get("RequestMemory") or values.get("Memory") or "2000"
    std_output = values.get("StdOutput") or f"out/{job_name}.out"
    std_error = values.get("StdError") or f"err/{job_name}.err"
    log_path = values.get("Log") or f"log/{job_name}.log"
    wall_time_mins = max(1, int(round(cpu_time / 60))) if cpu_time else 60
    desired_sites = values.get("DesiredSites") or ",".join(DEFAULT_DESIRED_SITES)
    required_os = values.get("REQUIRED_OS") or "rhel9"
    required_arch = values.get("REQUIRED_ARCH") or "X86_64"

    lines = [
        "Universe = vanilla",
        "",
        f"Executable = {executable}",
        f"Arguments = {arguments}",
        "",
        f"Log = {log_path}",
        f"Output = {std_output}",
        f"Error = {std_error}",
        "",
        "should_transfer_files = YES",
        "when_to_transfer_output = ON_EXIT",
        "",
        f'+DESIRED_Sites = "{desired_sites}"',
        f'REQUIRED_OS = "{required_os}"',
        f'REQUIRED_ARCH = "{required_arch}"',
        f'Requirements = (TARGET.Arch == "{required_arch}") && (TARGET.OpSys == "LINUX")',
        "",
        f"request_cpus = {request_cpus}",
        f"request_memory = {request_memory}",
        "",
        f"+MaxWallTimeMins = {wall_time_mins}",
        f'accounting_group = "{owner_group}.{owner}"',
        "",
        "Queue 1",
    ]
    return "\n".join(lines)


async def _set_job_statuses_sql_only(
    *,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    status_changes: dict[int, JobStatusUpdate],
    source: str,
) -> None:
    """Apply status updates only to SQL tables, skipping OpenSearch writes."""
    now = datetime.now(UTC)
    await job_db.set_job_attributes(
        {
            job_id: {
                key: value
                for key, value in {
                    "Status": status_update.status,
                    "MinorStatus": status_update.minor_status,
                    "ApplicationStatus": status_update.application_status,
                }.items()
                if value is not None
            }
            for job_id, status_update in status_changes.items()
        }
    )
    await job_logging_db.insert_records(
        [
            JobLoggingRecord(
                job_id=job_id,
                status=status_update.status or "idem",
                minor_status=status_update.minor_status or "idem",
                application_status=status_update.application_status or "idem",
                date=now,
                source=source,
            )
            for job_id, status_update in status_changes.items()
        ]
    )


@dataclasses.dataclass(frozen=True)
class CondorSubmitResult:
    cluster_id: int
    proc_id: int = 0
    schedd_name: str | None = None


@dataclasses.dataclass
class CondorJobExecutorMonitorTask(PeriodicBaseTask):
    """Move newly received jobs to Waiting and queue Condor submission tasks."""

    priority = Priority.BACKGROUND
    size = Size.MEDIUM
    _enabled = _settings.enabled
    default_schedule = IntervalSeconds(_settings.interval_seconds)

    async def execute(
        self,
        config: Config,
        job_db: JobDB,
        job_logging_db: JobLoggingDB,
        task_queue_db: TaskQueueDB,
        job_parameters_db: JobParametersDB,
    ) -> int:
        _, jobs = await job_db.search(
            ["JobID"],
            [
                ScalarSearchSpec(
                    parameter="Status",
                    operator=ScalarSearchOperator.EQUAL,
                    value=JobStatus.RECEIVED,
                )
            ],
            [],
        )
        if not jobs:
            return 0

        job_ids = [job["JobID"] for job in jobs]
        logger.info("Moving %d received job(s) to Waiting: %s", len(job_ids), job_ids)
        logger.warning(
            "Applying SQL-only Waiting status transition (OpenSearch disabled)"
        )
        await _set_job_statuses_sql_only(
            job_db=job_db,
            job_logging_db=job_logging_db,
            status_changes={
                job_id: JobStatusUpdate(
                    Status=JobStatus.WAITING,
                    MinorStatus=MINOR_STATUS,
                    Source=MINOR_STATUS,
                )
                for job_id in job_ids
            },
            source=MINOR_STATUS,
        )
        # await set_job_statuses(
        #     {
        #         job_id: {
        #             datetime.now(UTC): JobStatusUpdate(
        #                 Status=JobStatus.WAITING,
        #                 MinorStatus=MINOR_STATUS,
        #                 Source=MINOR_STATUS,
        #             )
        #         }
        #         for job_id in job_ids
        #     },
        #     config=config,
        #     job_db=job_db,
        #     job_logging_db=job_logging_db,
        #     task_queue_db=task_queue_db,
        #     job_parameters_db=job_parameters_db,
        # )

        for job_id in job_ids:
            await CondorJobExecutorTask(job_id=job_id).schedule()

        return len(job_ids)


@dataclasses.dataclass
class CondorJobExecutorTask(BaseTask):
    """Submit a single job to the configured HTCondor schedd."""

    priority = Priority.NORMAL
    size = Size.MEDIUM
    retry_policy = ExponentialBackoff(base_delay_seconds=10, max_retries=3)

    job_id: int

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(JOB, self.job_id)]

    async def submit_to_condor(
        self,
        *,
        config: Config,
        job_db: JobDB,
    ) -> CondorSubmitResult:
        del config
        schedd_name = _settings.schedd_name
        collector_host = _settings.collector_host

        _, jobs = await job_db.search(
            parameters=["JobID"],
            search=[
                {
                    "parameter": "JobID",
                    "operator": ScalarSearchOperator.EQUAL,
                    "value": self.job_id,
                }
            ],
            sorts=[],
        )
        if not jobs:
            raise ValueError(f"Job not found: {self.job_id}")

        jdls = await job_db.get_job_jdls([self.job_id])
        jdl = jdls.get(self.job_id)
        if not jdl:
            raise ValueError(f"No JDL found for job {self.job_id}")

        extracted_jdl = extractJDL(jdl)
        if not extracted_jdl:
            raise ValueError(f"Could not decode JDL for job {self.job_id}")

        extracted_jdl = _jdl_to_key_value_pairs(extracted_jdl)
        submit_description = _jdl_dict_to_submit_description(extracted_jdl)

        htcondor2.set_subsystem("TOOL")
        htcondor2.param["TOOL_DEBUG"] = "D_FULLDEBUG"
        htcondor2.param["TOOL_LOG"] = f"{tempfile.gettempdir()}/htcondor-python.log"
        htcondor2.enable_log()

        collector = htcondor2.Collector(collector_host)
        schedd_ad = collector.locate(htcondor2.DaemonTypes.Schedd, schedd_name)
        if not schedd_ad:
            raise RuntimeError(
                f"Could not locate schedd '{schedd_name}' via collector '{collector_host}'"
            )

        schedd = htcondor2.Schedd(schedd_ad)
        submit = htcondor2.Submit(submit_description)
        result = schedd.submit(submit, spool=True)
        schedd.spool(result)
        cluster_id = result.cluster()

        return CondorSubmitResult(
            cluster_id=int(cluster_id),
            proc_id=0,
            schedd_name=schedd_name,
        )

    async def execute(
        self,
        config: Config,
        job_db: JobDB,
        job_logging_db: JobLoggingDB,
        task_queue_db: TaskQueueDB,
        job_parameters_db: JobParametersDB,
    ) -> int:
        logger.info("Submitting job %d to HTCondor", self.job_id)
        submission = await self.submit_to_condor(config=config, job_db=job_db)
        target = submission.schedd_name or _settings.schedd_name
        application_status = (
            f"Submitted to HTCondor schedd {target} as "
            f"{submission.cluster_id}.{submission.proc_id}"
        )
        logger.warning(
            "Applying SQL-only Matched status transition for job %d (OpenSearch disabled)",
            self.job_id,
        )
        await _set_job_statuses_sql_only(
            job_db=job_db,
            job_logging_db=job_logging_db,
            status_changes={
                self.job_id: JobStatusUpdate(
                    Status=JobStatus.MATCHED,
                    MinorStatus=MINOR_STATUS,
                    ApplicationStatus=application_status,
                    Source=MINOR_STATUS,
                )
            },
            source=MINOR_STATUS,
        )
        # now = datetime.now(UTC)
        # await set_job_statuses(
        #     {
        #         self.job_id: {
        #             now: JobStatusUpdate(
        #                 Status=JobStatus.MATCHED,
        #                 MinorStatus=MINOR_STATUS,
        #                 ApplicationStatus=application_status,
        #                 Source=MINOR_STATUS,
        #             )
        #         }
        #     },
        #     config=config,
        #     job_db=job_db,
        #     job_logging_db=job_logging_db,
        #     task_queue_db=task_queue_db,
        #     job_parameters_db=job_parameters_db,
        # )
        return self.job_id
