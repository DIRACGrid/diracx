"""Custom executor for DIRAC CWL workflows with replica map management."""

from __future__ import annotations

import functools
import logging
from collections.abc import MutableMapping
from pathlib import Path
from typing import cast

from cwltool.context import RuntimeContext
from cwltool.errors import WorkflowException
from cwltool.executors import SingleJobExecutor
from cwltool.job import CommandLineJob
from cwltool.process import Process
from cwltool.stdfsaccess import StdFsAccess
from cwltool.utils import CWLOutputType
from cwltool.workflow_job import WorkflowJob

from diracx.core.models.replica_map import ReplicaMap

from .fs_access import DiracReplicaMapFsAccess

logger = logging.getLogger("dirac-cwl-run")


class DiracExecutor(SingleJobExecutor):
    """Custom executor that handles replica map management between steps.

    This executor overrides run_jobs() to intercept each CommandLineJob execution
    and manage replica maps before and after the job runs.
    """

    def __init__(self, global_map_path: Path | None = None):
        """Initialize executor with optional global replica map path."""
        super().__init__()
        self.global_map_path = global_map_path
        self.global_map: ReplicaMap | None = None

    def run_jobs(
        self,
        process: Process,
        job_order_object: MutableMapping[str, CWLOutputType | None],
        logger_arg: logging.Logger,
        runtime_context: RuntimeContext,
    ) -> None:
        """Override run_jobs to intercept each job execution.

        This method is called once at the top level and iterates through ALL jobs
        including nested CommandLineTools within subworkflows. The generator pattern
        flattens the workflow hierarchy so we see every CommandLineJob here.
        """
        # Load replica map once at the start
        if self.global_map is None:
            if self.global_map_path and self.global_map_path.exists():
                self.global_map = ReplicaMap.model_validate_json(
                    self.global_map_path.read_text()
                )
                logger.info(
                    "Loaded replica map with %d file(s)", len(self.global_map.root)
                )
            else:
                self.global_map = ReplicaMap(root={})
                logger.debug("Initialized empty replica map")

        # Set up custom filesystem access that can resolve LFNs via replica map
        # we create a partial function that binds the replica map to our custom
        # fs access class
        runtime_context.make_fs_access = cast(
            type[StdFsAccess],
            functools.partial(DiracReplicaMapFsAccess, replica_map=self.global_map),
        )

        # Store the replica map on the runtime context so DiracCommandLineTool
        # can pass it to DiracPathMapper in make_path_mapper()
        runtime_context.replica_map = self.global_map  # type: ignore[attr-defined]

        # Get job iterator - this yields ALL jobs including nested ones
        jobiter = process.job(job_order_object, self.output_callback, runtime_context)

        try:
            for job in jobiter:
                if job is not None:
                    # Standard setup from SingleJobExecutor.run_jobs
                    if runtime_context.builder is not None and hasattr(job, "builder"):
                        job.builder = runtime_context.builder
                    if job.outdir is not None:
                        self.output_dirs.add(job.outdir)

                    # Validation mode (from SingleJobExecutor.run_jobs)
                    if runtime_context.validate_only is True:
                        if isinstance(job, WorkflowJob):
                            name = job.tool.lc.filename
                        else:
                            name = getattr(job, "name", str(job))
                        print(
                            f"{name} is valid CWL. No errors detected in the inputs.",
                            file=runtime_context.validate_stdout,
                        )
                        return

                    # CUSTOM: Intercept CommandLineJob to manage replica maps
                    if isinstance(job, CommandLineJob):
                        # job_name = getattr(job, "name", "unknown")
                        self._prepare_job_replica_map(job, runtime_context)

                    # Execute the job
                    job.run(runtime_context)

                    # CUSTOM: Update replica map after CommandLineJob completes
                    if isinstance(job, CommandLineJob):
                        self._update_replica_map_from_job(job, runtime_context)
                else:
                    logger.error("Workflow cannot make any more progress.")
                    break
        except WorkflowException:
            raise
        except Exception as err:
            logger.exception("Got workflow error")
            raise WorkflowException(str(err)) from err

    def _prepare_job_replica_map(
        self, job: CommandLineJob, runtime_context: RuntimeContext
    ):
        """Prepare replica map for a specific CommandLineJob.

        Args:
            job: The CommandLineJob about to be executed
            runtime_context: Runtime context containing execution settings

        """
        job_name = getattr(job, "name", "unknown")

        # Extract LFNs from job inputs
        # job.builder.job contains the actual input dictionary
        job_inputs = job.builder.job if hasattr(job, "builder") else {}
        step_lfns = self._extract_lfns_from_inputs(job_inputs)

        # Filter global map for this step's inputs
        if step_lfns and self.global_map:
            step_replica_map = ReplicaMap(
                root={
                    lfn: entry
                    for lfn, entry in self.global_map.root.items()
                    if lfn in step_lfns
                }
            )
            found = len(step_replica_map.root)
            if found > 0:
                logger.info("%s: Found %d input files in replica map", job_name, found)
            else:
                logger.warning(
                    "%s: Expected input files not found in replica map: %s",
                    job_name,
                    step_lfns,
                )
                step_replica_map = ReplicaMap(root={})
        elif step_lfns:
            logger.warning(
                "%s: Input files requested but no replica map available: %s",
                job_name,
                step_lfns,
            )
            step_replica_map = ReplicaMap(root={})
        else:
            # Create empty replica map for steps with no LFN inputs (e.g., simulation)
            step_replica_map = ReplicaMap(root={})

        # Write step replica map to job's output directory
        if job.outdir:
            step_replica_map_path = Path(job.outdir) / "replica_map.json"
            step_replica_map_path.write_text(step_replica_map.model_dump_json(indent=2))
        else:
            logger.warning(
                "%s: Job has no output directory, cannot write replica map", job_name
            )

    def _update_replica_map_from_job(
        self, job: CommandLineJob, runtime_context: RuntimeContext
    ):
        """Update replica map from job outputs.

        After a job completes, the application may have added new LFNs
        to the step replica map. Merge those back into the global map.

        Args:
            job: The completed CommandLineJob
            runtime_context: Runtime context containing execution settings

        """
        job_name = getattr(job, "name", "unknown")

        if not job.outdir:
            logger.warning(
                "%s: Job has no output directory, cannot update replica map", job_name
            )
            return

        step_replica_map_path = Path(job.outdir) / "replica_map.json"
        if not step_replica_map_path.exists():
            logger.debug("%s: No step replica map found, skipping update", job_name)
            return

        try:
            step_replica_map = ReplicaMap.model_validate_json(
                step_replica_map_path.read_text()
            )

            if self.global_map is None:
                self.global_map = ReplicaMap(root={})

            # Only register files that are NOT already in the global map
            # This filters out input files that were copied to the step replica map
            new_entries = []
            updated_entries = []
            for lfn, entry in step_replica_map.root.items():
                if lfn in self.global_map.root:
                    # File already exists - check if it was updated
                    existing_entry = self.global_map.root[lfn]
                    # If the entry has changed (e.g., new replicas added), update it
                    if existing_entry != entry:
                        self.global_map.root[lfn] = entry
                        updated_entries.append(lfn)
                        logger.debug(
                            "%s: Updated replica map entry for %s", job_name, lfn
                        )
                    # Otherwise skip - this is an input file that hasn't changed
                else:
                    # This is a new file (output from this job)
                    self.global_map.root[lfn] = entry
                    new_entries.append(lfn)

            if new_entries:
                logger.info(
                    "%s: Registered %d new output file(s) (replica map total: %d)",
                    job_name,
                    len(new_entries),
                    len(self.global_map.root),
                )
            if updated_entries:
                logger.info(
                    "%s: Updated %d existing replica map entries",
                    job_name,
                    len(updated_entries),
                )
        except Exception as e:
            logger.exception("%s: Failed to update replica map - %s", job_name, e)

    def _extract_lfns_from_inputs(
        self, job_order: MutableMapping[str, CWLOutputType | None]
    ) -> list[str]:
        """Extract LFN paths from job inputs.

        Recursively searches through the job order dictionary to find File objects
        with paths that look like LFNs (start with "LFN:").

        Args:
            job_order: Job input dictionary

        Returns:
            List of LFN paths found in the inputs (with LFN: prefix stripped)

        """
        lfns = []

        def extract_recursive(obj):
            if isinstance(obj, dict):
                # Check if this looks like a File with an LFN path
                if "class" in obj and obj["class"] == "File":
                    # Check both "path" and "location" fields
                    for field in ["path", "location"]:
                        value = obj.get(field, "")
                        if value.startswith("LFN:"):
                            # Strip LFN: prefix for replica map lookup
                            lfn = value[4:]
                            if lfn not in lfns:
                                lfns.append(lfn)
                            break
                else:
                    for value in obj.values():
                        extract_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_recursive(item)

        extract_recursive(job_order)
        return lfns


def dirac_executor_factory(global_map_path: Path | None = None):
    """Create a DiracExecutor with configuration.

    Args:
        global_map_path: Path to master replica map JSON file

    Returns:
        Executor function compatible with cwltool

    """

    def executor(process, job_order, runtime_context, logger_arg):
        dirac_exec = DiracExecutor(global_map_path)
        return dirac_exec(process, job_order, runtime_context, logger_arg)

    return executor
