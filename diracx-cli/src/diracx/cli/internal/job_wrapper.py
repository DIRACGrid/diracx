#!/usr/bin/env python
"""Job wrapper template for executing CWL jobs."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from ruamel.yaml import YAML

from diracx.api.job_wrapper import JobWrapper


async def main():
    """Fetch a job's workflow + params from the diracX API and run it."""
    if len(sys.argv) < 2:
        logging.error("Usage: job_wrapper.py <jobID>")
        sys.exit(1)

    job_id = int(sys.argv[-1])

    from diracx.client.aio import AsyncDiracClient
    from diracx.core.models.search import ScalarSearchOperator, ScalarSearchSpec

    async with AsyncDiracClient() as client:
        results = await client.jobs.search(
            parameters=["JobID", "WorkflowID", "WorkflowParams"],
            search=[
                ScalarSearchSpec(
                    parameter="JobID",
                    operator=ScalarSearchOperator.EQUAL,
                    value=str(job_id),
                )
            ],
        )
        if not results:
            logging.error("Job %d not found", job_id)
            sys.exit(1)

        workflow_id = results[0].get("WorkflowID")
        workflow_params = results[0].get("WorkflowParams")

        if not workflow_id:
            logging.error("Job %d has no WorkflowID", job_id)
            sys.exit(1)

        cwl_yaml = (await client.jobs.get_workflow(workflow_id))["cwl"]

    # Write the workflow to disk verbatim — no parse, no round-trip.
    workflow_path = Path.cwd() / f"{workflow_id[:8]}.cwl"
    workflow_path.write_text(cwl_yaml)

    # Write the input parameters file if any. workflow_params is already
    # a dict pulled from the Job DB, so just dump it.
    params_path = None
    if workflow_params:
        params_path = Path.cwd() / "parameters.yaml"
        with open(params_path, "w") as f:
            YAML().dump(workflow_params, f)

    job_wrapper = JobWrapper(job_id)
    res = await job_wrapper.run_job(workflow_path, params_path)
    if res:
        logging.info("Job done.")
        return 0
    else:
        logging.info("Job failed.")
        return 1


def setup_diracx() -> None:
    """Configure DiracX credentials for AsyncDiracClient on the worker.

    Reuses ``DIRAC.Core.Security.DiracX.writeDiracxTokenCache`` so the cache
    file lands in the same hash-named tmpdir location DiracXClient uses.
    Exports ``DIRACX_URL`` and ``DIRACX_CREDENTIALS_PATH`` so a no-arg
    ``AsyncDiracClient()`` picks the credentials up via env-var fallthrough.
    """
    import DIRAC  # type: ignore[import-untyped]

    DIRAC.initialize()

    from DIRAC.Core.Security.DiracX import (
        writeDiracxTokenCache,  # type: ignore[import-untyped]
    )

    diracx_url, token_file = writeDiracxTokenCache()
    os.environ["DIRACX_URL"] = diracx_url
    os.environ["DIRACX_CREDENTIALS_PATH"] = str(token_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    setup_diracx()
    sys.exit(asyncio.run(main()))
