#!/usr/bin/env python
"""Job wrapper template for executing CWL jobs."""

from __future__ import annotations

import asyncio
import json
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
    """Get a DiracX client instance with the current user's credentials."""
    from pathlib import Path

    import DIRAC  # type: ignore[import-untyped]

    DIRAC.initialize()

    from DIRAC import gConfig
    from DIRAC.Core.Security.DiracX import (
        diracxTokenFromPEM,  # type: ignore[import-untyped]
    )
    from DIRAC.Core.Security.Locations import (
        getDefaultProxyLocation,  # type: ignore[import-untyped]
    )

    diracx_url = gConfig.getValue("/DiracX/URL")
    if not diracx_url:
        raise ValueError("Missing mandatory /DiracX/URL configuration")

    os.environ["DIRACX_URL"] = diracx_url

    proxy_location = getDefaultProxyLocation()
    diracx_token = diracxTokenFromPEM(proxy_location)
    if not diracx_token:
        raise ValueError(f"No diracx token in the proxy file {proxy_location}")

    token_file = Path.home() / ".cache" / "diracx" / "credentials.json"
    token_file.parent.mkdir(parents=True, exist_ok=True)
    with open(
        token_file,
        "w",
        encoding="utf-8",
        opener=lambda p, f: os.open(p, f | os.O_TRUNC, 0o600),
    ) as f:
        json.dump(diracx_token, f)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    setup_diracx()
    sys.exit(asyncio.run(main()))
