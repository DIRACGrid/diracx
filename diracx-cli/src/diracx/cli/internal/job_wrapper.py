#!/usr/bin/env python
"""Job wrapper template for executing CWL jobs."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
from typing import Any

from cwl_utils.parser import load_document_by_uri
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from ruamel.yaml import YAML

from diracx.api.job_wrapper import JobWrapper
from diracx.core.models.cwl_submission import JobModel


async def main():
    """Execute the job wrapper for a given job model.

    Fetches the CWL workflow definition and input parameters from the
    diracX API using the WorkflowID stored in the job config JSON.
    """
    if len(sys.argv) != 3:
        logging.error("2 arguments required, <json-file> <jobID>")
        sys.exit(1)

    job_id = int(sys.argv[2])

    # Fetch workflow_id, CWL, and params from diracX API using the job_id
    from diracx.client.aio import AsyncDiracClient
    from diracx.core.models.search import ScalarSearchOperator, ScalarSearchSpec

    async with AsyncDiracClient() as client:
        # Get workflow_id and params from job attributes
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

        job_attrs = results[0]
        workflow_id = job_attrs.get("WorkflowID")
        workflow_params = job_attrs.get("WorkflowParams")

        if not workflow_id:
            logging.error("Job %d has no WorkflowID", job_id)
            sys.exit(1)

        # Fetch CWL definition
        workflow_response = await client.jobs.get_workflow(workflow_id)
        cwl_yaml = workflow_response["cwl"]

    # Parse CWL
    yaml_doc = YAML()
    task_dict = yaml_doc.load(cwl_yaml)

    with tempfile.NamedTemporaryFile("w+", suffix=".cwl", delete=False) as f:
        YAML().dump(task_dict, f)
        f.flush()
        cwl_path = f.name

    try:
        task_obj = load_document_by_uri(cwl_path)
    finally:
        os.unlink(cwl_path)

    # Build job model
    job_model_dict: dict[str, Any] = {"task": task_obj, "input": None}

    # If workflow_params were stored, use them as CWL inputs
    if workflow_params:
        cwl_inputs_obj = load_inputfile(workflow_params)
        job_model_dict["input"] = {"sandbox": None, "cwl": cwl_inputs_obj}

    job = JobModel.model_validate(job_model_dict)
    job_wrapper = JobWrapper(job_id)

    res = await job_wrapper.run_job(job)
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
