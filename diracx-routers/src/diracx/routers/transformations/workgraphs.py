"""Provide the endpoint submitting a workgraph (DX-ADR-007).

The workgraph is a CWL v1.2 ``Workflow`` carrying ``dirac:`` hints, sent as
JSON, either plain or packed (``cwltool --pack``). It is compiled into the
Transformation System tables in one transaction, owned by the caller's VO
and subject.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, HTTPException

from diracx.core.exceptions import InvalidWorkgraphError
from diracx.core.models.transformation import WorkgraphSubmissionResult
from diracx.db.sql import TransformationDB
from diracx.logic.transformations import submit_workgraph as submit_workgraph_bl

from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckTransformationPolicyCallable

router = DiracxRouter()

_GAUSS = {
    "class": "CommandLineTool",
    "id": "#gauss",
    "baseCommand": ["lb-run", "Gauss"],
    "inputs": [{"id": "#gauss/seed", "type": ["File", "string"]}],
    "outputs": [
        {
            "id": "#gauss/sim-files",
            "type": "File[]",
            "outputBinding": {"glob": "*.sim"},
        }
    ],
}

EXAMPLE_WORKGRAPHS = {
    "Single-step simulation": {
        "value": {
            "cwlVersion": "v1.2",
            "$namespaces": {"dirac": "https://diracgrid.org/cwl#"},
            "$graph": [
                {
                    "class": "Workflow",
                    "id": "#main",
                    "label": "MC simulation",
                    "hints": [
                        {
                            "class": "dirac:Workgraph",
                            "schema_version": "1.0",
                            "type": "MCSimulation",
                        }
                    ],
                    "inputs": [
                        {
                            "id": "#main/events",
                            "type": {"type": "array", "items": ["File", "string"]},
                            "dirac:Feeder": {
                                "name": "Seeds",
                                "args": {"target_events": 600000},
                            },
                        }
                    ],
                    "steps": [
                        {
                            "id": "#main/MCSimulation",
                            "hints": [
                                {
                                    "class": "dirac:Transformation",
                                    "packer": {"name": "PerInput"},
                                }
                            ],
                            "run": "#gauss",
                            "in": [
                                {
                                    "id": "#main/MCSimulation/seed",
                                    "source": "#main/events",
                                }
                            ],
                            "out": ["#main/MCSimulation/sim-files"],
                        }
                    ],
                    "outputs": [
                        {
                            "id": "#main/datasets",
                            "type": "File[]",
                            "outputSource": "#main/MCSimulation/sim-files",
                        }
                    ],
                },
                _GAUSS,
            ],
        }
    },
}


@router.post("/workgraphs", status_code=HTTPStatus.CREATED)
async def submit_workgraph(
    document: Annotated[dict[str, Any], Body(openapi_examples=EXAMPLE_WORKGRAPHS)],
    transformation_db: TransformationDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckTransformationPolicyCallable,
) -> WorkgraphSubmissionResult:
    """Submit a workgraph and create its transformations.

    Every step of the workflow must carry ``dirac:Transformation`` and becomes
    a compute transformation; each ``data_management`` declaration of
    ``dirac:Workgraph`` becomes a data transformation. Everything is created
    in the ``New`` state. A step's ``run`` must be an inline process or a
    ``#fragment`` of a packed document.

    \f

    Args:
        document: The CWL workgraph document, as JSON.
        transformation_db: Database the workgraph is stored in.
        user_info: Authenticated identity of the submitter.
        check_permissions: Access-policy callback for workgraph submission.

    Returns:
        The identifiers of the workgraph and of its transformations.

    Raises:
        HTTPException: With status 422 listing every problem found if the
            document is not a valid workgraph.
    """
    await check_permissions(action=ActionType.SUBMIT_WORKGRAPH)
    try:
        return await submit_workgraph_bl(
            document,
            transformation_db=transformation_db,
            vo=user_info.vo,
            sub=user_info.sub,
            author=user_info.preferred_username,
        )
    except InvalidWorkgraphError as exc:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.errors
        ) from exc
