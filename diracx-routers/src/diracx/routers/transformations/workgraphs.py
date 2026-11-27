"""Provide the endpoints submitting and starting a workgraph (DX-ADR-007).

A workgraph is a CWL v1.2 ``Workflow`` carrying ``dirac:`` hints, sent as the
JSON document ``cwltool --pack`` makes. For now it must be made of a single
step, which becomes a compute transformation (DIRACGrid/diracx#1081).
Submission stores it ``New``; starting it moves it to ``Active``, which is
when its feeder runs.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any
from uuid import UUID

from fastapi import Body, Depends, HTTPException, Response

from diracx.core.exceptions import (
    InvalidWorkgraphError,
    InvalidWorkgraphTransitionError,
    WorkgraphNotFoundError,
)
from diracx.core.models.transformation import WorkgraphSubmissionResult
from diracx.db.sql import TransformationDB
from diracx.logic.transformations import start_workgraph as start_workgraph_bl
from diracx.logic.transformations import submit_workgraph as submit_workgraph_bl

from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckTransformationPolicyCallable

router = DiracxRouter()

# A workgraph as cwltool --pack writes it: dirac: names as full IRIs, and the
# step running another $graph entry
EXAMPLE_WORKGRAPHS = {
    "Simulation": {
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
                            "class": "https://diracgrid.org/cwl#Workgraph",
                            "schema_version": "1.0",
                            "type": "MCSimulation",
                        }
                    ],
                    "inputs": [
                        {
                            "id": "#main/events",
                            "type": {"type": "array", "items": ["File", "string"]},
                            "https://diracgrid.org/cwl#Feeder": {
                                "name": "Seeds",
                                "args": {
                                    "target_events": 600000,
                                    "events_per_seed": 1000,
                                },
                            },
                        }
                    ],
                    "steps": [
                        {
                            "id": "#main/MCSimulation",
                            "hints": [
                                {
                                    "class": "https://diracgrid.org/cwl#Transformation",
                                    "packer": {
                                        "name": "FixedSize",
                                        "args": {"group_size": 1},
                                    },
                                }
                            ],
                            "run": "#gauss.cwl",
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
                {
                    "class": "CommandLineTool",
                    "id": "#gauss.cwl",
                    "baseCommand": ["lb-run", "Gauss"],
                    "inputs": [{"id": "#gauss.cwl/seed", "type": ["File", "string"]}],
                    "outputs": [
                        {
                            "id": "#gauss.cwl/sim-files",
                            "type": "File[]",
                            "outputBinding": {"glob": "*.sim"},
                        }
                    ],
                },
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
    """Submit a workgraph made of a single transformation.

    The document is the one ``cwltool --pack`` makes, self-contained: a
    ``$graph`` whose ``#main`` workflow has exactly one step carrying
    ``dirac:Transformation``, its ``run`` naming another entry or holding an
    inline body. The workgraph and its transformation are created ``New``;
    start the workgraph to run it.

    \f

    Args:
        document: The CWL workgraph document, as JSON.
        transformation_db: Database the workgraph is stored in.
        user_info: Authenticated identity of the submitter.
        check_permissions: Access-policy callback for workgraph operations.

    Returns:
        The identifiers of the workgraph and of its transformation.

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
        )
    except InvalidWorkgraphError as exc:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.errors
        ) from exc


@router.post("/workgraphs/{workgraph_id}/start", status_code=HTTPStatus.NO_CONTENT)
async def start_workgraph(
    workgraph_id: UUID,
    transformation_db: TransformationDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckTransformationPolicyCallable,
) -> Response:
    """Start a ``New`` workgraph: it and its transformation become ``Active``.

    \f

    Args:
        workgraph_id: The workgraph to start.
        transformation_db: Database the workgraph is stored in.
        user_info: Authenticated identity of the caller, recorded as the
            author of the state changes.
        check_permissions: Access-policy callback for workgraph operations.

    Returns:
        An empty response.

    Raises:
        HTTPException: With status 404 if the workgraph does not exist, or
            409 if it is not ``New``.
    """
    await check_permissions(
        action=ActionType.START_WORKGRAPH,
        transformation_db=transformation_db,
        workgraph_id=workgraph_id,
    )
    try:
        await start_workgraph_bl(
            workgraph_id,
            transformation_db=transformation_db,
            author=user_info.preferred_username,
        )
    except WorkgraphNotFoundError as exc:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(exc)) from exc
    except InvalidWorkgraphTransitionError as exc:
        raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(exc)) from exc
    return Response(status_code=HTTPStatus.NO_CONTENT)
