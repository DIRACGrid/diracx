"""Define the access policy of the Transformation System endpoints.

Workgraphs are productions run on behalf of a VO, so submitting and starting
them is restricted to production managers, and a workgraph can only be
started from the VO that owns it. The policy is provisional: which other
identities may submit, such as users with their own analysis productions,
is not decided yet.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException

from diracx.core.exceptions import WorkgraphNotFoundError
from diracx.core.properties import PRODUCTION_MANAGEMENT
from diracx.db.sql import TransformationDB
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    """Operations protected by the transformation access policy."""

    # Submit a workgraph and create its transformation
    SUBMIT_WORKGRAPH = auto()
    # Move a workgraph from New to Active
    START_WORKGRAPH = auto()


class TransformationAccessPolicy(BaseAccessPolicy):
    """Authorize operations on workgraphs."""

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
        transformation_db: TransformationDB | None = None,
        workgraph_id: UUID | None = None,
    ):
        """Check whether the caller may perform the requested operation.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated identity and security properties.
            action: Operation to authorize.
            transformation_db: Database used to look up the workgraph's owner.
            workgraph_id: Workgraph the operation acts on, if any.

        Raises:
            ValueError: If the action, or the workgraph an action needs, is
                not provided.
            HTTPException: With status 403 if the caller lacks permission, or
                404 if the workgraph does not exist.
        """
        if action is None:
            raise ValueError("action is a mandatory parameter")

        if PRODUCTION_MANAGEMENT not in user_info.properties:
            raise HTTPException(
                status_code=HTTPStatus.FORBIDDEN,
                detail="Workgraph operations require ProductionManagement.",
            )

        if action == ActionType.SUBMIT_WORKGRAPH:
            return

        if action == ActionType.START_WORKGRAPH:
            if transformation_db is None or workgraph_id is None:
                raise ValueError("transformation_db and workgraph_id are mandatory")
            try:
                workgraph = await transformation_db.get_workgraph(workgraph_id)
            except WorkgraphNotFoundError as exc:
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND, detail=str(exc)
                ) from exc
            if workgraph["vo"] != user_info.vo:
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail="The workgraph belongs to another VO.",
                )
            return

        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail=f"Unknown action {action}"
        )


CheckTransformationPolicyCallable = Annotated[
    Callable, Depends(TransformationAccessPolicy.check)
]
