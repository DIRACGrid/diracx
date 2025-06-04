from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, status

from diracx.core.exceptions import (
    CredentialsAlreadyExistError,
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
    SecretNotFoundError,
)
from diracx.core.models import (
    PilotFieldsMapping,
    PilotSecretConstraints,
    PilotSecretsInfo,
)
from diracx.logic.pilots.auth import create_secrets
from diracx.logic.pilots.auth import (
    update_secrets_constraints as update_secrets_constraints_bl,
)
from diracx.logic.pilots.management import (
    associate_pilot_with_jobs as associate_pilot_with_jobs_bl,
)
from diracx.logic.pilots.management import update_pilots_fields

from ..dependencies import AuthSettings, PilotAgentsDB
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckDiracServicesPolicyCallable,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()

logger = logging.getLogger(__name__)


@router.post("/fields/secrets")
async def create_pilot_secrets(
    n: Annotated[int, Body(description="Number of secrets to create.")],
    vo: Annotated[
        str | None, Body(description="Virtual Organisation of the secrets to create.")
    ],
    expiration_minutes: Annotated[
        int | None, Body(description="Time in minutes before expiring.")
    ],
    pilot_secret_use_count_max: Annotated[
        int | None, Body(description="Number of times that we can use a secret.")
    ],
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotManagementPolicyCallable,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
) -> list[PilotSecretsInfo]:
    """Endpoint to create secrets."""
    await check_permissions(action=ActionType.CREATE_PILOT_OR_SECRET, vo=vo)

    if expiration_minutes and expiration_minutes <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="expiration_minutes must be strictly positive.",
        )
    if pilot_secret_use_count_max and pilot_secret_use_count_max <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="pilot_secret_use_count_max is either None or a positive number.",
        )

    credentials = await create_secrets(
        n, pilot_db, settings, pilot_secret_use_count_max, expiration_minutes
    )

    logger.info(
        f"{user_info.preferred_username} created {n} secrets that will expire in {expiration_minutes} minute(s)."
    )

    return credentials


@router.patch("/fields/secrets", status_code=HTTPStatus.NO_CONTENT)
async def update_secrets_constraints(
    secrets_to_constraints_dict: Annotated[
        dict[str, PilotSecretConstraints],
        Body(description="Mapping between secrets and pilots.", embed=False),
    ],
    pilot_agents_db: PilotAgentsDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint to associate pilots with secrets."""
    pilot_stamps = set()
    for constraints in secrets_to_constraints_dict.values():
        if "PilotStamps" in constraints:
            pilot_stamps.update(constraints["PilotStamps"])

    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_stamps=pilot_stamps,
        pilot_db=pilot_agents_db,
    )

    try:
        await update_secrets_constraints_bl(
            pilot_db=pilot_agents_db,
            secrets_to_constraints_dict=secrets_to_constraints_dict,
        )
    except CredentialsAlreadyExistError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="one of the pilot already is associated with one of the secrets",
            # TODO: Give more details
        ) from e
    except SecretNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="one of the secrets does not exist",
        ) from e
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="one of the pilots does not exist",
        ) from e

    logger.info(
        f"{user_info.preferred_username} associated {len(pilot_stamps)} pilots"
        f"with {len(secrets_to_constraints_dict)} secrets."
    )


EXAMPLE_UPDATE_FIELDS = {
    "Update the BenchMark field": {
        "summary": "Update BenchMark",
        "description": "Update only the BenchMark for one pilot.",
        "value": {
            "pilot_stamps_to_fields_mapping": [
                {"PilotStamp": "the_pilot_stamp", "BenchMark": 1.0}
            ]
        },
    },
    "Update multiple statuses": {
        "summary": "Update multiple pilots",
        "description": "Update multiple pilots statuses.",
        "value": {
            "pilot_stamps_to_fields_mapping": [
                {"PilotStamp": "the_first_pilot_stamp", "Status": "Waiting"},
                {"PilotStamp": "the_second_pilot_stamp", "Status": "Waiting"},
            ]
        },
    },
}


@router.patch("/fields", status_code=HTTPStatus.NO_CONTENT)
async def update_pilot_fields(
    pilot_stamps_to_fields_mapping: Annotated[
        list[PilotFieldsMapping],
        Body(
            description="(pilot_stamp, pilot_fields) mapping to change.",
            embed=True,
            openapi_examples=EXAMPLE_UPDATE_FIELDS,
        ),
    ],
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Modify a field of a pilot.

    Note: Only the fields in PilotFieldsMapping are mutable, except for the PilotStamp.
    """
    # TODO: Add an example for openapi
    pilot_stamps = [mapping.PilotStamp for mapping in pilot_stamps_to_fields_mapping]

    # Ensures stamps validity
    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_db=pilot_agents_db,
        pilot_stamps=pilot_stamps,
    )

    await update_pilots_fields(
        pilot_db=pilot_agents_db,
        pilot_stamps_to_fields_mapping=pilot_stamps_to_fields_mapping,
    )


@router.patch("/fields/jobs", status_code=HTTPStatus.NO_CONTENT)
async def associate_pilot_with_jobs(
    pilot_agents_db: PilotAgentsDB,
    pilot_stamp: Annotated[str, Body(description="The stamp of the pilot.")],
    pilot_jobs_ids: Annotated[
        list[int], Body(description="The jobs we want to add to the pilot.")
    ],
    check_permissions: CheckDiracServicesPolicyCallable,
):
    """Endpoint only for DIRAC services, to associate a pilot with a job."""
    await check_permissions()

    try:
        await associate_pilot_with_jobs_bl(
            pilot_db=pilot_agents_db,
            pilot_stamp=pilot_stamp,
            pilot_jobs_ids=pilot_jobs_ids,
        )
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="This pilot does not exist."
        ) from e
    except PilotAlreadyAssociatedWithJobError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This pilot is already associated with this job.",
        ) from e
