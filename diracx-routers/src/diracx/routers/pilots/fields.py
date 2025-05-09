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
from diracx.core.models import PilotFieldsMapping, PilotSecretsInfo
from diracx.logic.auth.pilot import (
    associate_pilots_with_secrets as associate_pilots_with_secrets_bl,
)
from diracx.logic.auth.pilot import (
    create_credentials,
    create_secrets_response,
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

    # TODO: Check max for pilot_max
    # [SQL: INSERT INTO `PilotSecrets` (`HashedSecret`, `SecretGlobalUseCount`, `SecretGlobalUseCountMax`, `SecretVO`)
    # VALUES (%s, %s, %s, %s)]
    # [parameters: ('debc69a1f3ada36060b7fea2d1f5bf09339971b089cc2b8037bc3ecba527babf', 0, 1000000000, 'diracAdmin')]

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

    secrets, _, expiration_dates = await create_credentials(
        n, pilot_db, settings, vo, pilot_secret_use_count_max, expiration_minutes
    )

    logger.info(
        f"{user_info.preferred_username} created {n} secrets that will last in {expiration_minutes} minute(s)."
    )

    return create_secrets_response(
        pilot_secrets=secrets,
        pilot_expiration_dates=expiration_dates,
    )


@router.patch("/fields/secrets", status_code=HTTPStatus.NO_CONTENT)
async def associate_pilots_with_secrets(
    pilot_stamps: Annotated[list[str], Body(description="List of all pilot stamps.")],
    pilot_secrets: Annotated[
        list[str],
        Body(
            description=(
                "List of all secrets."
                "Possibility of providing only one (1) secret, it will apply it to all pilots."
            )
        ),
    ],
    pilot_agents_db: PilotAgentsDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint to associate pilots with secrets."""
    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_stamps=pilot_stamps,
        pilot_db=pilot_agents_db,
    )

    if len(pilot_secrets) != 1 and len(pilot_secrets) != len(pilot_stamps):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="pilot_secrets length must be one (1) or the same length as pilot_stamps",
        )

    try:
        await associate_pilots_with_secrets_bl(
            pilot_db=pilot_agents_db, secrets=pilot_secrets, pilot_stamps=pilot_stamps
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
        f"with {len(pilot_secrets)} secrets."
    )


@router.patch("/fields", status_code=HTTPStatus.NO_CONTENT)
async def update_pilot_fields(
    pilot_stamps_to_fields_mapping: Annotated[
        list[PilotFieldsMapping],
        Body(description="(pilot_stamp, pilot_fields) mapping to change.", embed=True),
    ],
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Modify a field of a pilot."""
    # TODO: Test this route
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
