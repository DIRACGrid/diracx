from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, status

from diracx.core.exceptions import (
    CredentialsAlreadyExistError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
    SecretNotFoundError,
)
from diracx.core.models import (
    PilotCredentialsInfo,
    PilotFieldsMapping,
    PilotSecretsInfo,
    PilotStampInfo,
)
from diracx.logic.auth.pilot import (
    add_pilot_credentials,
    create_credentials,
    create_pilot_credentials_response,
    create_secrets_response,
    create_stamp_response,
    register_new_pilots,
)
from diracx.logic.auth.pilot import (
    associate_pilots_with_secrets as associate_pilots_with_secrets_bl,
)
from diracx.logic.pilots.management import delete_pilots_bulk, update_pilots_fields

from ..dependencies import (
    AuthSettings,
    PilotAgentsDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckPilotManagementPolicyCallable

router = DiracxRouter()

logger = logging.getLogger(__name__)


@router.post("/")
async def register_new_pilots_to_db(
    pilot_db: PilotAgentsDB,
    pilot_stamps: Annotated[
        list[str],
        Body(description="List of the pilot stamps we want to add to the db."),
    ],
    vo: Annotated[
        str,
        Body(description="Virtual Organisation associated with the inserted pilots."),
    ],
    settings: AuthSettings,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotManagementPolicyCallable,
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    pilot_references: Annotated[
        dict | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
    generate_secrets: Annotated[
        bool, Body(description="Boolean to allow secret creation or not.")
    ] = True,
    pilot_secret_use_count_max: Annotated[
        int, Body(description="Number of times we can use a secret.")
    ] = 1,
) -> list[PilotStampInfo] | list[PilotCredentialsInfo]:
    """Endpoint where a you can create pilots with their references.
    It will return the pilot secrets as well as an expiration date.

    If a pilot stamp already exists, it will block the insertion.
    """
    await check_permissions(action=ActionType.CREATE_PILOT_OR_SECRET, vo=vo)

    try:
        await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            vo=vo,
            grid_type=grid_type,
            pilot_job_references=pilot_references,
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e

    if generate_secrets:
        credentials, expiration_dates = await add_pilot_credentials(
            pilot_stamps=pilot_stamps,
            pilot_db=pilot_db,
            settings=settings,
            vo=vo,
            pilot_secret_use_count_max=pilot_secret_use_count_max,
        )
    # Logs credentials creation
    logger.debug(f"{user_info.preferred_username} added {len(pilot_stamps)} pilots.")

    if generate_secrets:
        return create_pilot_credentials_response(
            pilot_stamps=pilot_stamps,
            pilot_secrets=credentials,
            pilot_expiration_dates=expiration_dates,
        )
    else:
        return create_stamp_response(pilot_stamps=pilot_stamps)


@router.delete("/", status_code=HTTPStatus.NO_CONTENT)
async def patch_pilot_data(
    pilot_stamps: Annotated[
        list[str], Body(description="Stamps of the pilots we want to delete.")
    ],
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint to delete a pilot."""
    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_stamps=pilot_stamps,
        pilot_db=pilot_agents_db,
    )

    try:
        await delete_pilots_bulk(pilot_db=pilot_agents_db, pilot_stamps=pilot_stamps)
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one pilot has not been found.",
        ) from e


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
            status_code=status.HTTP_404_NOT_FOUND,
            detail="one of the secrets does not exist",
        ) from e
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
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
        Body(description="(pilot_stamp, pilot_fields) mapping to change."),
    ],
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):

    pilot_stamps = [mapping.PilotStamp for mapping in pilot_stamps_to_fields_mapping]

    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_db=pilot_agents_db,
        pilot_stamps=pilot_stamps,
    )

    await update_pilots_fields(
        pilot_db=pilot_agents_db,
        pilot_stamps_to_fields_mapping=pilot_stamps_to_fields_mapping,
    )
