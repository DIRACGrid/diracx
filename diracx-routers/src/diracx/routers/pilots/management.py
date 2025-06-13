from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, Query, status

from diracx.core.exceptions import (
    PilotAlreadyExistsError,
    PilotNotFoundError,
    SecretNotFoundError,
)
from diracx.core.models import (
    PilotCredentialsInfo,
    PilotFieldsMapping,
    PilotSecretConstraints,
    PilotSecretsInfo,
    PilotStatus,
)
from diracx.core.properties import GENERIC_PILOT, JOB_ADMINISTRATOR
from diracx.logic.pilots.auth import create_secrets
from diracx.logic.pilots.auth import (
    update_secrets_constraints as update_secrets_constraints_bl,
)
from diracx.logic.pilots.management import (
    delete_pilots as delete_pilots_bl,
)
from diracx.logic.pilots.management import (
    get_pilot_jobs_ids_by_stamp,
    register_new_pilots,
    update_pilots_fields,
)
from diracx.logic.pilots.query import get_pilot_ids_by_job_id
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token

from ..dependencies import AuthSettings, JobDB, PilotAgentsDB
from ..fastapi_classes import DiracxRouter
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()


@router.post("/")
async def add_pilot_stamps(
    pilot_db: PilotAgentsDB,
    pilot_stamps: Annotated[
        list[str],
        Body(description="List of the pilot stamps we want to add to the db."),
    ],
    vo: Annotated[str, Body(description="Pilot virtual organization.")],
    settings: AuthSettings,
    check_permissions: CheckPilotManagementPolicyCallable,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    grid_site: Annotated[str, Body(description="Pilots grid site.")] = "Unknown",
    destination_site: Annotated[
        str, Body(description="Pilots destination site.")
    ] = "NotAssigned",
    pilot_references: Annotated[
        dict[str, str] | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
    pilot_status: Annotated[
        PilotStatus, Body(description="Status of the pilots.")
    ] = PilotStatus.SUBMITTED,
    generate_secrets: Annotated[
        bool,
        Body(description="If we want to create secrets with the pilots."),
    ] = True,
    pilot_secret_use_count_max: Annotated[
        int | None,
        Body(description="How much time can a secret be used."),
    ] = 1,
) -> list[PilotCredentialsInfo] | None:
    """Endpoint where a you can create pilots with their references.

    If a pilot stamp already exists, it will block the insertion.
    """
    # TODO: Verify that grid types, sites, destination sites, etc. are valids
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        allow_legacy_pilots=True,  # dirac-admin-add-pilot
    )

    # Prevent someone who stole a pilot X509 to create thousands of pilots at a time
    # (It would be still able to create thousands of pilots, but slower)
    if GENERIC_PILOT in user_info.properties:
        if len(pilot_stamps) != 1:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="As a pilot, you can only create yourself.",
            )

    if JOB_ADMINISTRATOR not in user_info.properties:
        if not vo == user_info.vo:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can create pilots only for your VO.",
            )

    try:
        return await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            vo=user_info.vo,
            grid_type=grid_type,
            grid_site=grid_site,
            destination_site=destination_site,
            pilot_job_references=pilot_references,
            status=pilot_status,
            settings=settings,
            generate_secrets=generate_secrets,
            pilot_secret_use_count_max=pilot_secret_use_count_max,
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e


@router.delete("/", status_code=HTTPStatus.NO_CONTENT)
async def delete_pilots(
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    pilot_stamps: Annotated[
        list[str] | None, Query(description="Stamps of the pilots we want to delete.")
    ] = None,
    age_in_days: Annotated[
        int | None,
        Query(
            description=(
                "The number of days that define the maximum age of pilots to be deleted."
                "Pilots older than this age will be considered for deletion."
            )
        ),
    ] = None,
    delete_only_aborted: Annotated[
        bool,
        Query(
            description=(
                "Flag indicating whether to only delete pilots whose status is 'Aborted'."
                "If set to True, only pilots with the 'Aborted' status will be deleted."
                "It is set by default as True to avoid any mistake."
                "This flag is only used for deletion by time."
            )
        ),
    ] = False,
):
    """Endpoint to delete a pilot.

    Two features:

    1. Or you provide pilot_stamps, so you can delete pilots by their stamp
    2. Or you provide age_in_days, so you can delete pilots that lived more than age_in_days days.

    Note: If you delete a pilot, its logs and its associations with jobs WILL be deleted.
    """
    vo_constraint: str | None = None

    # If we delete by pilot_stamps, we check that we can access them
    # Else, we add a constraint to the request, to avoid deleting pilots from another VO
    if pilot_stamps:
        await check_permissions(
            action=ActionType.MANAGE_PILOTS,
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
        )
    else:
        vo_constraint = user_info.vo

    if not pilot_stamps and not age_in_days:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="pilot_stamps or age_in_days have to be provided.",
        )

    await delete_pilots_bl(
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
        age_in_days=age_in_days,
        delete_only_aborted=delete_only_aborted,
        vo_constraint=vo_constraint,
    )


@router.post("/secrets")
async def create_pilot_secrets(
    n: Annotated[int, Body(description="Number of secrets to create.")],
    expiration_minutes: Annotated[
        int | None, Body(description="Time in minutes before expiring.")
    ],
    pilot_secret_use_count_max: Annotated[
        int | None, Body(description="Number of times that we can use a secret.")
    ],
    vo: Annotated[str, Body(description="Only VO that can access a secret.")],
    check_permissions: CheckPilotManagementPolicyCallable,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
) -> list[PilotSecretsInfo]:
    """Endpoint to create secrets."""
    await check_permissions(action=ActionType.MANAGE_PILOTS)

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

    return await create_secrets(
        n=n,
        pilot_db=pilot_db,
        settings=settings,
        secret_constraint=PilotSecretConstraints(VOs=[vo]),
        pilot_secret_use_count_max=pilot_secret_use_count_max,
        expiration_minutes=expiration_minutes,
    )


@router.patch("/secrets", status_code=HTTPStatus.NO_CONTENT)
async def update_secrets_constraints(
    secrets_to_constraints_dict: Annotated[
        dict[str, PilotSecretConstraints],
        Body(description="Mapping between secrets and pilots.", embed=False),
    ],
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint to associate pilots with secrets."""
    pilot_stamps = set()
    for constraints in secrets_to_constraints_dict.values():
        if "PilotStamps" in constraints:
            pilot_stamps.update(constraints["PilotStamps"])

    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
    )

    try:
        await update_secrets_constraints_bl(
            pilot_db=pilot_db,
            secrets_to_constraints_dict=secrets_to_constraints_dict,
        )
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


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def update_pilot_fields(
    pilot_stamps_to_fields_mapping: Annotated[
        list[PilotFieldsMapping],
        Body(
            description="(pilot_stamp, pilot_fields) mapping to change.",
            embed=True,
            openapi_examples=EXAMPLE_UPDATE_FIELDS,  # type: ignore
        ),
    ],
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):
    """Modify a field of a pilot.

    Note: Only the fields in PilotFieldsMapping are mutable, except for the PilotStamp.
    """
    # Ensures stamps validity
    pilot_stamps = [mapping.PilotStamp for mapping in pilot_stamps_to_fields_mapping]
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
        allow_legacy_pilots=True,  # dirac-admin-add-pilot
    )

    # Prevent someone who stole a pilot X509 to modify thousands of pilots at a time
    # (It would be still able to modify thousands of pilots, but slower)
    # We are not able to affirm that this pilot modifies itself
    if GENERIC_PILOT in user_info.properties:
        if len(pilot_stamps) != 1:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="As a pilot, you can only modify yourself.",
            )

    await update_pilots_fields(
        pilot_db=pilot_db,
        pilot_stamps_to_fields_mapping=pilot_stamps_to_fields_mapping,
    )


@router.get("/jobs")
async def get_pilot_jobs(
    pilot_db: PilotAgentsDB,
    job_db: JobDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    pilot_stamp: Annotated[
        str | None, Query(description="The stamp of the pilot.")
    ] = None,
    job_id: Annotated[int | None, Query(description="The ID of the job.")] = None,
) -> list[int]:
    """Endpoint only for admins, to get jobs of a pilot."""
    if pilot_stamp:
        # Check VO
        await check_permissions(
            action=ActionType.READ_PILOT_FIELDS,
            pilot_db=pilot_db,
            pilot_stamps=[pilot_stamp],
        )

        return await get_pilot_jobs_ids_by_stamp(
            pilot_db=pilot_db,
            pilot_stamp=pilot_stamp,
        )
    elif job_id:
        # Check job owner
        await check_permissions(
            action=ActionType.READ_PILOT_FIELDS, job_db=job_db, job_ids=[job_id]
        )

        return await get_pilot_ids_by_job_id(pilot_db=pilot_db, job_id=job_id)

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="You must provide either pilot_stamp or job_id",
    )
