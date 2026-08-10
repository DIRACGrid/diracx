"""Unit tests for `PilotManagementAccessPolicy`.

These tests bypass the FastAPI test harness (which stubs the real policy
with `AlwaysAllowAccessPolicy`) and invoke the policy coroutine
directly, mirroring how it is called from a real request.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import HTTPException

from diracx.core.properties import GENERIC_PILOT, NORMAL_USER, SERVICE_ADMINISTRATOR
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.routers.pilots.access_policies import (
    ActionType,
    PilotManagementAccessPolicy,
)
from diracx.routers.utils.users import AuthorizedUserInfo

MAIN_VO = "lhcb"


def _user(*properties, vo: str = MAIN_VO) -> AuthorizedUserInfo:
    """Build a minimal AuthorizedUserInfo for policy tests."""
    return AuthorizedUserInfo(
        bearer_token="",
        token_id=str(uuid4()),
        properties=list(properties),
        sub="testingVO:sub",
        preferred_username="test-user",
        dirac_group="test_group",
        vo=vo,
        policies={},
    )


@pytest.fixture
async def pilot_db_with_pilots():
    """Yield a pilot DB seeded with two pilots, both in MAIN_VO."""
    db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        async with db as pdb:
            await pdb.register_pilots(pilot_stamps=["stamp-a", "stamp-b"], vo=MAIN_VO)
        yield db


async def test_manage_requires_service_administrator():
    """A normal user cannot manage pilots."""
    with pytest.raises(HTTPException) as exc_info:
        await PilotManagementAccessPolicy.policy(
            "PilotManagementAccessPolicy",
            _user(NORMAL_USER),
            action=ActionType.MANAGE_PILOTS,
        )
    assert exc_info.value.status_code == 403


async def test_manage_allows_service_administrator():
    # Must not raise
    await PilotManagementAccessPolicy.policy(
        "PilotManagementAccessPolicy",
        _user(SERVICE_ADMINISTRATOR),
        action=ActionType.MANAGE_PILOTS,
    )


async def test_manage_allows_legacy_pilot_when_opted_in():
    """`allow_legacy_pilots=True` lets GENERIC_PILOT identities manage."""
    # Must not raise
    await PilotManagementAccessPolicy.policy(
        "PilotManagementAccessPolicy",
        _user(GENERIC_PILOT),
        action=ActionType.MANAGE_PILOTS,
        allow_legacy_pilots=True,
    )


async def test_manage_rejects_legacy_pilot_when_not_opted_in():
    with pytest.raises(HTTPException) as exc_info:
        await PilotManagementAccessPolicy.policy(
            "PilotManagementAccessPolicy",
            _user(GENERIC_PILOT),
            action=ActionType.MANAGE_PILOTS,
        )
    assert exc_info.value.status_code == 403


async def test_manage_legacy_pilot_capped_to_single_stamp():
    """A legacy pilot identity may only act on one pilot stamp per call."""
    with pytest.raises(HTTPException) as exc_info:
        await PilotManagementAccessPolicy.policy(
            "PilotManagementAccessPolicy",
            _user(GENERIC_PILOT),
            action=ActionType.MANAGE_PILOTS,
            pilot_stamps=["stamp-a", "stamp-b"],
            allow_legacy_pilots=True,
        )
    assert exc_info.value.status_code == 403


async def test_manage_admin_not_capped_to_single_stamp(pilot_db_with_pilots):
    """Service administrators may act on several pilots in one call."""
    async with pilot_db_with_pilots as db:
        # Must not raise
        await PilotManagementAccessPolicy.policy(
            "PilotManagementAccessPolicy",
            _user(SERVICE_ADMINISTRATOR),
            action=ActionType.MANAGE_PILOTS,
            pilot_db=db,
            pilot_stamps=["stamp-a", "stamp-b"],
        )


async def test_manage_target_vo_own_vo_allowed():
    """A legacy pilot may register a pilot into its own VO."""
    # Must not raise
    await PilotManagementAccessPolicy.policy(
        "PilotManagementAccessPolicy",
        _user(GENERIC_PILOT),
        action=ActionType.MANAGE_PILOTS,
        target_vo=MAIN_VO,
        allow_legacy_pilots=True,
    )


async def test_manage_target_vo_cross_vo_rejected_for_legacy_pilot():
    with pytest.raises(HTTPException) as exc_info:
        await PilotManagementAccessPolicy.policy(
            "PilotManagementAccessPolicy",
            _user(GENERIC_PILOT),
            action=ActionType.MANAGE_PILOTS,
            target_vo="other-vo",
            allow_legacy_pilots=True,
        )
    assert exc_info.value.status_code == 403


async def test_manage_target_vo_cross_vo_allowed_for_admin():
    # Must not raise
    await PilotManagementAccessPolicy.policy(
        "PilotManagementAccessPolicy",
        _user(SERVICE_ADMINISTRATOR),
        action=ActionType.MANAGE_PILOTS,
        target_vo="other-vo",
    )


async def test_read_denies_generic_pilots():
    """A pilot identity is not allowed to read other pilots' metadata."""
    with pytest.raises(HTTPException) as exc_info:
        await PilotManagementAccessPolicy.policy(
            "PilotManagementAccessPolicy",
            _user(GENERIC_PILOT),
            action=ActionType.READ_PILOT_METADATA,
        )
    assert exc_info.value.status_code == 403


async def test_pilot_stamp_check_raises_404_on_unknown(pilot_db_with_pilots):
    """Supplying an unknown pilot stamp must surface as 404."""
    async with pilot_db_with_pilots as db:
        with pytest.raises(HTTPException) as exc_info:
            await PilotManagementAccessPolicy.policy(
                "PilotManagementAccessPolicy",
                _user(SERVICE_ADMINISTRATOR),
                action=ActionType.MANAGE_PILOTS,
                pilot_db=db,
                pilot_stamps=["stamp-a", "nope"],
            )
        assert exc_info.value.status_code == 404


async def test_pilot_stamp_check_raises_403_on_cross_vo(pilot_db_with_pilots):
    """A non-admin from another VO must not be able to act on this VO's pilots."""
    async with pilot_db_with_pilots as db:
        with pytest.raises(HTTPException) as exc_info:
            await PilotManagementAccessPolicy.policy(
                "PilotManagementAccessPolicy",
                _user(GENERIC_PILOT, vo="other-vo"),
                action=ActionType.MANAGE_PILOTS,
                pilot_db=db,
                pilot_stamps=["stamp-a"],
                allow_legacy_pilots=True,
            )
        assert exc_info.value.status_code == 403


async def test_pilot_stamp_check_cross_vo_allowed_for_admin(pilot_db_with_pilots):
    """Service administrators manage pilots across VOs, like they read them."""
    async with pilot_db_with_pilots as db:
        # Must not raise
        await PilotManagementAccessPolicy.policy(
            "PilotManagementAccessPolicy",
            _user(SERVICE_ADMINISTRATOR, vo="other-vo"),
            action=ActionType.MANAGE_PILOTS,
            pilot_db=db,
            pilot_stamps=["stamp-a"],
        )
