from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import HTTPException, status

from diracx.core.properties import GENERIC_PILOT, JOB_ADMINISTRATOR, NORMAL_USER
from diracx.routers.jobs.access_policies import (
    ActionType,
    SandboxAccessPolicy,
    WMSAccessPolicy,
)
from diracx.routers.utils.users import AuthorizedUserInfo

base_payload = {
    "sub": "testingVO:yellow-sub",
    "preferred_username": "preferred_username",
    "dirac_group": "test_group",
    "vo": "lhcb",
    "token_id": str(uuid4()),
    "bearer_token": "my_secret_token",
}


class FakeJobDB:
    async def summary(self, *args): ...


class FakePilotDB:
    async def get_pilot_by_reference(self, *args): ...
    async def get_pilot_job_ids(self, *args): ...


class FakeSBMetadataDB:
    async def get_owner_id(self, *args): ...
    async def get_sandbox_owner_id(self, *args): ...


@pytest.fixture
def job_db():
    yield FakeJobDB()


@pytest.fixture
def pilot_db():
    yield FakePilotDB()


@pytest.fixture
def sandbox_metadata_db():
    yield FakeSBMetadataDB()


WMS_POLICY_NAME = "WMSAccessPolicy_AlthoughItDoesNotMatter"
SANDBOX_POLICY_NAME = "SandboxAccessPolicy_AlthoughItDoesNotMatter"


async def test_wms_access_policy_weird_user(job_db):
    """USer without NORMAL_USER or JOB_ADMINISTRATION can't do anything."""
    weird_user = AuthorizedUserInfo(properties=[], **base_payload)
    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME, weird_user, action=ActionType.CREATE, job_db=job_db
        )

    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME, weird_user, action=ActionType.QUERY, job_db=job_db
        )

    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            weird_user,
            action=ActionType.READ,
            job_db=job_db,
            job_ids=[1, 2, 3],
        )


async def test_wms_access_policy_pilot(job_db, pilot_db, monkeypatch):

    normal_user = AuthorizedUserInfo(properties=[NORMAL_USER], **base_payload)
    pilot = AuthorizedUserInfo(properties=[GENERIC_PILOT], **base_payload)

    # ------------------------- Simple User accessing a pilot action -------------------------
    # A user cannot create any resource
    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}") as excinfo:
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            normal_user,
            action=ActionType.PILOT,
            job_db=job_db,
            pilot_db=pilot_db,
            job_ids=[1, 2],
        )

    # Split to distinguish the generated part ("403 ") from the message part ("you are not a pilot")
    assert str(excinfo.value) == "403: " + "you are not a pilot", excinfo

    # ------------------------- Lost pilot -------------------------
    async def get_pilot_by_reference_patch(*args):
        return []

    monkeypatch.setattr(
        pilot_db, "get_pilot_by_reference", get_pilot_by_reference_patch
    )

    # A pilot that has expired (removed from db) should not be able to access jobs
    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}") as excinfo:
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            pilot,
            action=ActionType.PILOT,
            pilot_db=pilot_db,
            job_db=job_db,
            job_ids=[1, 2],
        )

    assert str(excinfo.value) == "403: " + "this pilot is not registered", excinfo

    # ------------------------- Pilot accessing wrong jobs -------------------------
    async def get_pilot_by_reference_patch(*args, **kwargs):
        return {"PilotID": 1}

    async def get_pilot_job_ids_patch(*args, **kwargs):
        return []

    monkeypatch.setattr(
        pilot_db, "get_pilot_by_reference", get_pilot_by_reference_patch
    )
    monkeypatch.setattr(pilot_db, "get_pilot_job_ids", get_pilot_job_ids_patch)

    # A pilot that has is not associated with a job can't access a job
    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}") as excinfo:
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            pilot,
            action=ActionType.PILOT,
            pilot_db=pilot_db,
            job_db=job_db,
            job_ids=[1, 2],
        )

    assert (
        str(excinfo.value)
        == "403: " + "this pilot can't access/modify some jobs: ids={1, 2}"
    ), excinfo

    # ------------------------- Pilot accessing some of his jobs -------------------------
    async def get_pilot_job_ids_patch(*args, **kwargs):
        return [1, 2, 3, 4]

    monkeypatch.setattr(pilot_db, "get_pilot_job_ids", get_pilot_job_ids_patch)

    # A pilot that is associated with a job can access a job
    await WMSAccessPolicy.policy(
        WMS_POLICY_NAME,
        pilot,
        action=ActionType.PILOT,
        pilot_db=pilot_db,
        job_db=job_db,
        job_ids=[1, 2],
    )

    # ------------------------- Pilot accessing some of his jobs plus some forbidden -------------------------
    async def get_pilot_job_ids_patch(*args, **kwargs):
        return [1, 2, 3, 4]

    monkeypatch.setattr(pilot_db, "get_pilot_job_ids", get_pilot_job_ids_patch)

    # A pilot that fetches few jobs, one where he does not have the rights, and few where he has the rights
    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}") as excinfo:
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            pilot,
            action=ActionType.PILOT,
            pilot_db=pilot_db,
            job_db=job_db,
            job_ids=[1, 2, 12],
        )

    assert (
        str(excinfo.value)
        == "403: " + "this pilot can't access/modify some jobs: ids={12}"
    ), excinfo


async def test_wms_access_policy_create(job_db):

    admin_user = AuthorizedUserInfo(properties=[JOB_ADMINISTRATOR], **base_payload)
    normal_user = AuthorizedUserInfo(properties=[NORMAL_USER], **base_payload)

    # You can't create and give job_ids at the same time
    with pytest.raises(NotImplementedError):
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            normal_user,
            action=ActionType.CREATE,
            job_db=job_db,
            job_ids=[1, 2, 3],
        )
    with pytest.raises(NotImplementedError):
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            admin_user,
            action=ActionType.CREATE,
            job_db=job_db,
            job_ids=[1, 2, 3],
        )

    # An admin cannot create any resource
    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME, admin_user, action=ActionType.CREATE, job_db=job_db
        )

    # A normal user should be able to create jobs
    await WMSAccessPolicy.policy(
        WMS_POLICY_NAME, normal_user, action=ActionType.CREATE, job_db=job_db
    )

    ##############


async def test_wms_access_policy_query(job_db):
    admin_user = AuthorizedUserInfo(properties=[JOB_ADMINISTRATOR], **base_payload)
    normal_user = AuthorizedUserInfo(properties=[NORMAL_USER], **base_payload)

    # You can't create and give job_ids at the same time
    with pytest.raises(NotImplementedError):
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            normal_user,
            action=ActionType.QUERY,
            job_db=job_db,
            job_ids=[1, 2, 3],
        )

    # this does not trigger because the admin can do anything
    await WMSAccessPolicy.policy(
        WMS_POLICY_NAME,
        admin_user,
        action=ActionType.QUERY,
        job_db=job_db,
        job_ids=[1, 2, 3],
    )

    await WMSAccessPolicy.policy(
        WMS_POLICY_NAME, normal_user, action=ActionType.QUERY, job_db=job_db
    )


async def test_wms_access_policy_read_modify(job_db, monkeypatch):
    admin_user = AuthorizedUserInfo(properties=[JOB_ADMINISTRATOR], **base_payload)
    normal_user = AuthorizedUserInfo(properties=[NORMAL_USER], **base_payload)

    for tested_policy in (ActionType.READ, ActionType.MANAGE):
        # The admin can do anything
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            admin_user,
            action=tested_policy,
            job_db=job_db,
            job_ids=[1, 2, 3],
        )

        # We must give job ids
        with pytest.raises(NotImplementedError):
            await WMSAccessPolicy.policy(
                WMS_POLICY_NAME,
                normal_user,
                action=tested_policy,
                job_db=job_db,
            )

        # Standard case, querying for one own jobs
        async def summary_matching(*args):
            return [{"Owner": "preferred_username", "VO": "lhcb", "count": 3}]

        monkeypatch.setattr(job_db, "summary", summary_matching)

        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            normal_user,
            action=tested_policy,
            job_db=job_db,
            job_ids=[1, 2, 3],
        )

        # The admin can do anything
        await WMSAccessPolicy.policy(
            WMS_POLICY_NAME,
            admin_user,
            action=tested_policy,
            job_db=job_db,
            job_ids=[1, 2, 3],
        )

        # Jobs belong to somebody else
        async def summary_other_owner(*args):
            return [{"Owner": "other_owner", "VO": "lhcb", "count": 3}]

        monkeypatch.setattr(job_db, "summary", summary_other_owner)
        with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
            await WMSAccessPolicy.policy(
                WMS_POLICY_NAME,
                normal_user,
                action=tested_policy,
                job_db=job_db,
                job_ids=[1, 2, 3],
            )

        # Jobs belong to somebody else
        async def summary_other_vo(*args):
            return [{"Owner": "preferred_username", "VO": "gridpp", "count": 3}]

        monkeypatch.setattr(job_db, "summary", summary_other_vo)
        with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
            await WMSAccessPolicy.policy(
                WMS_POLICY_NAME,
                normal_user,
                action=tested_policy,
                job_db=job_db,
                job_ids=[1, 2, 3],
            )

        # Wrong job count
        async def summary_other_vo(*args):
            return [{"Owner": "preferred_username", "VO": "lhcb", "count": 2}]

        monkeypatch.setattr(job_db, "summary", summary_other_vo)
        with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
            await WMSAccessPolicy.policy(
                WMS_POLICY_NAME,
                normal_user,
                action=tested_policy,
                job_db=job_db,
                job_ids=[1, 2, 3],
            )


SE_NAME = "ProductionSEName"
SANDBOX_PREFIX = "/S3/bucket_name/myvo/mygroup/mypreferred_username"
USER_SANDBOX_PFN = f"{SANDBOX_PREFIX}/mysandbox.tar.gz"
OTHER_USER_SANDBOX_PFN = (
    "/S3/bucket_name/myothervo/myothergroup/myotherusername/mysandbox.tar.gz"
)


async def test_sandbox_access_policy_create(sandbox_metadata_db):

    admin_user = AuthorizedUserInfo(properties=[JOB_ADMINISTRATOR], **base_payload)
    normal_user = AuthorizedUserInfo(properties=[NORMAL_USER], **base_payload)

    # action is a mandatory parameter
    with pytest.raises(AssertionError):
        await SandboxAccessPolicy.policy(
            SANDBOX_POLICY_NAME,
            normal_user,
            sandbox_metadata_db=sandbox_metadata_db,
        )

    # An admin cannot create any resource
    with pytest.raises(HTTPException, match=f"{status.HTTP_403_FORBIDDEN}"):
        await SandboxAccessPolicy.policy(
            SANDBOX_POLICY_NAME,
            admin_user,
            action=ActionType.CREATE,
            sandbox_metadata_db=sandbox_metadata_db,
            pfns=[USER_SANDBOX_PFN],
        )

    # A normal user should be able to create sanbox
    await SandboxAccessPolicy.policy(
        SANDBOX_POLICY_NAME,
        normal_user,
        action=ActionType.CREATE,
        sandbox_metadata_db=sandbox_metadata_db,
        pfns=[USER_SANDBOX_PFN],
    )

    ##############


async def test_sandbox_access_policy_read(sandbox_metadata_db, monkeypatch):

    admin_user = AuthorizedUserInfo(properties=[JOB_ADMINISTRATOR], **base_payload)
    normal_user = AuthorizedUserInfo(properties=[NORMAL_USER], **base_payload)

    await SandboxAccessPolicy.policy(
        SANDBOX_POLICY_NAME,
        admin_user,
        action=ActionType.READ,
        sandbox_metadata_db=sandbox_metadata_db,
        pfns=[USER_SANDBOX_PFN],
        required_prefix=SANDBOX_PREFIX,
        se_name=SE_NAME,
    )

    await SandboxAccessPolicy.policy(
        SANDBOX_POLICY_NAME,
        admin_user,
        action=ActionType.READ,
        sandbox_metadata_db=sandbox_metadata_db,
        pfns=[OTHER_USER_SANDBOX_PFN],
        required_prefix=SANDBOX_PREFIX,
        se_name=SE_NAME,
    )

    # need required_prefix for READ
    with pytest.raises(NotImplementedError):
        await SandboxAccessPolicy.policy(
            SANDBOX_POLICY_NAME,
            normal_user,
            action=ActionType.READ,
            sandbox_metadata_db=sandbox_metadata_db,
            pfns=[USER_SANDBOX_PFN],
        )

    # need se_name for READ
    with pytest.raises(NotImplementedError):
        await SandboxAccessPolicy.policy(
            SANDBOX_POLICY_NAME,
            normal_user,
            action=ActionType.READ,
            sandbox_metadata_db=sandbox_metadata_db,
            pfns=[USER_SANDBOX_PFN],
            required_prefix=SANDBOX_PREFIX,
        )

    # User can act on his own sandbox
    async def get_owner_id(*args):
        return 1

    async def get_sandbox_owner_id(*args):
        return 1

    monkeypatch.setattr(sandbox_metadata_db, "get_owner_id", get_owner_id)
    monkeypatch.setattr(
        sandbox_metadata_db, "get_sandbox_owner_id", get_sandbox_owner_id
    )

    await SandboxAccessPolicy.policy(
        SANDBOX_POLICY_NAME,
        normal_user,
        action=ActionType.READ,
        sandbox_metadata_db=sandbox_metadata_db,
        pfns=[USER_SANDBOX_PFN],
        required_prefix=SANDBOX_PREFIX,
        se_name=SE_NAME,
    )

    # User cannot act on others
    async def get_owner_id(*args):
        return 2

    monkeypatch.setattr(sandbox_metadata_db, "get_owner_id", get_owner_id)

    with pytest.raises(HTTPException):
        await SandboxAccessPolicy.policy(
            SANDBOX_POLICY_NAME,
            normal_user,
            action=ActionType.READ,
            sandbox_metadata_db=sandbox_metadata_db,
            pfns=[OTHER_USER_SANDBOX_PFN],
            required_prefix=SANDBOX_PREFIX,
            se_name=SE_NAME,
        )

    with pytest.raises(HTTPException):
        await SandboxAccessPolicy.policy(
            SANDBOX_POLICY_NAME,
            normal_user,
            action=ActionType.READ,
            sandbox_metadata_db=sandbox_metadata_db,
            pfns=[USER_SANDBOX_PFN],
            required_prefix=SANDBOX_PREFIX,
            se_name="OTHER_SE_NAME",
        )
