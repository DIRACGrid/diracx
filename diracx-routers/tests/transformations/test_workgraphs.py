"""Router-level tests for workgraph submission and start."""

from __future__ import annotations

import copy
from http import HTTPStatus

import pytest
from fastapi import HTTPException
from uuid_utils import uuid7

from diracx.core.exceptions import WorkgraphNotFoundError
from diracx.core.properties import NORMAL_USER, PRODUCTION_MANAGEMENT
from diracx.routers.transformations.access_policies import (
    ActionType,
    TransformationAccessPolicy,
)
from diracx.routers.transformations.workgraphs import EXAMPLE_WORKGRAPHS
from diracx.routers.utils import AuthorizedUserInfo

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "DevelopmentSettings",
        "TransformationAccessPolicy",
        "TransformationDB",
    ]
)

ENDPOINT = "/api/transformations/workgraphs"
EXAMPLE = EXAMPLE_WORKGRAPHS["Simulation"]["value"]


@pytest.fixture
def normal_user_client(client_factory):
    # The test app grants every request; the policy is tested directly below
    with client_factory.normal_user() as client:
        yield client


def test_submit_then_start(normal_user_client):
    r = normal_user_client.post(ENDPOINT, json=EXAMPLE)
    assert r.status_code == HTTPStatus.CREATED, r.text
    result = r.json()
    assert set(result) == {"workgraph_id", "transformation_id"}

    r = normal_user_client.post(f"{ENDPOINT}/{result['workgraph_id']}/start")
    assert r.status_code == HTTPStatus.NO_CONTENT, r.text

    # Only a New workgraph can be started
    r = normal_user_client.post(f"{ENDPOINT}/{result['workgraph_id']}/start")
    assert r.status_code == HTTPStatus.CONFLICT, r.text


def test_start_unknown_workgraph(normal_user_client):
    r = normal_user_client.post(f"{ENDPOINT}/{uuid7()}/start")
    assert r.status_code == HTTPStatus.NOT_FOUND, r.text


def test_submit_invalid_workgraph(normal_user_client):
    document = copy.deepcopy(EXAMPLE)
    steps = document["$graph"][0]["steps"]
    second = copy.deepcopy(steps[0])
    second["id"] = "#main/Second"
    steps.append(second)
    r = normal_user_client.post(ENDPOINT, json=document)
    assert r.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, r.text
    assert r.json()["detail"] == [
        "a workgraph must have exactly one step for now, not 2"
    ]


def test_submit_requires_a_json_object(normal_user_client):
    r = normal_user_client.post(ENDPOINT, json=["not", "a", "document"])
    assert r.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_submit_requires_authentication(client_factory):
    with client_factory.unauthenticated() as client:
        r = client.post(ENDPOINT, json=EXAMPLE)
    assert r.status_code == HTTPStatus.UNAUTHORIZED


# --------------------------------------------------------------------------
# Access policy
# --------------------------------------------------------------------------

BASE_PAYLOAD = {
    "sub": "testingVO:yellow-sub",
    "preferred_username": "preferred_username",
    "dirac_group": "test_group",
    "vo": "lhcb",
    "token_id": str(uuid7()),
    "bearer_token": "my_secret_token",
}
MANAGER = AuthorizedUserInfo(properties=[PRODUCTION_MANAGEMENT], **BASE_PAYLOAD)
USER = AuthorizedUserInfo(properties=[NORMAL_USER], **BASE_PAYLOAD)


class FakeTransformationDB:
    def __init__(self, workgraphs):
        self.workgraphs = workgraphs

    async def get_workgraph(self, workgraph_id):
        if workgraph_id not in self.workgraphs:
            raise WorkgraphNotFoundError(workgraph_id)
        return {"vo": self.workgraphs[workgraph_id]}


async def test_policy_requires_an_action():
    with pytest.raises(ValueError):
        await TransformationAccessPolicy.policy("p", MANAGER)


@pytest.mark.parametrize("action", list(ActionType))
async def test_policy_requires_production_management(action):
    with pytest.raises(HTTPException) as exc_info:
        await TransformationAccessPolicy.policy("p", USER, action=action)
    assert exc_info.value.status_code == HTTPStatus.FORBIDDEN


async def test_policy_submit():
    await TransformationAccessPolicy.policy(
        "p", MANAGER, action=ActionType.SUBMIT_WORKGRAPH
    )


async def test_policy_start():
    own, other, missing = uuid7(), uuid7(), uuid7()
    db = FakeTransformationDB({own: "lhcb", other: "dteam"})

    async def start(workgraph_id):
        await TransformationAccessPolicy.policy(
            "p",
            MANAGER,
            action=ActionType.START_WORKGRAPH,
            transformation_db=db,
            workgraph_id=workgraph_id,
        )

    await start(own)
    with pytest.raises(HTTPException) as exc_info:
        await start(other)
    assert exc_info.value.status_code == HTTPStatus.FORBIDDEN
    with pytest.raises(HTTPException) as exc_info:
        await start(missing)
    assert exc_info.value.status_code == HTTPStatus.NOT_FOUND
    with pytest.raises(ValueError):
        await TransformationAccessPolicy.policy(
            "p", MANAGER, action=ActionType.START_WORKGRAPH
        )
