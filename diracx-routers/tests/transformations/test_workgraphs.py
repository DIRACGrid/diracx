"""Router-level tests for workgraph submission."""

from __future__ import annotations

import copy
from http import HTTPStatus

import pytest
from fastapi import HTTPException
from uuid_utils import uuid7

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
EXAMPLE = EXAMPLE_WORKGRAPHS["Single-step simulation"]["value"]


@pytest.fixture
def normal_user_client(client_factory):
    # The test app grants every request; the policy is tested directly below
    with client_factory.normal_user() as client:
        yield client


def test_submit_workgraph(normal_user_client):
    r = normal_user_client.post(ENDPOINT, json=EXAMPLE)
    assert r.status_code == HTTPStatus.CREATED, r.text
    result = r.json()
    assert set(result) == {"workgraph_id", "transformations"}
    (transformation,) = result["transformations"]
    assert transformation["name"] == "MCSimulation"
    assert transformation["kind"] == "Compute"
    assert transformation["transformation_id"] != result["workgraph_id"]


def test_submit_workgraph_twice_gives_two_workgraphs(normal_user_client):
    first = normal_user_client.post(ENDPOINT, json=EXAMPLE)
    second = normal_user_client.post(ENDPOINT, json=EXAMPLE)
    assert first.status_code == second.status_code == HTTPStatus.CREATED
    assert first.json()["workgraph_id"] != second.json()["workgraph_id"]


def test_submit_invalid_workgraph(normal_user_client):
    document = copy.deepcopy(EXAMPLE)
    step = document["$graph"][0]["steps"][0]
    del step["hints"]
    step["run"] = "transformations/gauss.cwl"
    r = normal_user_client.post(ENDPOINT, json=document)
    assert r.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, r.text
    detail = r.json()["detail"]
    assert isinstance(detail, list)
    assert any("must carry dirac:Transformation" in e for e in detail)


def test_submit_requires_a_json_object(normal_user_client):
    r = normal_user_client.post(ENDPOINT, json=["not", "a", "document"])
    assert r.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_submit_requires_authentication(client_factory):
    with client_factory.unauthenticated() as client:
        r = client.post(ENDPOINT, json=EXAMPLE)
    assert r.status_code == HTTPStatus.UNAUTHORIZED


BASE_PAYLOAD = {
    "sub": "testingVO:yellow-sub",
    "preferred_username": "preferred_username",
    "dirac_group": "test_group",
    "vo": "lhcb",
    "token_id": str(uuid7()),
    "bearer_token": "my_secret_token",
}


async def test_access_policy():
    policy = TransformationAccessPolicy.policy
    manager = AuthorizedUserInfo(properties=[PRODUCTION_MANAGEMENT], **BASE_PAYLOAD)
    user = AuthorizedUserInfo(properties=[NORMAL_USER], **BASE_PAYLOAD)

    with pytest.raises(ValueError):
        await policy("p", manager)

    await policy("p", manager, action=ActionType.SUBMIT_WORKGRAPH)
    with pytest.raises(HTTPException, match="ProductionManagement"):
        await policy("p", user, action=ActionType.SUBMIT_WORKGRAPH)
