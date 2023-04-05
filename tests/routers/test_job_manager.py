from fastapi.testclient import TestClient

from chrishackaton import app
from chrishackaton.routers.auth import create_access_token
from chrishackaton.properties import SecurityProperty
from pytest import fixture


from uuid import uuid4


lhcb_iam_endpoint = "https://lhcb-auth.web.cern.ch/"
# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "21e98a30bb41420dc601dea1dc1f85ecee3b4d702547bea355c07ab44fd7f3c3"
ALGORITHM = "HS256"
ISSUER = "http://lhcbdirac.cern.ch/"
AUDIENCE = "dirac"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


@fixture
def normal_user_client():
    with TestClient(app) as client:
        payload = {
            "sub": "testingVO:yellow-sub",
            "aud": AUDIENCE,
            "iss": ISSUER,
            "dirac_properties": [SecurityProperty.NORMAL_USER],
            "jti": str(uuid4()),
            "preferred_username": "preferred_username",
            "dirac_group": "test_group",
        }
        token = create_access_token(payload)
        breakpoint()
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


def test_read_main():
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200


def test_insert_and_list_jobs(normal_user_client):
    job_definitions = [
        {"owner": "owner1", "group": "group1", "vo": "vo1", "jdl": "jdl1"},
        {"owner": "owner2", "group": "group2", "vo": "vo2", "jdl": "jdl2"},
    ]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    r = normal_user_client.get("/jobs")
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    print(r.json())
