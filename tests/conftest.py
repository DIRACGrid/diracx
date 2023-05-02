from uuid import uuid4

from fastapi.testclient import TestClient
from pytest import fixture

from chrishackaton import app
from chrishackaton.properties import SecurityProperty
from chrishackaton.routers.auth import create_access_token

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
            "vo": "lhcb",
        }
        token = create_access_token(payload)
        client.headers["Authorization"] = f"Bearer {token}"
        client.dirac_token_payload = payload
        yield client
