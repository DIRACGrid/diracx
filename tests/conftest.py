from uuid import uuid4

from fastapi.testclient import TestClient
from git import Repo
from pytest import fixture

from chrishackaton.config import Config, LocalGitConfigSource
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
def with_config_repo(tmp_path, monkeypatch):
    monkeypatch.setenv("DIRAC_CS_SOURCE", str(tmp_path))

    repo = Repo.init(tmp_path, initial_branch="master")
    cs_file = tmp_path / "default.yml"
    example_cs = Config.parse_obj(
        {
            "DIRAC": {"DefaultGroup": {"lhcb": "lhcb_user"}},
            "Registry": {
                "BannedIPs": "",
                "DefaultGroup": {
                    "lhcb": ["lhcb_lowpriouser", "lhcb_priouser", "lhcb_user"]
                },
                "DefaultProxyLifeTime": 432000,
                "DefaultStorageQuota": 2000,
                "DefaultVOMSAttribute": "/lhcb/Role=user",
                "Users": {},
                "Groups": {},
            },
            "Operations": {"Defaults": {}},
        }
    )
    cs_file.write_text(example_cs.json())
    repo.index.add([cs_file])  # add it to the index
    repo.index.commit("Added a new file")
    yield tmp_path
    LocalGitConfigSource.clear_caches()


@fixture
def normal_user_client(with_config_repo):
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
