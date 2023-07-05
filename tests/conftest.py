from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from git import Repo

from diracx.core.config import Config, ConfigSource
from diracx.core.properties import NORMAL_USER
from diracx.routers import create_app_inner
from diracx.routers.auth import AuthSettings, create_access_token

# to get a string like this run:
# openssl rand -hex 32
ALGORITHM = "HS256"
ISSUER = "http://lhcbdirac.cern.ch/"
AUDIENCE = "dirac"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def pytest_addoption(parser):
    parser.addoption(
        "--regenerate-client",
        action="store_true",
        default=False,
        help="Regenerate the AutoREST client",
    )


def pytest_collection_modifyitems(config, items):
    """
    Disable the test_regenerate_client if not explicitly asked for
    """
    if config.getoption("--regenerate-client"):
        # --regenerate-client given in cli: allow client re-generation
        return
    skip_regen = pytest.mark.skip(reason="need --regenerate-client option to run")
    found = False
    for item in items:
        if item.name == "test_regenerate_client":
            item.add_marker(skip_regen)
            found = True
    # It's ok not to find it if we run only specific tests
    if not found and not config.getoption("file_or_dir"):
        raise RuntimeError("Could not find test_regenerate_client")


@pytest.fixture
def test_auth_settings() -> AuthSettings:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    yield AuthSettings(
        token_key=pem,
        allowed_redirects=[
            "http://diracx.test.invalid:8000/docs/oauth2-redirect",
        ],
    )


@pytest.fixture
def with_app(test_auth_settings, with_config_repo):
    """
    Create a DiracxApp with hard coded configuration for test
    """
    yield create_app_inner(
        enabled_systems={".well-known", "auth", "config", "jobs"},
        all_service_settings=[test_auth_settings],
        database_urls={
            "JobDB": "sqlite+aiosqlite:///:memory:",
            "AuthDB": "sqlite+aiosqlite:///:memory:",
        },
        config_source=ConfigSource.create_from_url(
            backend_url=f"git+file://{with_config_repo}"
        ),
    )


@pytest.fixture
def with_config_repo(tmp_path):
    repo = Repo.init(tmp_path, initial_branch="master")
    cs_file = tmp_path / "default.yml"
    example_cs = Config.parse_obj(
        {
            "DIRAC": {},
            "Registry": {
                "lhcb": {
                    "DefaultGroup": "lhcb_user",
                    "DefaultProxyLifeTime": 432000,
                    "DefaultStorageQuota": 2000,
                    "IdP": {
                        "URL": "https://lhcb-auth.web.cern.ch",
                        "ClientID": "test-idp",
                    },
                    "Users": {
                        "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041": {
                            "PreferedUsername": "chaen",
                            "Email": None,
                            "DN": "/DC=invalid/DC=testca/OU=Organic Units/OU=Users/CN=chaen/CN=1234/CN=Christophe Haen",
                            "CA": "/DC=invalid/DC=testca/CN=Test CA",
                        }
                    },
                    "Groups": {
                        "lhcb_user": {
                            "Properties": ["NormalUser", "PrivateLimitedDelegation"],
                            "Users": ["b824d4dc-1f9d-4ee8-8df5-c0ae55d46041"],
                        }
                    },
                }
            },
            "Operations": {"Defaults": {}},
        }
    )
    cs_file.write_text(example_cs.json())
    repo.index.add([cs_file])  # add it to the index
    repo.index.commit("Added a new file")
    yield tmp_path


@pytest.fixture
def test_client(with_app):
    with TestClient(with_app) as test_client:
        yield test_client


@pytest.fixture
def normal_user_client(test_client, test_auth_settings):
    payload = {
        "sub": "testingVO:yellow-sub",
        "aud": AUDIENCE,
        "iss": ISSUER,
        "dirac_properties": [NORMAL_USER],
        "jti": str(uuid4()),
        "preferred_username": "preferred_username",
        "dirac_group": "test_group",
        "vo": "lhcb",
    }
    token = create_access_token(payload, test_auth_settings)
    test_client.headers["Authorization"] = f"Bearer {token}"
    test_client.dirac_token_payload = payload
    yield test_client
