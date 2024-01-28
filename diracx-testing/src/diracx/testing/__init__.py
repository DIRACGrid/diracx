from __future__ import annotations

import asyncio
import contextlib
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urljoin, urlparse
from uuid import uuid4

import pytest
import requests

if TYPE_CHECKING:
    from diracx.core.properties import SecurityProperty
    from diracx.routers.auth import AuthSettings
    from diracx.routers.job_manager.sandboxes import SandboxStoreSettings

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
    parser.addoption(
        "--demo-dir",
        type=Path,
        default=None,
        help="Path to a diracx-charts directory with the demo running",
    )


def pytest_collection_modifyitems(config, items):
    """
    Disable the test_regenerate_client if not explicitly asked for
    """
    if config.getoption("--regenerate-client"):
        # --regenerate-client given in cli: allow client re-generation
        return
    skip_regen = pytest.mark.skip(reason="need --regenerate-client option to run")
    for item in items:
        if item.name == "test_regenerate_client":
            item.add_marker(skip_regen)


@pytest.fixture(scope="session")
def rsa_private_key_pem() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture(scope="session")
def test_auth_settings(rsa_private_key_pem) -> AuthSettings:
    from diracx.routers.auth import AuthSettings

    yield AuthSettings(
        token_key=rsa_private_key_pem,
        allowed_redirects=[
            "http://diracx.test.invalid:8000/api/docs/oauth2-redirect",
        ],
    )


@pytest.fixture(scope="session")
def aio_moto(worker_id):
    """Start the moto server in a separate thread and return the base URL

    The mocking provided by moto doesn't play nicely with aiobotocore so we use
    the server directly. See https://github.com/aio-libs/aiobotocore/issues/755
    """
    from moto.server import ThreadedMotoServer

    port = 27132
    if worker_id != "master":
        port += int(worker_id.replace("gw", "")) + 1
    server = ThreadedMotoServer(port=port)
    server.start()
    yield {
        "endpoint_url": f"http://localhost:{port}",
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
    }
    server.stop()


@pytest.fixture(scope="session")
def test_sandbox_settings(aio_moto) -> SandboxStoreSettings:
    from diracx.routers.job_manager.sandboxes import SandboxStoreSettings

    yield SandboxStoreSettings(
        bucket_name="sandboxes",
        s3_client_kwargs=aio_moto,
        auto_create_bucket=True,
    )


class UnavailableDependency:
    def __init__(self, key):
        self.key = key

    def __call__(self):
        raise NotImplementedError(
            f"{self.key} has not been made available to this test!"
        )


class ClientFactory:
    def __init__(
        self,
        tmp_path_factory,
        with_config_repo,
        test_auth_settings,
        test_sandbox_settings,
    ):
        from diracx.core.config import ConfigSource
        from diracx.core.extensions import select_from_extension
        from diracx.core.settings import ServiceSettingsBase
        from diracx.db.sql.utils import BaseSQLDB
        from diracx.routers import create_app_inner

        enabled_systems = {
            e.name for e in select_from_extension(group="diracx.services")
        }
        database_urls = {
            e.name: "sqlite+aiosqlite:///:memory:"
            for e in select_from_extension(group="diracx.db.sql")
        }
        self._cache_dir = tmp_path_factory.mktemp("empty-dbs")
        self._config_source = ConfigSource.create_from_url(
            backend_url=f"git+file://{with_config_repo}"
        )

        self.test_auth_settings = test_auth_settings

        self.app = create_app_inner(
            enabled_systems=enabled_systems,
            all_service_settings=[
                test_auth_settings,
                test_sandbox_settings,
            ],
            database_urls=database_urls,
            os_database_conn_kwargs={
                # TODO: JobParametersDB
            },
            config_source=self._config_source,
        )

        self.all_dependency_overrides = self.app.dependency_overrides.copy()
        self.app.dependency_overrides = {}
        for obj in self.all_dependency_overrides:
            assert issubclass(
                obj.__self__, (ServiceSettingsBase, BaseSQLDB, ConfigSource)
            ), obj

        self.all_lifetime_functions = self.app.lifetime_functions[:]
        self.app.lifetime_functions = []
        for obj in self.all_lifetime_functions:
            assert isinstance(
                obj.__self__, (ServiceSettingsBase, BaseSQLDB, ConfigSource)
            ), obj

    @contextlib.contextmanager
    def configure(self, enabled_dependencies):
        assert (
            self.app.dependency_overrides == {} and self.app.lifetime_functions == []
        ), "configure cannot be nested"

        for k, v in self.all_dependency_overrides.items():
            class_name = k.__self__.__name__
            if class_name in enabled_dependencies:
                self.app.dependency_overrides[k] = v
            else:
                self.app.dependency_overrides[k] = UnavailableDependency(class_name)

        for obj in self.all_lifetime_functions:
            if obj.__self__.__class__.__name__ in enabled_dependencies:
                self.app.lifetime_functions.append(obj)

        # Add create_db_schemas to the end of the lifetime_functions so that the
        # other lifetime_functions (i.e. those which run db.engine_context) have
        # already been ran
        self.app.lifetime_functions.append(self.create_db_schemas)

        yield

        self.app.dependency_overrides = {}
        self.app.lifetime_functions = []

    @contextlib.asynccontextmanager
    async def create_db_schemas(self):
        """Create DB schema's based on the DBs available in app.dependency_overrides"""
        import aiosqlite
        import sqlalchemy
        from sqlalchemy.util.concurrency import greenlet_spawn

        from diracx.db.sql.utils import BaseSQLDB

        for k, v in self.app.dependency_overrides.items():
            # Ignore dependency overrides which aren't BaseSQLDB.transaction
            if (
                isinstance(v, UnavailableDependency)
                or k.__func__ != BaseSQLDB.transaction.__func__
            ):
                continue
            # The first argument of the overridden BaseSQLDB.transaction is the DB object
            db = v.args[0]
            assert isinstance(db, BaseSQLDB), (k, db)

            # set PRAGMA foreign_keys=ON if sqlite
            if db.engine.url.drivername.startswith("sqlite"):

                def set_sqlite_pragma(dbapi_connection, connection_record):
                    cursor = dbapi_connection.cursor()
                    cursor.execute("PRAGMA foreign_keys=ON")
                    cursor.close()

                sqlalchemy.event.listen(
                    db.engine.sync_engine, "connect", set_sqlite_pragma
                )

            # We maintain a cache of the populated DBs in empty_db_dir so that
            # we don't have to recreate them for every test. This speeds up the
            # tests by a considerable amount.
            ref_db = self._cache_dir / f"{k.__self__.__name__}.db"
            if ref_db.exists():
                async with aiosqlite.connect(ref_db) as ref_conn:
                    conn = await db.engine.raw_connection()
                    await ref_conn.backup(conn.driver_connection)
                    await greenlet_spawn(conn.close)
            else:
                async with db.engine.begin() as conn:
                    await conn.run_sync(db.metadata.create_all)

                async with aiosqlite.connect(ref_db) as ref_conn:
                    conn = await db.engine.raw_connection()
                    await conn.driver_connection.backup(ref_conn)
                    await greenlet_spawn(conn.close)

        yield

    @contextlib.contextmanager
    def unauthenticated(self):
        from fastapi.testclient import TestClient

        with TestClient(self.app) as client:
            yield client

    @contextlib.contextmanager
    def normal_user(
        self,
        *,
        vo: str = "lhcb",
        sub: str = "yellow-sub",
        group: str = "test_group",
        properties: list[SecurityProperty] = None,
    ):
        from diracx.core.properties import NORMAL_USER, PRIVATE_LIMITED_DELEGATION
        from diracx.routers.auth import create_token

        if properties is None:
            properties = [NORMAL_USER, PRIVATE_LIMITED_DELEGATION]

        with self.unauthenticated() as client:
            payload = {
                "sub": f"{vo}:{sub}",
                "exp": datetime.now(tz=timezone.utc)
                + timedelta(self.test_auth_settings.access_token_expire_minutes),
                "aud": AUDIENCE,
                "iss": ISSUER,
                "dirac_properties": properties,
                "jti": str(uuid4()),
                "preferred_username": "preferred_username",
                "dirac_group": group,
                "vo": vo,
            }
            token = create_token(payload, self.test_auth_settings)

            client.headers["Authorization"] = f"Bearer {token}"
            client.dirac_token_payload = payload
            yield client

    @contextlib.contextmanager
    def admin_user(self):
        from diracx.core.properties import JOB_ADMINISTRATOR
        from diracx.routers.auth import create_token

        with self.unauthenticated() as client:
            payload = {
                "sub": "testingVO:yellow-sub",
                "aud": AUDIENCE,
                "iss": ISSUER,
                "dirac_properties": [JOB_ADMINISTRATOR],
                "jti": str(uuid4()),
                "preferred_username": "preferred_username",
                "dirac_group": "test_group",
                "vo": "lhcb",
            }
            token = create_token(payload, self.test_auth_settings)
            client.headers["Authorization"] = f"Bearer {token}"
            client.dirac_token_payload = payload
            yield client


@pytest.fixture(scope="session")
def session_client_factory(
    test_auth_settings, test_sandbox_settings, with_config_repo, tmp_path_factory
):
    """
    TODO
    """
    yield ClientFactory(
        tmp_path_factory, with_config_repo, test_auth_settings, test_sandbox_settings
    )


@pytest.fixture
def client_factory(session_client_factory, request):
    marker = request.node.get_closest_marker("enabled_dependencies")
    if marker is None:
        raise RuntimeError("This test requires the enabled_dependencies marker")
    (enabled_dependencies,) = marker.args
    with session_client_factory.configure(enabled_dependencies=enabled_dependencies):
        yield session_client_factory


@pytest.fixture(scope="session")
def with_config_repo(tmp_path_factory):
    from git import Repo

    from diracx.core.config import Config

    tmp_path = tmp_path_factory.mktemp("cs-repo")

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
                            "DNs": ["/O=Dirac Computing/O=CERN/CN=MrUser"],
                        },
                        "c935e5ed-2g0e-5ff9-9eg6-d1bf66e57152": {
                            "PreferedUsername": "albdr",
                            "Email": None,
                        },
                    },
                    "Groups": {
                        "lhcb_user": {
                            "Properties": ["NormalUser", "PrivateLimitedDelegation"],
                            "Users": [
                                "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041",
                                "c935e5ed-2g0e-5ff9-9eg6-d1bf66e57152",
                            ],
                            "VOMSRole": "arole",
                        },
                        "lhcb_tokenmgr": {
                            "Properties": ["NormalUser", "ProxyManagement"],
                            "Users": ["c935e5ed-2g0e-5ff9-9eg6-d1bf66e57152"],
                        },
                    },
                    "VOMS": {
                        "Servers": {
                            "voms.lhcb.invalid": {
                                "Info": '"lhcb" "voms.lhcb.invalid" "1256" '
                                '"/DC=mars/OU=computers/CN=voms.lhcb.invalid" "lhcb" "24"',
                                "Chain": [
                                    "/DC=mars/OU=computers/CN=voms.lhcb.invalid",
                                    "/DC=mars/CN=Fake Certification Authority",
                                ],
                            }
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


@pytest.fixture(scope="session")
def demo_dir(request) -> Path:
    demo_dir = request.config.getoption("--demo-dir")
    if demo_dir is None:
        pytest.skip("Requires a running instance of the DiracX demo")
    demo_dir = (demo_dir / ".demo").resolve()
    yield demo_dir


@pytest.fixture(scope="session")
def demo_urls(demo_dir):
    import yaml

    helm_values = yaml.safe_load((demo_dir / "values.yaml").read_text())
    yield helm_values["developer"]["urls"]


@pytest.fixture(scope="session")
def demo_kubectl_env(demo_dir):
    """Get the dictionary of environment variables for kubectl to control the demo"""
    kube_conf = demo_dir / "kube.conf"
    if not kube_conf.exists():
        raise RuntimeError(f"Could not find {kube_conf}, is the demo running?")

    env = {
        **os.environ,
        "KUBECONFIG": str(kube_conf),
        "PATH": f"{demo_dir}:{os.environ['PATH']}",
    }

    # Check that we can run kubectl
    pods_result = subprocess.check_output(
        ["kubectl", "get", "pods"], env=env, text=True
    )
    assert "diracx" in pods_result

    yield env


@pytest.fixture
def cli_env(monkeypatch, tmp_path, demo_urls, demo_dir):
    """Set up the environment for the CLI"""
    import httpx

    from diracx.core.preferences import get_diracx_preferences

    diracx_url = demo_urls["diracx"]
    ca_path = demo_dir / "demo-ca.pem"
    if not ca_path.exists():
        raise RuntimeError(f"Could not find {ca_path}, is the demo running?")

    # Ensure the demo is working
    r = httpx.get(f"{diracx_url}/api/openapi.json", verify=ca_path)
    r.raise_for_status()
    assert r.json()["info"]["title"] == "Dirac"

    env = {
        "DIRACX_URL": diracx_url,
        "DIRACX_CA_PATH": str(ca_path),
        "HOME": str(tmp_path),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    yield env

    # The DiracX preferences are cached however when testing this cache is invalid
    get_diracx_preferences.cache_clear()


@pytest.fixture
async def with_cli_login(monkeypatch, capfd, cli_env, tmp_path):
    try:
        credentials = await test_login(monkeypatch, capfd, cli_env)
    except Exception:
        pytest.skip("Login failed, fix test_login to re-enable this test")

    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text(credentials)
    monkeypatch.setenv("DIRACX_CREDENTIALS_PATH", str(credentials_path))
    yield


async def test_login(monkeypatch, capfd, cli_env):
    from diracx import cli

    poll_attempts = 0

    def fake_sleep(*args, **kwargs):
        nonlocal poll_attempts

        # Keep track of the number of times this is called
        poll_attempts += 1

        # After polling 5 times, do the actual login
        if poll_attempts == 5:
            # The login URL should have been printed to stdout
            captured = capfd.readouterr()
            match = re.search(rf"{cli_env['DIRACX_URL']}[^\n]+", captured.out)
            assert match, captured

            do_device_flow_with_dex(match.group(), cli_env["DIRACX_CA_PATH"])

        # Ensure we don't poll forever
        assert poll_attempts <= 100

        # Reduce the sleep duration to zero to speed up the test
        return unpatched_sleep(0)

    # We monkeypatch asyncio.sleep to provide a hook to run the actions that
    # would normally be done by a user. This includes capturing the login URL
    # and doing the actual device flow with dex.
    unpatched_sleep = asyncio.sleep

    expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )

    # Ensure the credentials file does not exist before logging in
    assert not expected_credentials_path.exists()

    # Run the login command
    with monkeypatch.context() as m:
        m.setattr("asyncio.sleep", fake_sleep)
        await cli.login(vo="diracAdmin", group=None, property=None)
    captured = capfd.readouterr()
    assert "Login successful!" in captured.out
    assert captured.err == ""

    # Ensure the credentials file exists after logging in
    assert expected_credentials_path.exists()

    # Return the credentials so this test can also be used by the
    # "with_cli_login" fixture
    return expected_credentials_path.read_text()


def do_device_flow_with_dex(url: str, ca_path: str) -> None:
    """Do the device flow with dex"""

    class DexLoginFormParser(HTMLParser):
        def handle_starttag(self, tag, attrs):
            nonlocal action_url
            if "form" in str(tag):
                assert action_url is None
                action_url = urljoin(login_page_url, dict(attrs)["action"])

    # Get the login page
    r = requests.get(url, verify=ca_path)
    r.raise_for_status()
    login_page_url = r.url  # This is not the same as URL as we redirect to dex
    login_page_body = r.text

    # Search the page for the login form so we know where to post the credentials
    action_url = None
    DexLoginFormParser().feed(login_page_body)
    assert action_url is not None, login_page_body

    # Do the actual login
    r = requests.post(
        action_url,
        data={"login": "admin@example.com", "password": "password"},
        verify=ca_path,
    )
    r.raise_for_status()
    approval_url = r.url  # This is not the same as URL as we redirect to dex
    # Do the actual approval
    r = requests.post(
        approval_url,
        {"approval": "approve", "req": parse_qs(urlparse(r.url).query)["req"][0]},
        verify=ca_path,
    )

    # This should have redirected to the DiracX page that shows the login is complete
    assert "Please close the window" in r.text
