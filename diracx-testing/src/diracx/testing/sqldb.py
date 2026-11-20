"""Pytest fixtures to run SQL database tests against real database servers.

By default SQL tests run against an in-memory SQLite database. When pytest is
given ``--demo-dir`` (pointing at a diracx-charts checkout with the demo
running), the ``sql_db_url`` fixture also provides the MySQL and MariaDB
servers of the demo, so that the same tests also run against them.

Each MySQL/MariaDB test gets its own freshly created database, which is
dropped afterwards, so tests never share state with each other nor with the
services of the demo.
"""

from __future__ import annotations

__all__ = [
    "SERVER_SQL_BACKENDS",
    "SQL_BACKENDS",
    "mariadb_db_url",
    "mariadb_server_url",
    "mysql_db_url",
    "mysql_server_url",
    "sql_db_url",
    "sql_db_with_schema",
]

import base64
import contextlib
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from subprocess import check_output
from typing import TYPE_CHECKING, TypeVar

import pytest
from sqlalchemy import event
from sqlalchemy.engine import URL, make_url
from uuid_utils import uuid7

from .utils import kubectl_port_forward

if TYPE_CHECKING:
    from diracx.db.sql.utils import BaseSQLDB

    T = TypeVar("T", bound=BaseSQLDB)

SQLITE_MEMORY_URL = "sqlite+aiosqlite:///:memory:"
SQL_BACKENDS = ["sqlite", "mysql", "mariadb"]
# The backends which are real database servers, provided by the demo
SERVER_SQL_BACKENDS = ["mysql", "mariadb"]

# Prefix of the databases created by the tests
TEST_DB_PREFIX = "diracx_test_"


@dataclass(frozen=True)
class _DemoDBServer:
    """Where to find a database server in the demo."""

    name: str
    # Label selector matching both the pod and the (non-headless) service
    selector: str
    # Secret and key holding the root password
    secret: str
    secret_key: str


_MYSQL = _DemoDBServer(
    name="MySQL",
    selector="app.kubernetes.io/name=mysql,app.kubernetes.io/component=primary",
    secret="mysql-secret",
    secret_key="mysql-root-password",
)
# The test MariaDB of the demo uses the same root password as MySQL
_MARIADB = _DemoDBServer(
    name="MariaDB",
    selector="app.kubernetes.io/name=mariadb,app.kubernetes.io/component=test-database",
    secret="mysql-secret",
    secret_key="mysql-root-password",
)


def _kubectl(env: dict[str, str], *args: str) -> str:
    return check_output(["kubectl", *args], text=True, env=env).strip()


def _demo_server_url(server: _DemoDBServer, demo_kubectl_env) -> Iterator[str]:
    """Port-forward a database server of the demo and yield its root URL.

    The service is forwarded to a random local port, so that several
    pytest-xdist workers can each forward it without conflicting.
    The URL is yielded as a string, with the password escaped.

    Root is used because the DiracX user of the demo is only granted access to
    the pre-created DiracX databases, and the tests need to create their own.
    """
    pod_status = _kubectl(
        demo_kubectl_env,
        "get",
        "pod",
        "-l",
        server.selector,
        "-o",
        "jsonpath={.items[*].status.phase}",
    )
    if not pod_status:
        pytest.skip(f"{server.name} is not deployed in the demo")
    if pod_status != "Running":
        raise RuntimeError(f"{server.name} pod is not running: {pod_status=}")

    service = _kubectl(
        demo_kubectl_env,
        "get",
        "service",
        "-l",
        server.selector,
        "-o",
        "jsonpath={.items[?(@.spec.clusterIP!='None')].metadata.name}",
    )
    root_password = base64.b64decode(
        _kubectl(
            demo_kubectl_env,
            "get",
            "secret",
            server.secret,
            "-o",
            f"jsonpath={{.data.{server.secret_key}}}",
        )
    ).decode()

    with kubectl_port_forward(demo_kubectl_env, f"service/{service}", 3306) as port:
        # URL.create takes care of escaping the password
        yield URL.create(
            "mysql+aiomysql",
            username="root",
            password=root_password,
            host="127.0.0.1",
            port=port,
        ).render_as_string(hide_password=False)


@pytest.fixture(scope="session")
def mysql_server_url(demo_kubectl_env) -> Iterator[str]:
    """Root URL (without database) of the MySQL server of the demo."""
    yield from _demo_server_url(_MYSQL, demo_kubectl_env)


@pytest.fixture(scope="session")
def mariadb_server_url(demo_kubectl_env) -> Iterator[str]:
    """Root URL (without database) of the test MariaDB server of the demo."""
    yield from _demo_server_url(_MARIADB, demo_kubectl_env)


@contextlib.contextmanager
def _fresh_database(server_url: str) -> Iterator[str]:
    """Create an empty database on the server, and drop it afterwards."""
    from sqlalchemy import create_engine, text

    db_name = f"{TEST_DB_PREFIX}{uuid7().hex}"
    url = make_url(server_url)
    # A synchronous engine avoids depending on the event loop of the test
    engine = create_engine(
        url.set(drivername="mysql+pymysql"), isolation_level="AUTOCOMMIT"
    )
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE `{db_name}`"))
        yield url.set(database=db_name).render_as_string(hide_password=False)
    finally:
        with engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{db_name}`"))
        engine.dispose()


@pytest.fixture
def mysql_db_url(mysql_server_url) -> Iterator[str]:
    """URL of a freshly created, empty MySQL database, dropped after the test."""
    with _fresh_database(mysql_server_url) as url:
        yield url


@pytest.fixture
def mariadb_db_url(mariadb_server_url) -> Iterator[str]:
    """URL of a freshly created, empty MariaDB database, dropped after the test."""
    with _fresh_database(mariadb_server_url) as url:
        yield url


@pytest.fixture(params=SQL_BACKENDS)
def sql_db_url(request) -> str:
    """URL of an empty database, for each of the supported SQL backends.

    The MySQL and MariaDB variants are skipped unless pytest is given
    ``--demo-dir``.
    """
    if request.param == "sqlite":
        return SQLITE_MEMORY_URL
    return request.getfixturevalue(f"{request.param}_db_url")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "xfail_sql_backends(backends, *, reason): mark the variants of a test "
        "using sql_db_url with one of the given backends as (strictly) expected "
        "to fail",
    )


def pytest_collection_modifyitems(items):
    """Turn ``xfail_sql_backends`` into ``xfail`` on the matching variants.

    This is done at collection time (rather than from within the fixture) so
    that failures happening while setting up other fixtures are covered too.
    """
    for item in items:
        marker = item.get_closest_marker("xfail_sql_backends")
        callspec = getattr(item, "callspec", None)
        if marker is None or callspec is None:
            continue
        if callspec.params.get("sql_db_url") in marker.args[0]:
            item.add_marker(
                pytest.mark.xfail(reason=marker.kwargs["reason"], strict=True)
            )


@contextlib.asynccontextmanager
async def sql_db_with_schema(db: T) -> AsyncIterator[T]:
    """Enter the engine context of ``db`` and create its schema.

    SQLite only enforces foreign keys when asked to, per connection, so it is
    asked to, to behave like MySQL.
    """
    async with db.engine_context():
        if db.engine.dialect.name == "sqlite":
            event.listen(
                db.engine.sync_engine,
                "connect",
                lambda dbapi_conn, _: dbapi_conn.cursor().execute(
                    "PRAGMA foreign_keys=ON"
                ),
            )
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
            await db.post_create(conn)
        yield db
