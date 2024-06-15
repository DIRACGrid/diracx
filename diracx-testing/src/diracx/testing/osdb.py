from __future__ import annotations

import socket
from subprocess import PIPE, Popen, check_output

import pytest

from .dummy_osdb import DummyOSDB
from .mock_osdb import MockOSDBMixin

OPENSEARCH_PORT = 28000


def require_port_availability(port: int) -> bool:
    """Raise an exception if the given port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("localhost", port)) == 0:
            raise RuntimeError(f"This test requires port {port} to be available")


@pytest.fixture(scope="session")
def opensearch_conn_kwargs(demo_kubectl_env):
    """Fixture to get the OpenSearch connection kwargs.

    This fixture uses kubectl to forward a port from OpenSearch service in the
    diracx-demo. This port can then be used for testing DiracX against a real
    OpenSearch instance.
    """
    require_port_availability(OPENSEARCH_PORT)

    # Ensure the pod is running
    cmd = [
        "kubectl",
        "get",
        "pod/opensearch-cluster-master-0",
        "-o",
        "jsonpath={.status.phase}",
    ]
    pod_status = check_output(cmd, text=True, env=demo_kubectl_env)
    if pod_status != "Running":
        raise RuntimeError(f"OpenSearch pod is not running: {pod_status=}")

    # Forward the actual port and wait until it has been forwarded before yielding
    cmd = [
        "kubectl",
        "port-forward",
        "service/opensearch-cluster-master",
        f"{OPENSEARCH_PORT}:9200",
    ]
    output_lines = []
    with Popen(cmd, stdout=PIPE, stderr=PIPE, text=True, env=demo_kubectl_env) as proc:
        for line in proc.stdout:
            output_lines.append(line)
            if line.startswith("Forwarding from"):
                yield {
                    "hosts": f"admin:admin@localhost:{OPENSEARCH_PORT}",
                    "use_ssl": True,
                    "verify_certs": False,
                }
                proc.kill()
                break
        else:
            raise RuntimeError(
                f"Could not start port forwarding with {cmd=}\n{output_lines=}"
            )
    proc.wait()


@pytest.fixture
async def dummy_opensearch_db_without_template(opensearch_conn_kwargs):
    """Fixture which returns a DummyOSDB object."""
    db = DummyOSDB(opensearch_conn_kwargs)
    async with db.client_context():
        yield db
        # Clean up after the test
        await db.client.indices.delete(index=f"{db.index_prefix}*")


@pytest.fixture
async def dummy_opensearch_db(dummy_opensearch_db_without_template):
    """Fixture which returns a DummyOSDB object with the index template applied."""
    db = dummy_opensearch_db_without_template
    await db.create_index_template()
    yield db
    await db.client.indices.delete_index_template(name=db.index_prefix)


@pytest.fixture
async def sql_opensearch_db():
    """Fixture which returns a SQLOSDB object."""

    class MockDummyOSDB(MockOSDBMixin, DummyOSDB):
        pass

    db = MockDummyOSDB(
        connection_kwargs={"sqlalchemy_dsn": "sqlite+aiosqlite:///:memory:"}
    )
    async with db.client_context():
        await db.create_index_template()
        yield db
        # No need to cleanup as this uses an in-memory sqlite database
