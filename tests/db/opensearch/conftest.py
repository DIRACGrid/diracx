from __future__ import annotations

import secrets
import socket
import subprocess

import pytest

from diracx.db.os.utils import BaseOSDB

OPENSEARCH_PORT = 28000


def require_port_availability(port: int) -> bool:
    """Raise an exception if the given port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("localhost", port)) == 0:
            raise RuntimeError(f"This test requires port {port} to be available")


class DummyOSDB(BaseOSDB):
    """Example DiracX OpenSearch database class for testing.

    A new random prefix is created each time the class is defined to ensure
    test runs are independent of each other.
    """

    mapping = {
        "properties": {
            "DateField": {"type": "date"},
            "IntegerField": {"type": "long"},
            "KeywordField1": {"type": "keyword"},
            "KeywordField2": {"type": "keyword"},
        }
    }

    def __init__(self, *args, **kwargs):
        # Randomize the index prefix to ensure tests are independent
        self.index_prefix = f"dummy_{secrets.token_hex(8)}"
        super().__init__(*args, **kwargs)

    def index_name(self, doc_id: int) -> str:
        return f"{self.index_prefix}-{doc_id // 1e6:.0f}m"


@pytest.fixture(scope="session")
def opensearch_conn_kwargs(demo_kubectl_env):
    """Fixture which forwards a port from the diracx-demo and returns the connection kwargs."""
    require_port_availability(OPENSEARCH_PORT)
    command = [
        "kubectl",
        "port-forward",
        "service/opensearch-cluster-master",
        f"{OPENSEARCH_PORT}:9200",
    ]
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, universal_newlines=True, env=demo_kubectl_env
    ) as proc:
        for line in proc.stdout:
            if line.startswith("Forwarding from"):
                yield {
                    "hosts": f"admin:admin@localhost:{OPENSEARCH_PORT}",
                    "use_ssl": True,
                    "verify_certs": False,
                }
                proc.kill()
                break
    proc.wait()


@pytest.fixture
async def dummy_opensearch_db(opensearch_conn_kwargs):
    """Fixture which returns a DummyOSDB object."""
    db = DummyOSDB(opensearch_conn_kwargs)
    async with db.client_context():
        yield db
