"""Tests for the OpenTelemetry instrumentation of the routers."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from diracx.routers.factory import ClientMinVersionCheckMiddleware
from diracx.testing.otel import install_otel_providers, metric_value


@pytest.fixture(scope="session")
def otel_metrics():
    install_otel_providers()


@pytest.fixture
def client(otel_metrics):
    app = FastAPI()
    app.add_middleware(ClientMinVersionCheckMiddleware)

    @app.get("/api/jobs")
    async def jobs():
        return []

    @app.get("/api/health/live")
    async def live():
        return "ok"

    return TestClient(app)


def test_requests_are_counted_per_client_version(client):
    before = metric_value(
        "client_requests_total", client_version="1.0.0", outcome="accepted"
    )
    for _ in range(2):
        response = client.get("/api/jobs", headers={"DiracX-Client-Version": "1.0.0"})
        assert response.status_code == 200
    assert (
        metric_value(
            "client_requests_total", client_version="1.0.0", outcome="accepted"
        )
        == before + 2
    )


def test_outdated_and_invalid_clients_are_counted_as_rejected(client):
    rejected_before = metric_value(
        "client_requests_total", client_version="0.0.0", outcome="rejected"
    )
    invalid_before = metric_value(
        "client_requests_total", client_version="invalid", outcome="rejected"
    )

    response = client.get("/api/jobs", headers={"DiracX-Client-Version": "0.0.0"})
    assert response.status_code == 400
    response = client.get("/api/jobs", headers={"DiracX-Client-Version": "not!valid"})
    assert response.status_code == 400

    assert (
        metric_value(
            "client_requests_total", client_version="0.0.0", outcome="rejected"
        )
        == rejected_before + 1
    )
    assert (
        metric_value(
            "client_requests_total", client_version="invalid", outcome="rejected"
        )
        == invalid_before + 1
    )


def test_requests_without_version_and_probes(client):
    before = metric_value("client_requests_total", client_version="none")
    assert client.get("/api/jobs").status_code == 200
    # The kubernetes probes are not counted
    assert client.get("/api/health/live").status_code == 200
    assert metric_value("client_requests_total", client_version="none") == before + 1


def test_client_versions_are_bounded(client, monkeypatch):
    from diracx.routers import otel

    monkeypatch.setattr(otel, "_client_versions", set())
    monkeypatch.setattr(otel, "MAX_CLIENT_VERSIONS", 2)
    for version in ("7.0.0", "7.0.1", "7.0.2"):
        client.get("/api/jobs", headers={"DiracX-Client-Version": version})

    assert metric_value("client_requests_total", client_version="7.0.1") == 1
    assert metric_value("client_requests_total", client_version="7.0.2") == 0
    assert metric_value("client_requests_total", client_version="other") >= 1
