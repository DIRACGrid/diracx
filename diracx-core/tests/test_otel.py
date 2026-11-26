"""Tests for the OpenTelemetry setup shared by all the processes."""

from __future__ import annotations

import logging
import os
import shutil
import ssl
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)

from diracx.core.otel import configure_otel
from diracx.core.otel._setup import _ca_bundle, _create_exporters, _setup_log_handler
from diracx.core.settings import OTELSettings
from diracx.testing.otel import install_otel_providers


@pytest.fixture(scope="session")
def otel_providers():
    return install_otel_providers()


def test_configure_otel_disabled():
    assert configure_otel("test", settings=OTELSettings(enabled=False)) is None


@pytest.mark.parametrize(
    "settings, module, endpoints",
    [
        (
            OTELSettings(protocol="grpc", grpc_endpoint="collector:4317"),
            "grpc",
            ["collector:4317"] * 3,
        ),
        (
            OTELSettings(protocol="http", http_endpoint="https://collector:4318/"),
            "http",
            [
                "https://collector:4318/v1/traces",
                "https://collector:4318/v1/metrics",
                "https://collector:4318/v1/logs",
            ],
        ),
    ],
)
def test_exporters_follow_the_protocol(settings, module, endpoints):
    exporters = _create_exporters(settings)
    assert [type(e).__module__.split(".")[-2] for e in exporters] == [module] * 3
    assert [e._endpoint for e in exporters] == endpoints


def test_unknown_protocol_is_rejected():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        OTELSettings(protocol="udp")


def _self_signed_ca(common_name: str) -> tuple[bytes, bytes]:
    """Return a PEM self-signed CA certificate, valid for localhost, and its key."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
        )
        .sign(key, hashes.SHA256())
    )
    private_key = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return certificate.public_bytes(serialization.Encoding.PEM), private_key


PEM = b"-----BEGIN CERTIFICATE-----\nfirst\n-----END CERTIFICATE-----"


def test_ca_bundle_reads_only_hashed_certificates(tmp_path):
    (tmp_path / "01234567.0").write_bytes(PEM)
    (tmp_path / "89abcdef.1").symlink_to(tmp_path / "01234567.0")
    (tmp_path / "01234567.r0").write_text("a CRL")
    (tmp_path / "01234567.signing_policy").write_text("a policy")
    (tmp_path / "ca.pem").write_bytes(PEM)
    # Not usable as certificates, despite their names
    (tmp_path / "aaaaaaaa.0").write_bytes(b"not PEM")
    (tmp_path / "bbbbbbbb.0").symlink_to(tmp_path / "removed.pem")
    (tmp_path / "cccccccc.0").mkdir()
    assert _ca_bundle(tmp_path) == PEM + b"\n" + PEM + b"\n"


@pytest.mark.parametrize("content", [None, b"not PEM"])
def test_ca_bundle_rejects_directory_without_hashed_certificates(tmp_path, content):
    (tmp_path / "ca.pem").write_bytes(PEM)
    if content:
        (tmp_path / "01234567.0").write_bytes(content)
    with pytest.raises(ValueError, match="No OpenSSL-hashed PEM CA certificates"):
        _ca_bundle(tmp_path)


def test_ca_bundle_rejects_unreadable_certificate(tmp_path):
    (tmp_path / "01234567.0").write_bytes(PEM)
    (tmp_path / "01234567.0").chmod(0)
    if os.access(tmp_path / "01234567.0", os.R_OK):
        pytest.skip("Permissions are not enforced for this user")
    with pytest.raises(ValueError, match="Cannot read the CA certificates"):
        _ca_bundle(tmp_path)


@pytest.mark.parametrize("protocol", ["http", "grpc"])
def test_ca_directory_is_checked_when_creating_exporters(protocol, tmp_path):
    with pytest.raises(ValueError, match="No OpenSSL-hashed PEM CA certificates"):
        _create_exporters(OTELSettings(protocol=protocol, ca_path=tmp_path))


def test_ca_directory_is_reported_as_ignored_without_tls(tmp_path, caplog):
    (tmp_path / "01234567.0").write_bytes(PEM)
    settings = OTELSettings(
        protocol="http", http_endpoint="http://collector:4318", ca_path=tmp_path
    )
    with caplog.at_level(logging.WARNING, logger="diracx.core.otel._setup"):
        exporters = _create_exporters(settings)
    for exporter in exporters:
        exporter.shutdown()
    assert "DIRACX_OTEL_CA_PATH is ignored" in caplog.text


def test_missing_ca_directory_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        OTELSettings(ca_path=tmp_path / "missing")


def test_ca_directory_enables_grpc_tls(tmp_path):
    (tmp_path / "01234567.0").write_bytes(_self_signed_ca("Test CA")[0])
    exporters = _create_exporters(OTELSettings(protocol="grpc", ca_path=tmp_path))
    try:
        assert all(not e._insecure and e._credentials for e in exporters)
    finally:
        for exporter in exporters:
            exporter.shutdown()


@pytest.mark.parametrize("protocol", ["http", "grpc"])
def test_exporters_connect_to_tls_collector_with_ca_directory(
    protocol, tmp_path, monkeypatch
):
    import certifi
    import grpc
    from opentelemetry.proto.collector.logs.v1 import (
        logs_service_pb2,
        logs_service_pb2_grpc,
    )
    from opentelemetry.proto.collector.metrics.v1 import (
        metrics_service_pb2,
        metrics_service_pb2_grpc,
    )
    from opentelemetry.proto.collector.trace.v1 import (
        trace_service_pb2,
        trace_service_pb2_grpc,
    )
    from opentelemetry.sdk._logs.export import LogRecordExportResult
    from opentelemetry.sdk.metrics.export import MetricExportResult, MetricsData
    from opentelemetry.sdk.trace.export import SpanExportResult

    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("openssl is required to prepare the CA directory")
    # The CA bundle of the other clients of the process must not be used instead
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", certifi.where())
    cert, private_key = _self_signed_ca("localhost")
    ca_path = tmp_path / "ca"
    ca_path.mkdir()
    (ca_path / "ca.pem").write_bytes(cert)
    subprocess.run([openssl, "rehash", str(ca_path)], check=True)
    cert_file, key_file = tmp_path / "server.pem", tmp_path / "server.key"
    cert_file.write_bytes(cert)
    key_file.write_bytes(private_key)
    received = []

    if protocol == "http":

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                received.append(self.path)
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(200)
                self.end_headers()

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(("localhost", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_file, key_file)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        settings = OTELSettings(
            protocol="http",
            http_endpoint=f"https://localhost:{server.server_port}",
            ca_path=ca_path,
        )
    else:

        class TraceService(trace_service_pb2_grpc.TraceServiceServicer):
            def Export(self, request, context):  # noqa: N802
                received.append("traces")
                return trace_service_pb2.ExportTraceServiceResponse()

        class MetricsService(metrics_service_pb2_grpc.MetricsServiceServicer):
            def Export(self, request, context):  # noqa: N802
                received.append("metrics")
                return metrics_service_pb2.ExportMetricsServiceResponse()

        class LogsService(logs_service_pb2_grpc.LogsServiceServicer):
            def Export(self, request, context):  # noqa: N802
                received.append("logs")
                return logs_service_pb2.ExportLogsServiceResponse()

        server = grpc.server(ThreadPoolExecutor(max_workers=3))
        trace_service_pb2_grpc.add_TraceServiceServicer_to_server(
            TraceService(), server
        )
        metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(
            MetricsService(), server
        )
        logs_service_pb2_grpc.add_LogsServiceServicer_to_server(LogsService(), server)
        port = server.add_secure_port(
            "localhost:0", grpc.ssl_server_credentials([(private_key, cert)])
        )
        server.start()
        settings = OTELSettings(
            protocol="grpc",
            grpc_endpoint=f"localhost:{port}",
            ca_path=ca_path,
        )

    exporters = ()
    try:
        exporters = _create_exporters(settings)
        traces, metrics, logs = exporters
        assert traces.export([]) == SpanExportResult.SUCCESS
        assert (
            metrics.export(MetricsData(resource_metrics=[]))
            == MetricExportResult.SUCCESS
        )
        assert logs.export([]) == LogRecordExportResult.SUCCESS
        assert received == (
            ["/v1/traces", "/v1/metrics", "/v1/logs"]
            if protocol == "http"
            else ["traces", "metrics", "logs"]
        )
    finally:
        for exporter in exporters:
            exporter.shutdown()
        if protocol == "http":
            server.shutdown()
            thread.join()
            server.server_close()
        else:
            server.stop(0).wait()


def test_logs_are_exported_once_with_the_message_as_body(otel_providers):
    tracer_provider, _ = otel_providers
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider()
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    handler = _setup_log_handler(logger_provider, ["diracx.test_otel_logs"])
    logger = logging.getLogger("diracx.test_otel_logs.child")
    logger.setLevel(logging.INFO)
    try:
        with tracer_provider.get_tracer("test").start_as_current_span("x") as span:
            logger.info("hello %s", "world")
            try:
                raise ValueError("boom")
            except ValueError:
                logger.exception("failed")
        # Not below the configured loggers: not exported
        logging.getLogger("sqlalchemy.test_otel_logs").warning("third party")
    finally:
        logging.getLogger("diracx.test_otel_logs").removeHandler(handler)

    records = [r.log_record for r in exporter.get_finished_logs()]
    assert [r.body for r in records] == ["hello world", "failed"]
    assert {r.trace_id for r in records} == {span.get_span_context().trace_id}
    assert records[1].attributes["exception.type"] == "ValueError"
    assert "boom" in records[1].attributes["exception.stacktrace"]


def test_uvicorn_access_logs_are_structured_in_the_exported_records():
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider()
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    access = logging.getLogger("uvicorn.access")
    level = access.level
    access.setLevel(logging.INFO)
    handler = _setup_log_handler(logger_provider, ["uvicorn.access"])
    try:
        access.info(
            '%s - "%s %s HTTP/%s" %d',
            "127.0.0.1:1234",
            "POST",
            "/api/jobs/",
            "1.1",
            201,
        )
    finally:
        access.removeHandler(handler)
        access.setLevel(level)

    [record] = [r.log_record for r in exporter.get_finished_logs()]
    assert record.attributes["http.request.method"] == "POST"
    assert record.attributes["url.path"] == "/api/jobs/"
    assert record.attributes["http.response.status_code"] == 201
    assert record.attributes["client.address"] == "127.0.0.1:1234"
    assert record.attributes["network.protocol.version"] == "1.1"
