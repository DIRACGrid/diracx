"""Synthetic source checks for the public release environment template."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "generate_release_env", ROOT / "scripts/generate_release_env.py"
)
assert SPEC and SPEC.loader
release_env = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release_env
SPEC.loader.exec_module(release_env)
COMMIT = "a" * 40
CANARY = "SYNTHETIC_SECRET_CANARY_DO_NOT_PUBLISH"

DEMO_NAMES = (
    "DIRACX_SERVICE_AUTH_TOKEN_ISSUER",
    "DIRACX_CONFIG_BACKEND_URL",
    "DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS",
    "DIRACX_SANDBOX_STORE_BUCKET_NAME",
    "DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS",
    "DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET",
    "DIRACX_OTEL_ENABLED",
    "DIRACX_OTEL_GRPC_ENDPOINT",
    "DIRACX_OTEL_GRPC_INSECURE",
    "DIRACX_SERVICE_AUTH_ACCESS_TOKEN_EXPIRE_MINUTES",
    "DIRACX_SERVICE_AUTH_REFRESH_TOKEN_EXPIRE_MINUTES",
    "DIRACX_TASKS_DUMMY_JOB_EXECUTOR_ENABLED",
    "DIRACX_TASKS_DUMMY_JOB_EXECUTOR_INTERVAL_SECONDS",
)
SQL_DBS = (
    "AuthDB",
    "JobDB",
    "JobLoggingDB",
    "SandboxMetadataDB",
    "TaskQueueDB",
    "TaskDB",
    "PilotAgentsDB",
    "ResourceStatusDB",
)
OS_DBS = ("JobParametersDB", "PilotLogsDB")


def chart_fixture(tmp_path: Path, names: tuple[str, ...] = DEMO_NAMES) -> Path:
    charts = tmp_path / "charts"
    (charts / "demo").mkdir(parents=True)
    templates = charts / "diracx/templates/diracx"
    (templates / "init-secrets").mkdir(parents=True)
    (charts / "diracx/values.yaml").write_text(
        "diracx:\n  settings:\n    DIRACX_SERVICE_AUTH_TOKEN_KEYSTORE: " + CANARY + "\n"
    )
    (charts / "demo/ci_values.yaml").write_text(
        "diracx:\n  settings:\n    DIRACX_DEV_CRASH_ON_MISSED_ACCESS_POLICY: "
        + CANARY
        + "\n"
    )
    (charts / "demo/values.tpl.yaml").write_text(
        "diracx:\n  settings:\n"
        + "".join(f"    {name}: {CANARY}\n" for name in names)
        + "  sqlDbs:\n    dbs:\n"
        + "".join(f"      {name}:\n" for name in SQL_DBS)
        + "  osDbs:\n    dbs:\n"
        + "".join(f"      {name}:\n" for name in OS_DBS)
    )
    (templates / "init-secrets/_init-secrets.sh.tpl").write_text(
        "DIRACX_SERVICE_AUTH_STATE_KEY=" + CANARY + "\n"
        "DIRACX_DB_URL_{{ $dbName | upper }}=" + CANARY + "\n"
        "DIRACX_OS_DB_{{ $osDbName | upper }}=" + CANARY + "\n"
        "DIRACX_TASKS_REDIS_URL=" + CANARY + "\n"
    )
    (templates / "deployment-cli.yaml").write_text("DIRACX_URL:\nDIRACX_CA_PATH:\n")
    return charts


def build(
    charts: Path, entries=release_env.POLICY_ENTRIES, charts_commit: str = COMMIT
) -> bytes:
    return release_env.build_env(ROOT, charts, "v1.2.3", COMMIT, charts_commit, entries)


def test_expected_settings_and_dynamic_families_are_emitted(tmp_path: Path) -> None:
    values = release_env.parse_dotenv(build(chart_fixture(tmp_path)))
    assert set(DEMO_NAMES) <= values.keys()
    assert values["DIRACX_SANDBOX_STORE_BUCKET_NAME"] == "demo-sandboxes"
    assert {f"DIRACX_DB_URL_{name.upper()}" for name in SQL_DBS} <= values.keys()
    assert {f"DIRACX_OS_DB_{name.upper()}" for name in OS_DBS} <= values.keys()
    assert "DIRACX_TASKS_REDIS_URL" in values
    assert "DIRACX_SERVICE_AUTH_STATE_KEY" in values


def test_sensitive_source_values_and_process_canary_never_flow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DIRACX_SERVICE_AUTH_STATE_KEY", CANARY)
    data = build(chart_fixture(tmp_path))
    values = release_env.parse_dotenv(data)
    assert CANARY.encode() not in data
    assert values["DIRACX_SERVICE_AUTH_STATE_KEY"] == "REPLACE_WITH_FERNET_KEY"
    assert (
        "REPLACE_WITH_SECRET_ACCESS_KEY"
        in values["DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS"]
    )
    assert "PASSWORD@DB_HOST" in values["DIRACX_DB_URL_AUTHDB"]
    assert "PASSWORD@OPENSEARCH_HOST" in values["DIRACX_OS_DB_JOBPARAMETERSDB"]


def test_unclassified_new_chart_name_fails(tmp_path: Path) -> None:
    charts = chart_fixture(
        tmp_path, DEMO_NAMES + ("DIRACX_SERVICE_AUTH_DIRAC_CLIENT_ID",)
    )
    with pytest.raises(ValueError, match="unclassified=.*DIRAC_CLIENT_ID"):
        build(charts)


def test_stale_policy_entry_fails(tmp_path: Path) -> None:
    charts = chart_fixture(tmp_path, DEMO_NAMES[1:])
    with pytest.raises(ValueError, match="stale=.*TOKEN_ISSUER"):
        build(charts)


def test_schema_and_chart_mismatch_fails(tmp_path: Path) -> None:
    charts = chart_fixture(tmp_path, DEMO_NAMES + ("DIRACX_UNKNOWN_NOT_IN_SCHEMA",))
    with pytest.raises(ValueError, match="absent from DIRACX schema"):
        build(charts)


def test_duplicate_or_conflicting_classification_fails(tmp_path: Path) -> None:
    charts = chart_fixture(tmp_path)
    duplicate = release_env.POLICY_ENTRIES + (
        ("DIRACX_TASKS_REDIS_URL", release_env.Rule("safe_literal", "bad")),
    )
    with pytest.raises(ValueError, match="duplicate"):
        build(charts, duplicate)


def test_deterministic_sorted_parseable_bytes(tmp_path: Path) -> None:
    charts = chart_fixture(tmp_path)
    first = build(charts)
    second = build(charts)
    assert first == second
    settings_file = charts / "demo/values.tpl.yaml"
    settings_file.write_text(
        settings_file.read_text()
        .replace("    DIRACX_SERVICE_AUTH_TOKEN_ISSUER: " + CANARY + "\n", "")
        .replace(
            "  sqlDbs:",
            "    DIRACX_SERVICE_AUTH_TOKEN_ISSUER: " + CANARY + "\n  sqlDbs:",
        )
    )
    assert build(charts) == first
    parsed = release_env.parse_dotenv(first)
    assert list(parsed) == sorted(parsed)
    assert len(parsed) == len(release_env.POLICY_ENTRIES)


def test_malformed_dotenv_rejected() -> None:
    with pytest.raises(ValueError, match="malformed"):
        release_env.parse_dotenv(b'DIRACX_TEST="unterminated\n')


def test_dynamic_template_drift_fails(tmp_path: Path) -> None:
    charts = chart_fixture(tmp_path)
    script = charts / "diracx/templates/diracx/init-secrets/_init-secrets.sh.tpl"
    script.write_text(script.read_text().replace("$osDbName", "$newDbName"))
    with pytest.raises(ValueError, match="dynamic name contract changed"):
        build(charts)


@pytest.mark.parametrize("charts_commit", ["b" * 39, "b" * 41, "B" * 40, "g" * 40])
def test_malformed_charts_commit_fails(tmp_path: Path, charts_commit: str) -> None:
    charts = chart_fixture(tmp_path)
    with pytest.raises(ValueError, match="invalid charts commit"):
        build(charts, charts_commit=charts_commit)


def test_supplied_charts_commit_is_recorded(tmp_path: Path) -> None:
    charts = chart_fixture(tmp_path)
    charts_commit = "b" * 40
    data = build(charts, charts_commit=charts_commit)
    assert f"# diracx-charts source: {charts_commit}\n".encode() in data
    assert f"# diracx-charts source: {COMMIT}\n".encode() not in data
