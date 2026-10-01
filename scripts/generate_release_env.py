#!/usr/bin/env python3
"""Build the public release .env example from reviewed source contracts.

Only names are read from charts and settings source. Output values come solely
from the explicit policy below; neither chart values nor runtime values flow to
the artifact. New chart names require a reviewed policy update.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path

NAME_RE = re.compile(r"DIRACX_[A-Z0-9_]+\Z")
COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
TAG_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
YAML_KEY_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_]*):(?:\s|$)")
DOTENV_LINE_RE = re.compile(r'^(DIRACX_[A-Z0-9_]+)=("(?:[^"\\]|\\.)*")$')


@dataclass(frozen=True)
class Rule:
    classification: str
    example: str


# Every current chart-derived name has one reviewed output rule. A new DB or
# setting name fails until a rule is deliberately added here.
POLICY_ENTRIES = (
    (
        "DIRACX_CONFIG_BACKEND_URL",
        Rule("url_placeholder", "git+file:///path/to/config-repo"),
    ),
    ("DIRACX_DEV_CRASH_ON_MISSED_ACCESS_POLICY", Rule("safe_literal", "true")),
    ("DIRACX_OTEL_ENABLED", Rule("safe_literal", "false")),
    ("DIRACX_OTEL_GRPC_ENDPOINT", Rule("safe_synthetic", "otel.example.invalid:4317")),
    ("DIRACX_OTEL_GRPC_INSECURE", Rule("safe_literal", "true")),
    ("DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET", Rule("safe_literal", "true")),
    ("DIRACX_SANDBOX_STORE_BUCKET_NAME", Rule("safe_synthetic", "demo-sandboxes")),
    (
        "DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS",
        Rule(
            "json_placeholder",
            '{"endpoint_url":"https://s3.example.invalid",'
            '"aws_access_key_id":"REPLACE_WITH_ACCESS_KEY_ID",'
            '"aws_secret_access_key":"REPLACE_WITH_SECRET_ACCESS_KEY"}',
        ),
    ),
    ("DIRACX_SERVICE_AUTH_ACCESS_TOKEN_EXPIRE_MINUTES", Rule("safe_literal", "120")),
    (
        "DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS",
        Rule(
            "json_placeholder",
            '["https://diracx.example.invalid:8000/api/docs/oauth2-redirect"]',
        ),
    ),
    ("DIRACX_SERVICE_AUTH_REFRESH_TOKEN_EXPIRE_MINUTES", Rule("safe_literal", "360")),
    (
        "DIRACX_SERVICE_AUTH_STATE_KEY",
        Rule("credential_placeholder", "REPLACE_WITH_FERNET_KEY"),
    ),
    (
        "DIRACX_SERVICE_AUTH_TOKEN_ISSUER",
        Rule("url_placeholder", "https://diracx.example.invalid:8000"),
    ),
    (
        "DIRACX_SERVICE_AUTH_TOKEN_KEYSTORE",
        Rule("structural_placeholder", "file:///path/to/jwks.json"),
    ),
    ("DIRACX_TASKS_DUMMY_JOB_EXECUTOR_ENABLED", Rule("safe_literal", "true")),
    ("DIRACX_TASKS_DUMMY_JOB_EXECUTOR_INTERVAL_SECONDS", Rule("safe_literal", "10")),
    (
        "DIRACX_TASKS_REDIS_URL",
        Rule("url_placeholder", "redis://redis.example.invalid:6379"),
    ),
    (
        "DIRACX_DB_URL_AUTHDB",
        Rule("url_placeholder", "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/AuthDB"),
    ),
    (
        "DIRACX_DB_URL_JOBDB",
        Rule("url_placeholder", "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/JobDB"),
    ),
    (
        "DIRACX_DB_URL_JOBLOGGINGDB",
        Rule(
            "url_placeholder",
            "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/JobLoggingDB",
        ),
    ),
    (
        "DIRACX_DB_URL_SANDBOXMETADATADB",
        Rule(
            "url_placeholder",
            "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/SandboxMetadataDB",
        ),
    ),
    (
        "DIRACX_DB_URL_TASKQUEUEDB",
        Rule(
            "url_placeholder", "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/TaskQueueDB"
        ),
    ),
    (
        "DIRACX_DB_URL_TASKDB",
        Rule("url_placeholder", "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/TaskDB"),
    ),
    (
        "DIRACX_DB_URL_PILOTAGENTSDB",
        Rule(
            "url_placeholder",
            "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/PilotAgentsDB",
        ),
    ),
    (
        "DIRACX_DB_URL_RESOURCESTATUSDB",
        Rule(
            "url_placeholder",
            "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/ResourceStatusDB",
        ),
    ),
    (
        "DIRACX_OS_DB_JOBPARAMETERSDB",
        Rule(
            "json_placeholder",
            '{"hosts":"USER:PASSWORD@OPENSEARCH_HOST:9200","use_ssl":true,"verify_certs":true}',
        ),
    ),
    (
        "DIRACX_OS_DB_PILOTLOGSDB",
        Rule(
            "json_placeholder",
            '{"hosts":"USER:PASSWORD@OPENSEARCH_HOST:9200","use_ssl":true,"verify_certs":true}',
        ),
    ),
)

CLASS_SOURCES = {
    "DevelopmentSettings": "diracx-core/src/diracx/core/settings.py",
    "AuthSettings": "diracx-core/src/diracx/core/settings.py",
    "SandboxStoreSettings": "diracx-core/src/diracx/core/settings.py",
    "FactorySettings": "diracx-core/src/diracx/core/settings.py",
    "OTELSettings": "diracx-routers/src/diracx/routers/otel.py",
    "DummyJobExecutorSettings": "diracx-tasks/src/diracx/tasks/jobs/dummy_job_executor.py",
}


def policy_map(entries: tuple[tuple[str, Rule], ...]) -> dict[str, Rule]:
    policy: dict[str, Rule] = {}
    for name, rule in entries:
        if not NAME_RE.fullmatch(name) or name in policy:
            raise ValueError(f"duplicate or malformed policy name: {name}")
        if rule.classification not in {
            "safe_literal",
            "safe_synthetic",
            "structural_placeholder",
            "credential_placeholder",
            "url_placeholder",
            "json_placeholder",
        }:
            raise ValueError(f"unknown classification for {name}")
        if not rule.example or any(char in rule.example for char in "\r\n\x00"):
            raise ValueError(f"unsafe example format for {name}")
        if rule.classification == "json_placeholder":
            json.loads(rule.example)
        policy[name] = rule
    return policy


def yaml_keys(path: Path, parent: tuple[str, ...]) -> set[str]:
    """Read key names at a known YAML path, never its values or Jinja tokens."""
    stack: list[tuple[int, str]] = []
    found: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.lstrip(" ")
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(stripped)
        while stack and stack[-1][0] >= indent:
            stack.pop()
        match = YAML_KEY_RE.match(stripped)
        if tuple(item[1] for item in stack) == parent and not match:
            raise ValueError(f"cannot parse chart key at {'.'.join(parent)} in {path}")
        if not match:
            continue
        key = match.group(1)
        if tuple(item[1] for item in stack) == parent:
            if key in found:
                raise ValueError(f"duplicate chart key {key} in {path}")
            found.add(key)
        stack.append((indent, key))
    if not found:
        raise ValueError(f"missing chart keys at {'.'.join(parent)} in {path}")
    return found


def schema_names(source_root: Path) -> tuple[set[str], set[str]]:
    names: set[str] = set()
    dynamic_prefixes: set[str] = set()
    parsed = {
        path: ast.parse((source_root / path).read_text(encoding="utf-8"))
        for path in set(CLASS_SOURCES.values())
    }
    for class_name, path in CLASS_SOURCES.items():
        classes = [
            node
            for node in parsed[path].body
            if isinstance(node, ast.ClassDef) and node.name == class_name
        ]
        if len(classes) != 1:
            raise ValueError(f"missing or duplicate settings class {class_name}")
        cls = classes[0]
        prefixes = [
            node.value
            for node in ast.walk(cls)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.startswith("DIRACX_")
            and node.value.endswith("_")
        ]
        if class_name != "FactorySettings":
            prefix_candidates = [
                value
                for value in prefixes
                if value not in {"DIRACX_DB_URL_", "DIRACX_OS_DB_"}
            ]
            if len(prefix_candidates) != 1:
                raise ValueError(
                    f"ambiguous env prefix in {class_name}: {prefix_candidates}"
                )
            prefix = prefix_candidates[0]
            names.update(
                prefix + node.target.id.upper()
                for node in cls.body
                if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
            )
        else:
            for node in cls.body:
                if not isinstance(node, ast.AnnAssign) or not isinstance(
                    node.value, ast.Call
                ):
                    continue
                for keyword in node.value.keywords:
                    if keyword.arg != "validation_alias":
                        continue
                    if not isinstance(keyword.value, ast.Constant) or not isinstance(
                        keyword.value.value, str
                    ):
                        raise ValueError(
                            "FactorySettings validation_alias is no longer a string literal"
                        )
                    names.add(keyword.value.value)
            dynamic_prefixes = {
                part.value
                for node in ast.walk(cls)
                if isinstance(node, ast.JoinedStr)
                for part in node.values[:1]
                if isinstance(part, ast.Constant)
                and isinstance(part.value, str)
                and part.value.startswith("DIRACX_")
            }
    if dynamic_prefixes != {"DIRACX_SERVICE_", "DIRACX_OS_DB_", "DIRACX_DB_URL_"}:
        raise ValueError(f"DIRACX dynamic settings aliases changed: {dynamic_prefixes}")
    return names, dynamic_prefixes


def source_inventory(source_root: Path, charts_root: Path) -> set[str]:
    demo = charts_root / "demo/values.tpl.yaml"
    defaults = charts_root / "diracx/values.yaml"
    ci = charts_root / "demo/ci_values.yaml"
    settings = set().union(
        *(yaml_keys(path, ("diracx", "settings")) for path in (demo, defaults, ci))
    )
    if any(not NAME_RE.fullmatch(name) for name in settings):
        raise ValueError("malformed chart DIRACX setting name")
    sql_dbs = yaml_keys(demo, ("diracx", "sqlDbs", "dbs"))
    os_dbs = yaml_keys(demo, ("diracx", "osDbs", "dbs"))
    if any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*DB", db) for db in sql_dbs | os_dbs):
        raise ValueError("unrecognized chart database name")
    template_root = charts_root / "diracx/templates/diracx"
    template_names = set().union(
        *(
            set(re.findall(r"DIRACX_[A-Z0-9_]+", path.read_text(encoding="utf-8")))
            for path in template_root.rglob("*")
            if path.is_file()
        )
    )
    if template_names != {
        "DIRACX_CA_PATH",
        "DIRACX_URL",
        "DIRACX_DB_URL_",
        "DIRACX_OS_DB_",
        "DIRACX_SERVICE_AUTH_STATE_KEY",
        "DIRACX_TASKS_REDIS_URL",
    }:
        raise ValueError(
            f"chart template env-name construction changed: {sorted(template_names)}"
        )
    init_script = (template_root / "init-secrets/_init-secrets.sh.tpl").read_text(
        encoding="utf-8"
    )
    for marker in (
        "DIRACX_DB_URL_{{ $dbName | upper }}",
        "DIRACX_OS_DB_{{ $osDbName | upper }}",
    ):
        prefix = marker.split("{{", 1)[0]
        if marker not in init_script or init_script.count(prefix) != init_script.count(
            marker
        ):
            raise ValueError(f"chart dynamic name contract changed: {marker}")
    recognized, dynamic = schema_names(source_root)
    if not settings <= recognized:
        raise ValueError(
            f"chart settings absent from DIRACX schema: {sorted(settings - recognized)}"
        )
    if not {"DIRACX_DB_URL_", "DIRACX_OS_DB_"} <= dynamic:
        raise ValueError("DIRACX database alias construction changed")
    if not {"DIRACX_SERVICE_AUTH_STATE_KEY", "DIRACX_TASKS_REDIS_URL"} <= recognized:
        raise ValueError("DIRACX dynamic secret aliases changed")
    return (
        settings
        | {"DIRACX_SERVICE_AUTH_STATE_KEY", "DIRACX_TASKS_REDIS_URL"}
        | {f"DIRACX_DB_URL_{db.upper()}" for db in sql_dbs}
        | {f"DIRACX_OS_DB_{db.upper()}" for db in os_dbs}
    )


def parse_dotenv(data: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in data.decode("utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        match = DOTENV_LINE_RE.fullmatch(line)
        if not match or match.group(1) in result:
            raise ValueError(f"malformed or duplicate dotenv line: {line[:80]}")
        value = json.loads(match.group(2))
        if not isinstance(value, str):
            raise ValueError("non-string dotenv value")
        result[match.group(1)] = value
    return result


def build_env(
    source_root: Path,
    charts_root: Path,
    release_tag: str,
    source_commit: str,
    charts_commit: str,
    entries: tuple[tuple[str, Rule], ...] = POLICY_ENTRIES,
) -> bytes:
    if not TAG_RE.fullmatch(release_tag) or not COMMIT_RE.fullmatch(source_commit):
        raise ValueError("invalid release tag or DIRACX commit")
    if not COMMIT_RE.fullmatch(charts_commit):
        raise ValueError("invalid charts commit")
    expected = source_inventory(source_root, charts_root)
    policy = policy_map(entries)
    if expected != policy.keys():
        unclassified = sorted(expected - policy.keys())
        stale = sorted(policy.keys() - expected)
        raise ValueError(
            f"release env policy/source drift: unclassified={unclassified}; stale={stale}"
        )
    lines = [
        "# DIRACX public container configuration example; save as .env and replace placeholders.",
        "# This is a template, not a runtime environment dump.",
        f"# DIRACX release: {release_tag}",
        f"# DIRACX source: {source_commit}",
        f"# diracx-charts source: {charts_commit}",
    ]
    for name in sorted(expected):
        rule = policy[name]
        lines.append(f"# {rule.classification}")
        lines.append(f"{name}={json.dumps(rule.example, ensure_ascii=True)}")
    data = ("\n".join(lines) + "\n").encode("utf-8")
    if parse_dotenv(data) != {name: policy[name].example for name in expected}:
        raise ValueError("dotenv serialization did not round-trip")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--charts-root", type=Path)
    parser.add_argument("--release-tag")
    parser.add_argument("--source-commit")
    parser.add_argument("--charts-commit")
    parser.add_argument("--output", type=Path, default=Path(".env"))
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    if not all(
        (args.charts_root, args.release_tag, args.source_commit, args.charts_commit)
    ):
        parser.error(
            "--charts-root, --release-tag, --source-commit, and --charts-commit are required"
        )
    source_root = Path(__file__).resolve().parent.parent
    data = build_env(
        source_root,
        args.charts_root,
        args.release_tag,
        args.source_commit,
        args.charts_commit,
    )
    if args.validate:
        if args.output.read_bytes() != data:
            raise ValueError(
                "existing .env differs from validated source-derived bytes"
            )
    else:
        args.output.write_bytes(data)


if __name__ == "__main__":
    main()
