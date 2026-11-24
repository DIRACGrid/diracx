#!/usr/bin/env python3
"""Build the public `diracx.env` example for installations without Kubernetes.

Only names are read from the DiracX sources (the ServiceSettingsBase
subclasses of the diracx-* packages, and the database entry points). Output
values come solely from the explicit policy below. A new database, a new setting
with no default, or a policy name the sources no longer know, fails until the
policy is updated.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TypeGuard

NAME_RE = re.compile(r"DIRACX_[A-Z0-9_]+\Z")
DB_PREFIXES = ("DIRACX_DB_URL_", "DIRACX_OS_DB_")
EXAMPLE_PATH = Path("docs/admin/how-to/install-without-kubernetes/diracx.env.example")
# Values are written unquoted: podman's --env-file (used by podman-compose
# up to 1.1.0) keeps quotes, while python-dotenv (podman-compose >= 1.2.0) and
# docker compose read an unquoted value as is. They all see the same string as
# long as it has no "$", which docker compose and python-dotenv expand.
DOTENV_LINE_RE = re.compile(r"^(DIRACX_[A-Z0-9_]+)=(.+)$")


@dataclass(frozen=True)
class Rule:
    classification: str
    example: str


# Every name in the example has one reviewed output rule. A new database or a
# new setting with no default fails until a rule is deliberately added here.
# Settings with a default may be left out (development-only ones must be).
POLICY_ENTRIES = (
    (
        "DIRACX_CONFIG_BACKEND_URL",
        Rule("url_placeholder", "git+file:///path/to/config-repo"),
    ),
    ("DIRACX_LOG_FORMAT", Rule("safe_literal", "text")),
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
    (
        "DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY",
        Rule("credential_placeholder", "REPLACE_WITH_SHA256_OF_LEGACY_API_KEY"),
    ),
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
        "DIRACX_DB_URL_TRANSFORMATIONDB",
        Rule(
            "url_placeholder",
            "mysql+aiomysql://USER:PASSWORD@DB_HOST:3306/TransformationDB",
        ),
    ),
    (
        "DIRACX_OS_DB_JOBPARAMETERSDB",
        Rule(
            "json_placeholder",
            '{"hosts":"USER:PASSWORD@OPENSEARCH_HOST:9200","use_ssl":true,"verify_certs":true}',
        ),
    ),
)

SETTINGS_BASE = "ServiceSettingsBase"
# Development-only settings are not public: a rule for one is an unknown setting.
DEVELOPMENT_CLASS = "DevelopmentSettings"
SOURCE_GLOB = "diracx-*/src/**/*.py"


def base_names(node: ast.ClassDef) -> set[str]:
    return {
        base.id if isinstance(base, ast.Name) else getattr(base, "attr", "")
        for base in node.bases
    }


def settings_classes(source_root: Path) -> dict[str, ast.ClassDef]:
    """Return the direct ServiceSettingsBase subclasses of the DiracX packages."""
    classes: dict[str, ast.ClassDef] = {}
    modules = [
        ast.parse(path.read_text(encoding="utf-8"))
        for path in sorted(source_root.glob(SOURCE_GLOB))
    ]
    for module in modules:
        for node in module.body:
            if not isinstance(node, ast.ClassDef) or SETTINGS_BASE not in base_names(
                node
            ):
                continue
            if node.name in classes:
                raise ValueError(f"duplicate settings class {node.name}")
            classes[node.name] = node
    if "FactorySettings" not in classes:
        raise ValueError("FactorySettings not found")
    # A subclass of a settings class inherits fields the scan does not model.
    for module in modules:
        for node in ast.walk(module):
            if isinstance(node, ast.ClassDef) and base_names(node) & classes.keys():
                raise ValueError(f"unsupported indirect settings class {node.name}")
    return classes


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
        if (
            not rule.example
            or not rule.example.isascii()
            or any(char in rule.example for char in "\r\n\x00")
            # dotenv would strip the whitespace, the quotes or a " #" comment
            or rule.example != rule.example.strip()
            or rule.example[0] in "\"'`"
            or " #" in rule.example
            or "$" in rule.example
        ):
            raise ValueError(f"unsafe example format for {name}")
        if rule.classification == "json_placeholder":
            json.loads(rule.example)
        policy[name] = rule
    return policy


def has_default(field: ast.AnnAssign) -> bool:
    """Return whether a settings field can be left out of the environment."""
    value = field.value
    if value is None:
        return False
    if not isinstance(value, ast.Call):
        return True
    func = value.func
    if (
        func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
    ) != "Field":
        return True
    for keyword in value.keywords:
        if keyword.arg == "default_factory":
            return True
        if keyword.arg == "default":
            return not is_ellipsis(keyword.value)
    return bool(value.args) and not is_ellipsis(value.args[0])


def is_field(node: ast.stmt) -> TypeGuard[ast.AnnAssign]:
    """Return whether node declares a pydantic field (not a private attr or ClassVar)."""
    if not isinstance(node, ast.AnnAssign) or not isinstance(node.target, ast.Name):
        return False
    annotation = node.annotation
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        annotation = ast.parse(annotation.value, mode="eval").body
    if isinstance(annotation, ast.Subscript):
        annotation = annotation.value
    is_classvar = (
        annotation.id
        if isinstance(annotation, ast.Name)
        else getattr(annotation, "attr", "")
    ) == "ClassVar"
    return not node.target.id.startswith("_") and not is_classvar


def is_ellipsis(node: ast.expr) -> bool:
    """Return whether node is ``...``, pydantic's marker for a required field."""
    return isinstance(node, ast.Constant) and node.value is Ellipsis


def field_alias(field: ast.AnnAssign) -> str | None:
    """Return the env name a Field alias sets, which replaces the env prefix."""
    if not isinstance(field.value, ast.Call):
        return None
    keywords = {k.arg: k for k in field.value.keywords}
    # pydantic-settings reads validation_alias when both are set.
    for name in ("validation_alias", "alias"):
        keyword = keywords.get(name)
        if keyword is None:
            continue
        if not isinstance(keyword.value, ast.Constant) or not isinstance(
            keyword.value.value, str
        ):
            raise ValueError(
                f"settings alias of {ast.unparse(field.target)} is not a string"
            )
        return keyword.value.value
    return None


def schema_names(source_root: Path) -> tuple[set[str], set[str], set[str]]:
    """Return the settings env names, those with no default, and dynamic prefixes."""
    names: set[str] = set()
    required: set[str] = set()
    dynamic_prefixes: set[str] = set()
    classes = settings_classes(source_root)
    if DEVELOPMENT_CLASS not in classes:
        raise ValueError(f"{DEVELOPMENT_CLASS} not found")
    for class_name, cls in classes.items():
        if class_name == DEVELOPMENT_CLASS:
            continue
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
            for node in cls.body:
                if is_field(node):
                    name = (
                        field_alias(node) or prefix + ast.unparse(node.target).upper()
                    )
                    names.add(name)
                    if not has_default(node):
                        required.add(name)
        else:
            for node in cls.body:
                if not is_field(node) or not isinstance(node.value, ast.Call):
                    continue
                if alias := field_alias(node):
                    names.add(alias)
                    if not has_default(node):
                        required.add(alias)
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
    return names, required, dynamic_prefixes


def database_names(source_root: Path) -> set[str]:
    """Return the env names of the databases registered by diracx-db."""
    pyproject = tomllib.loads(
        (source_root / "diracx-db/pyproject.toml").read_text(encoding="utf-8")
    )
    entry_points = pyproject["project"]["entry-points"]
    return {f"DIRACX_DB_URL_{db.upper()}" for db in entry_points["diracx.dbs.sql"]} | {
        f"DIRACX_OS_DB_{db.upper()}" for db in entry_points["diracx.dbs.os"]
    }


def check_policy(source_root: Path, policy: dict[str, Rule]) -> None:
    """Fail when the policy and the DiracX sources disagree."""
    recognized, required, dynamic = schema_names(source_root)
    if not set(DB_PREFIXES) <= dynamic:
        raise ValueError("DIRACX database alias construction changed")
    databases = database_names(source_root)
    listed = {name for name in policy if name.startswith(DB_PREFIXES)}
    unknown = sorted(policy.keys() - listed - recognized)
    missing = sorted(databases - listed)
    stale = sorted(listed - databases)
    unset = sorted(required - policy.keys())
    if unknown or missing or stale or unset:
        raise ValueError(
            "diracx.env policy/source drift: "
            f"unknown settings={unknown}; databases without a rule={missing}; "
            f"rules for removed databases={stale}; "
            f"settings with no default and no rule={unset}"
        )


def parse_dotenv(data: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in data.decode("utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        match = DOTENV_LINE_RE.fullmatch(line)
        if not match or match.group(1) in result:
            raise ValueError(f"malformed or duplicate dotenv line: {line[:80]}")
        result[match.group(1)] = match.group(2)
    return result


def build_env(
    source_root: Path, entries: tuple[tuple[str, Rule], ...] = POLICY_ENTRIES
) -> bytes:
    policy = policy_map(entries)
    check_policy(source_root, policy)
    lines = [
        "# DiracX container configuration example: save as diracx.env and replace the placeholders.",
        "# Generated by scripts/generate_release_env.py, do not edit by hand.",
    ]
    for name in sorted(policy):
        rule = policy[name]
        lines.append(f"# {rule.classification}")
        lines.append(f"{name}={rule.example}")
    data = ("\n".join(lines) + "\n").encode("utf-8")
    if parse_dotenv(data) != {name: rule.example for name, rule in policy.items()}:
        raise ValueError("dotenv serialization did not round-trip")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--validate",
        action="store_true",
        help="fail if the file differs, instead of writing it",
    )
    args = parser.parse_args()
    source_root = Path(__file__).resolve().parent.parent
    output = args.output or source_root / EXAMPLE_PATH
    data = build_env(source_root)
    if args.validate:
        if output.read_bytes() != data:
            raise ValueError(f"{output} is out of date: run {Path(__file__).name}")
    else:
        output.write_bytes(data)


if __name__ == "__main__":
    main()
