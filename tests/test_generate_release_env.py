"""Checks for the public diracx.env example and its generator."""

from __future__ import annotations

import importlib
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

Rule = release_env.Rule
ENTRIES = release_env.POLICY_ENTRIES


def test_committed_example_is_up_to_date() -> None:
    assert (ROOT / release_env.EXAMPLE_PATH).read_bytes() == release_env.build_env(ROOT)


def test_every_registered_database_is_emitted() -> None:
    emitted = release_env.parse_dotenv(release_env.build_env(ROOT))
    assert release_env.database_names(ROOT) <= emitted.keys()
    assert "DIRACX_SERVICE_AUTH_STATE_KEY" in emitted


def test_output_is_sorted_and_parseable() -> None:
    data = release_env.build_env(ROOT)
    names = list(release_env.parse_dotenv(data))
    assert names == sorted(names)
    assert data == release_env.build_env(ROOT, tuple(reversed(ENTRIES)))


def test_database_without_a_rule_fails() -> None:
    entries = tuple(e for e in ENTRIES if e[0] != "DIRACX_DB_URL_AUTHDB")
    with pytest.raises(ValueError, match="databases without a rule"):
        release_env.build_env(ROOT, entries)


def test_rule_for_a_removed_database_fails() -> None:
    entries = ENTRIES + (
        ("DIRACX_DB_URL_NOSUCHDB", Rule("url_placeholder", "mysql://x")),
    )
    with pytest.raises(ValueError, match="rules for removed databases"):
        release_env.build_env(ROOT, entries)


def test_setting_unknown_to_the_schema_fails() -> None:
    entries = ENTRIES + (("DIRACX_NOT_A_SETTING", Rule("safe_literal", "x")),)
    with pytest.raises(ValueError, match="unknown settings"):
        release_env.build_env(ROOT, entries)


def test_setting_with_no_default_and_no_rule_fails() -> None:
    entries = tuple(e for e in ENTRIES if e[0] != "DIRACX_SERVICE_AUTH_TOKEN_ISSUER")
    with pytest.raises(ValueError, match="no default and no rule"):
        release_env.build_env(ROOT, entries)


def copy_sources(tmp_path: Path, addition: str, after: str | None = None) -> Path:
    """Copy the settings and DB sources, adding text after a unique line or at the end."""
    settings = "diracx-core/src/diracx/core/settings.py"
    for path in (settings, "diracx-db/pyproject.toml"):
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_bytes((ROOT / path).read_bytes())
    source = (tmp_path / settings).read_text(encoding="utf-8")
    if after is None:
        source += addition
    else:
        assert source.count(after) == 1
        source = source.replace(after, after + addition)
    (tmp_path / settings).write_text(source, encoding="utf-8")
    return tmp_path


def test_field_with_ellipsis_default_is_required(tmp_path: Path) -> None:
    root = copy_sources(
        tmp_path,
        '    new_setting: str = Field(default=..., description="x")\n',
        "    token_issuer: str\n",
    )
    with pytest.raises(ValueError, match="DIRACX_SERVICE_AUTH_NEW_SETTING"):
        release_env.build_env(root)


def test_alias_replaces_the_env_prefix(tmp_path: Path) -> None:
    root = copy_sources(
        tmp_path,
        '    new_setting: str = Field(validation_alias="DIRACX_NEW_SETTING")\n',
        "    token_issuer: str\n",
    )
    with pytest.raises(ValueError, match=r"no rule=\['DIRACX_NEW_SETTING'\]"):
        release_env.build_env(root)


def test_indirect_settings_class_fails(tmp_path: Path) -> None:
    root = copy_sources(tmp_path, "\n\nclass ChildSettings(AuthSettings):\n    pass\n")
    with pytest.raises(ValueError, match="indirect settings class ChildSettings"):
        release_env.build_env(root)


def test_duplicate_classification_fails() -> None:
    with pytest.raises(ValueError, match="duplicate or malformed"):
        release_env.build_env(ROOT, ENTRIES + ENTRIES[:1])


def test_malformed_dotenv_rejected() -> None:
    with pytest.raises(ValueError, match="malformed or duplicate"):
        release_env.parse_dotenv(b"DIRACX_X=\n")


@pytest.mark.parametrize(
    "example", ['"quoted"', " padded", "a #comment", "\u00b5s", "a$b"]
)
def test_example_unsafe_unquoted_fails(example: str) -> None:
    entries = ENTRIES + (("DIRACX_OTEL_HEADERS", Rule("safe_literal", example)),)
    with pytest.raises(ValueError, match="unsafe example format"):
        release_env.build_env(ROOT, entries)


def test_required_settings_match_pydantic() -> None:
    """The AST scan must find every setting pydantic itself requires."""
    from diracx.core.settings import ServiceSettingsBase

    # Import every module defining a settings class, so its subclass exists.
    for path in ROOT.glob(release_env.SOURCE_GLOB):
        if release_env.SETTINGS_BASE in path.read_text(encoding="utf-8"):
            src = next(p for p in path.parents if p.name == "src")
            importlib.import_module(
                ".".join(path.relative_to(src).with_suffix("").parts)
            )

    def subclasses(
        cls: type[ServiceSettingsBase],
    ) -> list[type[ServiceSettingsBase]]:
        return [c for sub in cls.__subclasses__() for c in (sub, *subclasses(sub))]

    required = set()
    for cls in subclasses(ServiceSettingsBase):
        prefix = cls.model_config.get("env_prefix", "")
        for name, field in cls.model_fields.items():
            if field.is_required():
                alias = field.validation_alias
                required.add(
                    alias if isinstance(alias, str) else f"{prefix}{name}".upper()
                )
    _, ast_required, _ = release_env.schema_names(ROOT)
    assert required == ast_required
