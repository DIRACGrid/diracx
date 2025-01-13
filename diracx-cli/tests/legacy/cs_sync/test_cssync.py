from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from diracx.cli import app
from diracx.core.config.schema import Config

runner = CliRunner()

file_path = Path(__file__).parent


def test_cs_sync(tmp_path, monkeypatch):
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "Yes")

    output_file = tmp_path / "default.yml"

    result = runner.invoke(
        app,
        [
            "internal",
            "legacy",
            "cs-sync",
            f"{file_path / 'integration_test.cfg'}",
            str(output_file),
        ],
    )
    assert result.exit_code == 0
    assert output_file.is_file()
    actual_output = yaml.safe_load(output_file.read_text())
    expected_output = yaml.safe_load((file_path / "integration_test.yaml").read_text())
    assert actual_output == expected_output
    Config.model_validate(actual_output)


def test_disabled_vos_empty(tmp_path, monkeypatch):
    # # DisabledVOs cannot be set if any Legacy clients are enabled
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "Yes")

    output_file = tmp_path / "default.yml"

    result = runner.invoke(
        app,
        [
            "internal",
            "legacy",
            "cs-sync",
            f"{file_path / 'integration_test_buggy.cfg'}",
            str(output_file),
        ],
    )
    assert result.exit_code == 1
    assert not output_file.is_file()
    assert isinstance(result.exception, RuntimeError)
    assert "DisabledVOs cannot be set" in str(result.exception)
