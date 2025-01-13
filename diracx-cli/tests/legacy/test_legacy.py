from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from diracx.cli import app

runner = CliRunner()

file_path = Path(__file__).parent


def test_generate_helm_values(tmp_path, monkeypatch):
    output_file = tmp_path / "values.yaml"

    result = runner.invoke(
        app,
        [
            "internal",
            "legacy",
            "generate-helm-values",
            "--public-cfg",
            str(file_path / "cs_sync" / "integration_test.cfg"),
            "--secret-cfg",
            str(file_path / "cs_sync" / "integration_test_secret.cfg"),
            "--output-file",
            str(output_file),
        ],
    )
    assert result.exit_code == 0
    assert output_file.is_file()

    assert isinstance(yaml.safe_load(output_file.read_text()), dict)
