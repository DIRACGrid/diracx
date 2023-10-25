from pathlib import Path

import yaml
from typer.testing import CliRunner

from diracx.cli import app
from diracx.core.config.schema import Config

runner = CliRunner()

file_path = Path(__file__).parent


def test_cs_sync(tmp_path, monkeypatch):
    monkeypatch.setenv("DIRAC_COMPAT_ENABLE_CS_CONVERSION", "Yes")

    output_file = tmp_path / "default.yaml"

    result = runner.invoke(
        app,
        [
            "internal",
            "legacy",
            "cs-sync",
            f"{file_path / 'integration_test.cfg'}",
            f"{file_path / 'convert_integration_test.yaml'}",
            str(output_file),
        ],
    )
    assert result.exit_code == 0
    assert output_file.is_file()
    actual_output = yaml.safe_load(output_file.read_text())
    expected_output = yaml.safe_load((file_path / "integration_test.yaml").read_text())
    assert actual_output == expected_output
    Config.parse_obj(actual_output)
