from __future__ import annotations

from typer.testing import CliRunner

from diracx.cli import app

runner = CliRunner()


def test_generate_cs(tmp_path):
    cs_repo = tmp_path / "cs-repo"
    result = runner.invoke(app, ["internal", "generate-cs", str(cs_repo)])
    assert result.exit_code == 0
    assert (cs_repo / ".git").is_dir()
    assert (cs_repo / "default.yml").is_file()

    # Running a second time should fail
    result = runner.invoke(app, ["internal", "generate-cs", str(cs_repo)])
    assert result.exit_code != 0
    assert "already exists" in result.stdout
