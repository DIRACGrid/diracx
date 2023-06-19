from __future__ import annotations

import pytest
from typer.testing import CliRunner

from diracx.cli import app

runner = CliRunner()


@pytest.mark.parametrize("protocol", [None, "file://"])
def test_generate_cs(tmp_path, protocol):
    cs_repo = f"{tmp_path}"
    if protocol is None:
        cs_repo = f"file://{cs_repo}"

    result = runner.invoke(app, ["internal", "generate-cs", str(cs_repo)])
    assert result.exit_code == 0
    assert (tmp_path / ".git").is_dir()
    assert (tmp_path / "default.yml").is_file()

    # Running a second time should fail
    result = runner.invoke(app, ["internal", "generate-cs", str(cs_repo)])
    assert result.exit_code != 0
