import os

from diracx.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_lollygag_cli():
    """
    Makes sure that lollygag is available in the dirac cli
    """
    result = runner.invoke(app, ["--help"], env=os.environ)
    assert result.exit_code == 0, result.output
    assert "lollygag" in result.output, result.output


def test_lollygag_hello():
    """
    We can call a trivial lollygag function
    """
    result = runner.invoke(app, ["lollygag", "hello"], env=os.environ)
    assert result.exit_code == 0, result.output
    assert "Baby" in result.output, result.output


def test_lollygag_owner(with_cli_login):
    """
    We can call a more complex function, involving the client and
    calling the router
    """
    result = runner.invoke(app, ["lollygag", "get-owners"], env=os.environ)
    assert result.exit_code == 0, result.output
    assert "[]" in result.output, result.output


def test_lollygag_sensei(with_cli_login):
    """
    We expect to not be able to call that method because our token
    does not have the correct property
    """
    result = runner.invoke(app, ["lollygag", "sensei"], env=os.environ)
    assert result.exit_code != 0, result.output
    assert result.exception.status_code == 403


def test_config_extra():
    """
    Test adding an extra function
    """
    result = runner.invoke(app, ["config", "gubbins-extra"], env=os.environ)
    assert result.exit_code == 0, result.output
    assert "Adding something extra" in result.output, result.output


def test_config_overwrite():
    """
    Test overwriting a function
    """
    result = runner.invoke(app, ["config", "dump"], env=os.environ)
    assert result.exit_code == 0, result.output
    assert "I replaced the original dump" in result.output, result.output
