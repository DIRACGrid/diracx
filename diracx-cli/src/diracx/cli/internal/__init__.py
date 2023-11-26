from pathlib import Path

import git
import typer
import yaml
from pydantic import parse_obj_as

from diracx.core.config import ConfigSource, ConfigSourceUrl
from diracx.core.config.schema import (
    Config,
    DIRACConfig,
    GroupConfig,
    IdpConfig,
    OperationsConfig,
    RegistryConfig,
    UserConfig,
)

from ..utils import AsyncTyper
from . import legacy

app = AsyncTyper()
app.add_typer(legacy.app, name="legacy")


@app.command()
def generate_cs(
    config_repo: str,
    *,
    vo: str = "testvo",
    user_group: str = "user",
    idp_url: str = "https://idp.invalid",
    idp_client_id: str = "idp-client-id",
):
    """Generate a minimal DiracX configuration repository"""
    # TODO: The use of parse_obj_as should be moved in to typer itself
    config_repo = parse_obj_as(ConfigSourceUrl, config_repo)
    if config_repo.scheme != "git+file":
        raise NotImplementedError("Only git+file:// URLs are supported")
    repo_path = Path(config_repo.path)
    if repo_path.exists() and list(repo_path.iterdir()):
        typer.echo(f"ERROR: Directory {repo_path} already exists", err=True)
        raise typer.Exit(1)

    registry = RegistryConfig(
        IdP=IdpConfig(URL=idp_url, ClientID=idp_client_id),
        DefaultGroup=user_group,
        Users={},
        Groups={
            user_group: GroupConfig(Properties={"NormalUser"}, Quota=None, Users=set())
        },
    )
    config = Config(
        Registry={vo: registry},
        DIRAC=DIRACConfig(),
        Operations={"Defaults": OperationsConfig()},
    )

    repo = git.Repo.init(repo_path, initial_branch="master")
    yaml_path = repo_path / "default.yml"
    typer.echo(f"Writing configuration to {yaml_path}", err=True)
    yaml_path.write_text(yaml.safe_dump(config.dict(exclude_unset=True)))
    repo.index.add([yaml_path.relative_to(repo_path)])
    repo.index.commit("Initial commit")
    typer.echo(f"Successfully created repo in {config_repo}", err=True)


@app.command()
def add_user(
    config_repo: str,
    *,
    vo: str = "testvo",
    user_group: str = "user",
    sub: str = "usersub",
    preferred_username: str = "preferred_username",
):
    """Add a user to an existing vo and group"""

    # TODO: The use of parse_obj_as should be moved in to typer itself
    config_repo = parse_obj_as(ConfigSourceUrl, config_repo)

    repo_path = Path(config_repo.path)

    new_user = UserConfig(PreferedUsername=preferred_username)

    config = ConfigSource.create_from_url(backend_url=repo_path).read_config()

    if sub in config.Registry[vo].Users:
        typer.echo(f"ERROR: User {sub} already exists", err=True)
        raise typer.Exit(1)

    config.Registry[vo].Users[sub] = new_user

    config.Registry[vo].Groups[user_group].Users.add(sub)

    repo = git.Repo.init(repo_path)
    yaml_path = repo_path / "default.yml"
    typer.echo(f"Writing back configuration to {yaml_path}", err=True)
    yaml_path.write_text(yaml.safe_dump(config.dict(exclude_unset=True)))
    repo.index.add([yaml_path.relative_to(repo_path)])
    repo.index.commit(
        f"Added user {sub} ({preferred_username}) to vo {vo} and user_group {user_group}"
    )
    typer.echo(f"Successfully added user to {config_repo}", err=True)
