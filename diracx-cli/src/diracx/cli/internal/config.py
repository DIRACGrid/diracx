from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import git
import typer
import yaml
from pydantic import TypeAdapter

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

app = AsyncTyper()


@app.command()
def generate_cs(config_repo: str):
    """Generate a minimal DiracX configuration repository."""
    # TODO: The use of TypeAdapter should be moved in to typer itself
    config_repo = TypeAdapter(ConfigSourceUrl).validate_python(config_repo)
    if config_repo.scheme != "git+file" or config_repo.path is None:
        raise NotImplementedError("Only git+file:// URLs are supported")
    repo_path = Path(config_repo.path)
    if repo_path.exists() and list(repo_path.iterdir()):
        typer.echo(f"ERROR: Directory {repo_path} already exists", err=True)
        raise typer.Exit(1)

    config = Config(
        Registry={},
        DIRAC=DIRACConfig(),
        Operations={"Defaults": OperationsConfig()},
    )

    git.Repo.init(repo_path, initial_branch="master")
    update_config_and_commit(
        repo_path=repo_path, config=config, message="Initial commit"
    )
    typer.echo(f"Successfully created repo in {config_repo}", err=True)


@app.command()
def add_vo(
    config_repo: str,
    *,
    vo: Annotated[str, typer.Option()],
    default_group: Optional[str] = "user",
    idp_url: Annotated[str, typer.Option()],
    idp_client_id: Annotated[str, typer.Option()],
):
    """Add a registry entry (vo) to an existing configuration repository."""
    # TODO: The use of TypeAdapter should be moved in to typer itself
    config_repo = TypeAdapter(ConfigSourceUrl).validate_python(config_repo)
    if config_repo.scheme != "git+file" or config_repo.path is None:
        raise NotImplementedError("Only git+file:// URLs are supported")
    repo_path = Path(config_repo.path)

    # A VO should at least contain a default group
    new_registry = RegistryConfig(
        IdP=IdpConfig(URL=idp_url, ClientID=idp_client_id),
        DefaultGroup=default_group,
        Users={},
        Groups={
            default_group: GroupConfig(
                Properties={"NormalUser"}, Quota=None, Users=set()
            )
        },
    )

    config = ConfigSource.create_from_url(backend_url=repo_path).read_config()

    if vo in config.Registry:
        typer.echo(f"ERROR: VO {vo} already exists", err=True)
        raise typer.Exit(1)

    config.Registry[vo] = new_registry

    update_config_and_commit(
        repo_path=repo_path,
        config=config,
        message=f"Added vo {vo} registry (default group {default_group} and idp {idp_url})",
    )
    typer.echo(f"Successfully added vo to {config_repo}", err=True)


@app.command()
def add_group(
    config_repo: str,
    *,
    vo: Annotated[str, typer.Option()],
    group: Annotated[str, typer.Option()],
    properties: list[str] = ["NormalUser"],
):
    """Add a group to an existing vo in the configuration repository."""
    # TODO: The use of TypeAdapter should be moved in to typer itself
    config_repo = TypeAdapter(ConfigSourceUrl).validate_python(config_repo)
    if config_repo.scheme != "git+file" or config_repo.path is None:
        raise NotImplementedError("Only git+file:// URLs are supported")
    repo_path = Path(config_repo.path)

    new_group = GroupConfig(Properties=set(properties), Quota=None, Users=set())

    config = ConfigSource.create_from_url(backend_url=repo_path).read_config()

    if vo not in config.Registry:
        typer.echo(f"ERROR: Virtual Organization {vo} does not exist", err=True)
        raise typer.Exit(1)

    if group in config.Registry[vo].Groups.keys():
        typer.echo(f"ERROR: Group {group} already exists in {vo}", err=True)
        raise typer.Exit(1)

    config.Registry[vo].Groups[group] = new_group

    update_config_and_commit(
        repo_path=repo_path, config=config, message=f"Added group {group} in {vo}"
    )
    typer.echo(f"Successfully added group to {config_repo}", err=True)


@app.command()
def add_user(
    config_repo: str,
    *,
    vo: Annotated[str, typer.Option()],
    groups: Annotated[Optional[list[str]], typer.Option("--group")] = None,
    sub: Annotated[str, typer.Option()],
    preferred_username: Annotated[str, typer.Option()],
):
    """Add a user to an existing vo and group."""
    # TODO: The use of TypeAdapter should be moved in to typer itself
    config_repo = TypeAdapter(ConfigSourceUrl).validate_python(config_repo)
    if config_repo.scheme != "git+file" or config_repo.path is None:
        raise NotImplementedError("Only git+file:// URLs are supported")

    repo_path = Path(config_repo.path)

    new_user = UserConfig(PreferedUsername=preferred_username)

    config = ConfigSource.create_from_url(backend_url=repo_path).read_config()

    if vo not in config.Registry:
        typer.echo(f"ERROR: Virtual Organization {vo} does not exist", err=True)
        raise typer.Exit(1)

    if sub in config.Registry[vo].Users:
        typer.echo(f"ERROR: User {sub} already exists", err=True)
        raise typer.Exit(1)

    config.Registry[vo].Users[sub] = new_user

    if not groups:
        groups = [config.Registry[vo].DefaultGroup]

    for group in set(groups):
        if group not in config.Registry[vo].Groups:
            typer.echo(f"ERROR: Group {group} does not exist in {vo}", err=True)
            raise typer.Exit(1)
        if sub in config.Registry[vo].Groups[group].Users:
            typer.echo(f"ERROR: User {sub} already exists in group {group}", err=True)
            raise typer.Exit(1)

        config.Registry[vo].Groups[group].Users.add(sub)

    update_config_and_commit(
        repo_path=repo_path,
        config=config,
        message=f"Added user {sub} ({preferred_username}) to vo {vo} and groups {groups}",
    )
    typer.echo(f"Successfully added user to {config_repo}", err=True)


def update_config_and_commit(repo_path: Path, config: Config, message: str):
    """Update the yaml file in the repo and commit it."""
    repo = git.Repo(repo_path)
    yaml_path = repo_path / "default.yml"
    typer.echo(f"Writing back configuration to {yaml_path}", err=True)
    yaml_path.write_text(
        yaml.safe_dump(config.model_dump(exclude_unset=True, mode="json"))
    )
    repo.index.add([yaml_path.relative_to(repo_path)])
    repo.index.commit(message)
