from __future__ import absolute_import

import json
from pathlib import Path

import git
import typer
import yaml
from pydantic import parse_obj_as

from diracx.core.config.schema import (
    Config,
    DIRACConfig,
    GroupConfig,
    IdpConfig,
    OperationsConfig,
    RegistryConfig,
)
from diracx.core.settings import LocalFileUrl

from .utils import AsyncTyper

app = AsyncTyper()


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
    config_repo = parse_obj_as(LocalFileUrl, config_repo)
    if config_repo.scheme != "file":
        raise NotImplementedError("Only file:// URLs are supported")
    repo_path = Path(config_repo.path)
    if repo_path.exists() and list(repo_path.iterdir()):
        typer.echo(f"ERROR: Directory {repo_path} already exists", err=True)
        raise typer.Exit(1)

    registry = RegistryConfig(
        IdP=IdpConfig(URL=idp_url, ClientID=idp_client_id),
        DefaultGroup=user_group,
        Users={},
        Groups={
            user_group: GroupConfig(
                JobShare=None, Properties=["NormalUser"], Quota=None, Users=[]
            )
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
    config_data = json.loads(config.json(exclude_unset=True))
    yaml_path.write_text(yaml.safe_dump(config_data))
    repo.index.add([yaml_path])
    repo.index.commit("Initial commit")
    typer.echo(f"Successfully created repo in {config_repo}", err=True)
