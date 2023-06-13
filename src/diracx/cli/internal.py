from __future__ import absolute_import

import json
from pathlib import Path
from typing import Annotated

import git
import typer
import yaml

from diracx.core.config.schema import (
    Config,
    DIRACConfig,
    GroupConfig,
    IdpConfig,
    OperationsConfig,
    RegistryConfig,
)

from .utils import AsyncTyper

app = AsyncTyper()


@app.command()
def generate_cs(
    config_repo: Annotated[
        Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
    ],
    *,
    vo: str = "testvo",
    user_group: str = "user",
    idp_url: str = "https://idp.invalid",
    idp_client_id: str = "idp-client-id",
):
    """Generate a minimal DiracX configuration repository"""
    if config_repo.exists():
        typer.echo(f"ERROR: Directory {config_repo} already exists", err=True)
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

    repo = git.Repo.init(config_repo, initial_branch="master")
    yaml_path = config_repo / "default.yml"
    typer.echo(f"Writing configuration to {yaml_path}", err=True)
    config_data = json.loads(config.json(exclude_unset=True))
    yaml_path.write_text(yaml.safe_dump(config_data))
    repo.index.add([yaml_path])
    repo.index.commit("Initial commit")
    typer.echo(f"Successfully created repo in {config_repo}", err=True)
