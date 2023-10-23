import base64
import hashlib
import json
import os
from pathlib import Path
from typing import cast
from urllib.parse import urljoin, urlparse

import diraccfg
import typer
import yaml
from pydantic import BaseModel
from typer import Option

from diracx.core.config import Config
from diracx.core.config.schema import Field, SupportInfo

from ..utils import AsyncTyper

app = AsyncTyper()


class IdPConfig(BaseModel):
    URL: str
    ClientID: str


class VOConfig(BaseModel):
    DefaultGroup: str
    IdP: IdPConfig
    UserSubjects: dict[str, str]
    Support: SupportInfo = Field(default_factory=SupportInfo)


class ConversionConfig(BaseModel):
    VOs: dict[str, VOConfig]


# def parse_args():
#     parser = argparse.ArgumentParser("Convert the legacy DIRAC CS to the new format")
#     parser.add_argument("old_file", type=Path)
#     parser.add_argument("conversion_config", type=Path)
#     parser.add_argument("repo", type=Path)
#     args = parser.parse_args()


#     main(args.old_file, args.conversion_config, args.repo / DEFAULT_CONFIG_FILE)


@app.command()
def cs_sync(old_file: Path, conversion_config: Path, new_file: Path):
    """Load the old CS and convert it to the new YAML format"""
    if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
        raise RuntimeError(
            "DIRAC_COMPAT_ENABLE_CS_CONVERSION must be set for the conversion to be possible"
        )

    old_data = old_file.read_text()
    cfg = diraccfg.CFG().loadFromBuffer(old_data)
    raw = cfg.getAsDict()

    _apply_fixes(raw, conversion_config)

    config = Config.parse_obj(raw)
    new_file.write_text(yaml.safe_dump(config.dict(exclude_unset=True)))


def _apply_fixes(raw, conversion_config: Path):
    """Modify raw in place to make any layout changes between the old and new structure"""

    conv_config = ConversionConfig.parse_obj(
        yaml.safe_load(conversion_config.read_text())
    )

    raw.pop("DiracX", None)
    # Remove dips specific parts from the CS
    raw["DIRAC"].pop("Extensions", None)
    raw["DIRAC"].pop("Framework", None)
    raw["DIRAC"].pop("Security", None)

    # This is VOMS specific and no longer reqired
    raw["DIRAC"].pop("ConnConf", None)

    # Setups are no longer supported
    raw["DIRAC"].pop("DefaultSetup", None)
    raw["DIRAC"].pop("Setups", None)
    raw["DIRAC"].pop("Configuration", None)

    # All installations are no multi-VO
    raw["DIRAC"].pop("VirtualOrganization", None)

    # The default group now lives in /Registry
    raw["DIRAC"].pop("DefaultGroup", None)

    # Check that we have the config for all the VOs
    vos = set(raw["Registry"].get("VO", []))
    if non_configured_vos := vos - set(conv_config.VOs):
        print(f"{non_configured_vos} don't have a migration config, ignoring")

    # Modify the registry to be fully multi-VO
    original_registry = raw.pop("Registry")
    raw["Registry"] = {}

    for vo, vo_meta in conv_config.VOs.items():
        raw["Registry"][vo] = {
            "IdP": vo_meta.IdP,
            "DefaultGroup": vo_meta.DefaultGroup,
            "Users": {},
            "Groups": {},
            "Support": vo_meta.Support,
        }
        if "DefaultStorageQuota" in original_registry:
            raw["Registry"][vo]["DefaultStorageQuota"] = original_registry[
                "DefaultStorageQuota"
            ]
        if "DefaultProxyLifeTime" in original_registry:
            raw["Registry"][vo]["DefaultProxyLifeTime"] = original_registry[
                "DefaultProxyLifeTime"
            ]
        # Find the groups that belong to this VO
        vo_users = set()
        for name, info in original_registry["Groups"].items():
            if "VO" not in info:
                print(
                    f"Can't convert group {name} because it is not associated to any VO"
                )
                continue
            if info.get("VO", None) == vo:
                raw["Registry"][vo]["Groups"][name] = {
                    k: v for k, v in info.items() if k not in {"IdPRole", "VO"}
                }
                nicknames = {u.strip() for u in info["Users"].split(",") if u.strip()}
                vo_users |= nicknames
                raw["Registry"][vo]["Groups"][name]["Users"] = [
                    vo_meta.UserSubjects[n]
                    for n in nicknames
                    if n in vo_meta.UserSubjects
                ]
        # Find the users that belong to this VO
        for name, info in original_registry["Users"].items():
            if name in vo_users:
                if subject := vo_meta.UserSubjects.get(name):
                    raw["Registry"][vo]["Users"][subject] = info | {
                        "PreferedUsername": name
                    }
                    # We ignore the DN and CA
                    raw["Registry"][vo]["Users"][subject].pop("DN", None)
                    raw["Registry"][vo]["Users"][subject].pop("CA", None)


@app.command()
def generate_helm_values(
    public_cfg: Path = Option(help="Path to the cfg file served by the CS"),
    secret_cfg: Path = Option(
        default=None, help="Path to the cfg containing the secret"
    ),
    output_file: Path = Option(help="Where to dump the yam file"),
):
    """Generate an initial values.yaml to run a DiracX installation
    compatible with a DIRAC instance. The file is not complete, and needs
    manual editing"""

    helm_values = {
        "developer": {"enabled": False},
        "init-cs": {"enabled": True},
        "init-secrets": {"enabled": True},
        "init-sql": {"enabled": False, "env": {}},
        "cert-manager": {"enabled": False},
        "cert-manager-issuer": {"enabled": False},
        "minio": {"enabled": False},
        "dex": {"enabled": False},
        "opensearch": {"enabled": False},
        "ingress": {
            "enabled": True,
            "className": None,
            "tlsSecretName": None,
            "annotations": {
                "route.openshift.io/termination": "edge",
                "haproxy.router.openshift.io/ip_whitelist": "",
            },
        },
        "rabbitmq": {"enabled": False},
        "mysql": {"enabled": False},
        "diracx": {
            "manageOSIndices": False,
            "mysqlDatabases": [],
            "osDatabases": [],
            "settings": {},
        },
    }

    cfg = diraccfg.CFG().loadFromBuffer(public_cfg.read_text())

    if secret_cfg:
        cfg = cfg.mergeWith(diraccfg.CFG().loadFromBuffer(secret_cfg.read_text()))

    cfg = cast(dict, cfg.getAsDict())

    diracx_url = cfg["DiracX"]["URL"]
    diracx_hostname = urlparse(diracx_url).netloc.split(":", 1)[0]
    # Remove the port
    diracx_config = {
        "manageOSIndices": False,
        "mysqlDatabases": [],
        "osDatabases": [],
        "settings": {},
    }

    diracx_settings: dict[str, str] = {}
    diracx_config["settings"] = diracx_settings
    helm_values["diracx"] = diracx_config
    diracx_config["hostname"] = diracx_hostname

    diracx_settings["DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS"] = json.dumps(
        [
            urljoin(diracx_url, "api/docs/oauth2-redirect"),
            urljoin(diracx_url, "/#authentication-callback"),
        ]
    )

    default_db_user = cfg["Systems"].get("Databases", {}).get("User")
    default_db_password = cfg["Systems"].get("Databases", {}).get("Password")

    default_setup = cfg["DIRAC"]["Setup"]

    all_db_configs = {}
    for system, system_config in cfg["Systems"].items():
        system_setup = cfg["DIRAC"]["Setups"][default_setup].get(system, None)
        if system_setup:
            all_db_configs.update(system_config[system_setup].get("Databases", {}))

    from diracx.core.extensions import select_from_extension

    for entry_point in select_from_extension(group="diracx.db.sql"):
        db_name = entry_point.name
        # There is a DIRAC AuthDB, but it is not the same
        # as the DiracX one
        if db_name == "AuthDB":
            url_name = "DIRACX_DB_URL_AUTHDB"
            connection_string = "FILL ME: I am a new DB, create me"
        else:
            db_config = all_db_configs[db_name]
            url_name = f"DIRACX_DB_URL_{entry_point.name.upper()}"
            db_user = db_config.get("User", default_db_user)
            db_password = db_config.get("Password", default_db_password)
            db_host = db_config["Host"]
            db_port = db_config["Port"]
            indb_name = db_config["DBName"]

            connection_string = f"mysql+aiomysql://{db_user}:{db_password}@{db_host}:{db_port}/{indb_name}"
        diracx_settings[url_name] = connection_string

    # Settings for the legacy
    try:
        diracx_settings["DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY"] = hashlib.sha256(
            base64.urlsafe_b64decode(cfg["DiracX"]["LegacyExchangeApiKey"])
        ).hexdigest()
    except KeyError:
        typer.echo(
            "ERROR: you must have '/DiracX/LegacyExchangeApiKey' already set", err=True
        )
        raise typer.Exit(1) from None
    # Sandboxstore settings
    # TODO: Integrate minio for production use (ingress, etc)
    # By default, take the server hostname and prepend "sandboxes"
    diracx_settings[
        "DIRACX_SANDBOX_STORE_BUCKET_NAME"
    ] = f"{diracx_hostname.split('.')[0]}-sandboxes"
    diracx_settings["DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS"] = json.dumps(
        {
            "endpoint_url": "FILL ME",
            "aws_access_key_id": "FILL ME",
            "aws_secret_access_key": "FILL ME",
        }
    )

    diracx_settings["DIRACX_SERVICE_JOBS_ENABLED"] = "true"
    diracx_settings["DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET"] = "true"
    output_file.write_text(yaml.safe_dump(helm_values))
