from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import urljoin, urlparse

import diraccfg
import typer
import yaml

if TYPE_CHECKING:
    from diraccfg.cfg import CFGAsDict

from pydantic import BaseModel
from typer import Option

from diracx.core.config import Config
from diracx.core.config.schema import Field, SupportInfo
from diracx.core.extensions import select_from_extension

from ..utils import AsyncTyper

app = AsyncTyper()


BASE_64_URL_SAFE_PATTERN = (
    r"(?:[A-Za-z0-9\-_]{4})*(?:[A-Za-z0-9\-_]{2}==|[A-Za-z0-9\-_]{3}=)?"
)
LEGACY_EXCHANGE_PATTERN = rf"diracx:legacy:({BASE_64_URL_SAFE_PATTERN})"


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


@app.command()
def cs_sync(old_file: Path, new_file: Path):
    """Load the old CS and convert it to the new YAML format."""
    if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
        raise RuntimeError(
            "DIRAC_COMPAT_ENABLE_CS_CONVERSION must be set for the conversion to be possible"
        )

    old_data = old_file.read_text()
    cfg = diraccfg.CFG().loadFromBuffer(old_data)
    raw = cfg.getAsDict()

    diracx_section = cast("CFGAsDict", raw["DiracX"])
    # DisabledVOs cannot be set if any Legacy clients are enabled
    disabled_vos = diracx_section.get("DisabledVOs")
    enabled_clients = []
    for _, client_status in cast(
        "CFGAsDict", diracx_section.get("LegacyClientEnabled", {})
    ).items():
        for _, str_status in cast("CFGAsDict", client_status).items():
            enabled_clients.append(str_status == "True")
    if disabled_vos and any(enabled_clients):
        raise RuntimeError(
            "DisabledVOs cannot be set if any Legacy clients are enabled"
        )

    _apply_fixes(raw)
    config_class: Config = select_from_extension(group="diracx", name="config")[
        0
    ].load()
    config = config_class.model_validate(raw)
    new_file.write_text(
        yaml.safe_dump(config.model_dump(exclude_unset=True, mode="json"))
    )


def _apply_fixes(raw):
    """Modify raw in place to make any layout changes between the old and new structure."""
    conv_config = ConversionConfig.model_validate(raw["DiracX"]["CsSync"])

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
            "IdP": vo_meta.IdP.model_dump(),
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
        # Copy over the necessary parts of the VO section
        for key in {"VOMSName"}:
            if key in original_registry.get("VO", {}).get(vo, {}):
                raw["Registry"][vo][key] = original_registry["VO"][vo][key]
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
                    # Strip any DNs which are from the failed OAuth2 attempt
                    raw_dn = raw["Registry"][vo]["Users"][subject].pop("DN", None)
                    raw["Registry"][vo]["Users"][subject]["DNs"] = [
                        dn.strip()
                        for dn in raw_dn.split(",")
                        if not dn.strip().startswith("/O=DIRAC/")
                    ]
                    # We ignore the CA
                    raw["Registry"][vo]["Users"][subject].pop("CA", None)


@app.command()
def generate_helm_values(
    public_cfg: Path = Option(help="Path to the cfg file served by the CS"),
    secret_cfg: Path = Option(
        default=None, help="Path to the cfg containing the secret"
    ),
    output_file: Path = Option(help="Where to dump the yam file"),
):
    """Generate an initial values.yaml to run a DiracX installation.

    The file generated is not complete, and needs manual editing.
    """
    helm_values = {
        "developer": {"enabled": False},
        "initCs": {"enabled": True},
        "initSecrets": {"enabled": True},
        "initSql": {"enabled": False},
        "cert-manager": {"enabled": False},
        "cert-manager-issuer": {"enabled": False},
        "minio": {"enabled": False},
        "dex": {"enabled": False},
        "opensearch": {"enabled": False},
        # This is Openshift specific, change it maybe
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
        "global": {"images": {"services": "FILL ME"}, "storageClassName": "FILL ME"},
    }

    cfg = diraccfg.CFG().loadFromBuffer(public_cfg.read_text())

    if secret_cfg:
        cfg = cfg.mergeWith(diraccfg.CFG().loadFromBuffer(secret_cfg.read_text()))

    cfg = cast(dict, cfg.getAsDict())

    diracx_url = cfg["DiracX"]["URL"]
    diracx_hostname = urlparse(diracx_url).netloc.split(":", 1)[0]

    diracx_config: dict = {
        "sqlDbs": {},
        "osDbs": {},
    }

    diracx_settings: dict[str, str] = {"DIRACX_CONFIG_BACKEND_URL": "FILL ME"}
    diracx_config["settings"] = diracx_settings
    helm_values["diracx"] = diracx_config
    diracx_config["hostname"] = diracx_hostname

    diracx_settings["DIRACX_SERVICE_AUTH_TOKEN_ISSUER"] = diracx_url
    diracx_settings["DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS"] = json.dumps(
        [
            urljoin(diracx_url, "api/docs/oauth2-redirect"),
            urljoin(diracx_url, "/#authentication-callback"),
        ]
    )

    ### SQL DBs

    default_db_user = cfg["Systems"].get("Databases", {}).get("User")
    default_db_password = cfg["Systems"].get("Databases", {}).get("Password")
    default_db_host = cfg["Systems"].get("Databases", {}).get("Host", "FILL ME")
    default_db_port = cfg["Systems"].get("Databases", {}).get("Port", "FILL ME")

    all_db_configs = {}
    sql_dbs = {
        "dbs": {},
        "default": {
            "host": f"{default_db_host}:{default_db_port}",
            "password": default_db_password,
            "rootPassword": "FILL ME",
            "rootUser": "FILL ME",
            "user": default_db_user,
        },
    }
    for _system, system_config in cfg["Systems"].items():
        all_db_configs.update(system_config.get("Databases", {}))

    from diracx.core.extensions import select_from_extension

    for entry_point in select_from_extension(group="diracx.db.sql"):

        db_name = entry_point.name
        db_config = all_db_configs.get(db_name, {})

        sql_dbs["dbs"][db_name] = {}
        # There is a DIRAC AuthDB, but it is not the same
        # as the DiracX one
        if db_name == "AuthDB":
            sql_dbs["dbs"]["AuthDB"] = {"internalName": "DiracXAuthDB"}

        if "DBName" in db_config:
            indb_name = db_config["DBName"]
            if indb_name != db_name:
                sql_dbs["dbs"]["internalName"] = indb_name
        if "User" in db_config:
            sql_dbs["dbs"][db_name]["user"] = db_config.get("User")
        if "Password" in db_config:
            sql_dbs["dbs"][db_name]["password"] = db_config.get("Password")
        if "Host" in db_config or "Port" in db_config:
            sql_dbs["dbs"][db_name][
                "host"
            ] = f"{db_config.get('Host', default_db_host)}:{db_config.get('Port', default_db_port)}"
        if not sql_dbs["dbs"][db_name]:
            sql_dbs["dbs"][db_name] = None

    diracx_config["sqlDbs"] = sql_dbs

    #### END SQL DB

    # #### OS DBs

    default_os_db_user = cfg["Systems"].get("NoSQLDatabases", {}).get("User")
    default_os_db_password = cfg["Systems"].get("NoSQLDatabases", {}).get("Password")
    default_os_db_host = cfg["Systems"].get("NoSQLDatabases", {}).get("Host", "FILL ME")

    os_dbs = {
        "dbs": {},
        "default": {
            "host": f"{default_os_db_host}",
            "password": default_os_db_password,
            "rootPassword": "FILL ME",
            "rootUser": "FILL ME",
            "user": default_os_db_user,
        },
    }

    for entry_point in select_from_extension(group="diracx.db.os"):
        db_name = entry_point.name
        db_config = all_db_configs.get(db_name, {})

        os_dbs["dbs"][db_name] = {}
        # There is a DIRAC AuthDB, but it is not the same
        # as the DiracX one

        if "DBName" in db_config:
            indb_name = db_config["DBName"]
            if indb_name != db_name:
                os_dbs["dbs"]["internalName"] = indb_name
        if "User" in db_config:
            os_dbs["dbs"][db_name]["user"] = db_config["User"]
        if "Password" in db_config:
            os_dbs["dbs"][db_name]["password"] = db_config["Password"]
        if "Host" in db_config:
            os_dbs["dbs"][db_name]["host"] = db_config["Host"]

        if not os_dbs["dbs"][db_name]:
            os_dbs["dbs"][db_name] = None

    diracx_config["osDbs"] = os_dbs

    #### End OS DBs

    # Settings for the legacy
    try:
        if match := re.fullmatch(
            LEGACY_EXCHANGE_PATTERN, cfg["DiracX"]["LegacyExchangeApiKey"]
        ):
            raw_token = base64.urlsafe_b64decode(match.group(1))
        else:
            raise ValueError(
                "Invalid authorization header",
            )

        diracx_settings["DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY"] = hashlib.sha256(
            raw_token
        ).hexdigest()
    except KeyError:
        error_msg = """
            ERROR: you must have '/DiracX/LegacyExchangeApiKey' already set.
            See the `legacy_exchange` function definition for how to generate it in python
        """
        typer.echo(error_msg, err=True)
        raise typer.Exit(1) from None
    # Sandboxstore settings
    # TODO: Integrate minio for production use (ingress, etc)
    # By default, take the server hostname and prepend "sandboxes"
    diracx_settings["DIRACX_SANDBOX_STORE_BUCKET_NAME"] = (
        f"{diracx_hostname.split('.')[0]}-sandboxes"
    )
    try:
        diracx_settings["DIRACX_SANDBOX_STORE_SE_NAME"] = cfg["Systems"][
            "WorkloadManagement"
        ]["Services"]["SandboxStore"]["LocalSE"]
    except KeyError:
        pass

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
    typer.echo(
        "The file is incomplete and needs manual editing (grep for 'FILL ME')", err=True
    )
