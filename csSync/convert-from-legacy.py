#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import diraccfg
import yaml
from pydantic import BaseModel

from diracx.core.config import DEFAULT_CONFIG_FILE, Config


def _git_path(value: str) -> Path:
    repo = Path(value)
    if not (repo / ".git").is_dir():
        raise ValueError(f"{repo} does not appear to be a git repository")
    return repo


def _list_to_str(original: str) -> list[str]:
    return [x.strip() for x in original.split(",") if x.strip()]


class IdPConfig(BaseModel):
    URL: str
    ClientID: str


class VOConfig(BaseModel):
    DefaultGroup: str
    IdP: IdPConfig
    UserSubjects: dict[str, str]


class ConversionConfig(BaseModel):
    VOs: dict[str, VOConfig]


def parse_args():
    parser = argparse.ArgumentParser("Convert the legacy DIRAC CS to the new format")
    parser.add_argument("old_file", type=Path)
    parser.add_argument("conversion_config", type=Path)
    parser.add_argument("repo", type=_git_path)
    args = parser.parse_args()

    if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
        raise RuntimeError(
            "DIRAC_COMPAT_ENABLE_CS_CONVERSION must be set for the conversion to be possible"
        )

    main(args.old_file, args.conversion_config, args.repo / DEFAULT_CONFIG_FILE)


def main(old_file: Path, conversion_config: Path, new_file: Path):
    """Load the old CS and convert it to the new YAML format"""
    old_data = old_file.read_text()
    cfg = diraccfg.CFG().loadFromBuffer(old_data)
    raw = cfg.getAsDict()

    apply_fixes(raw, conversion_config)

    config = Config.parse_obj(raw)
    new_data = json.loads(config.json(exclude_unset=True))
    new_file.write_text(yaml.safe_dump(new_data))


def apply_fixes(raw, conversion_config: Path):
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
    vos = set(raw["Registry"]["VO"])
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


if __name__ == "__main__":
    parse_args()
