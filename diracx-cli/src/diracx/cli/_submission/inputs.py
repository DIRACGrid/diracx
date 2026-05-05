"""Input parsing utilities for CWL job submission."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml


def parse_input_files(files: list[Path]) -> list[dict[str, Any]]:
    """Parse job input sets from YAML or JSON files.

    YAML files support multi-document format (--- separators).
    JSON files are treated as a single input set.
    Multiple files are concatenated into a flat list of job inputs.
    """
    results: list[dict[str, Any]] = []
    for path in files:
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        content = path.read_text()
        if path.suffix.lower() == ".json":
            results.append(json.loads(content))
        else:
            # YAML — may be multi-document
            docs = list(yaml.safe_load_all(content))
            results.extend(doc for doc in docs if doc is not None)
    return results


def parse_range(range_str: str) -> tuple[str, int, int, int]:
    """Parse a --range argument of the form PARAM=END, PARAM=START:END, or PARAM=START:END:STEP.

    Returns (param, start, end, step).
    """
    if "=" not in range_str:
        raise ValueError(
            f"Invalid range format {range_str!r}: expected PARAM=END, "
            "PARAM=START:END, or PARAM=START:END:STEP"
        )
    param, _, spec = range_str.partition("=")
    parts = spec.split(":")
    try:
        if len(parts) == 1:
            return param, 0, int(parts[0]), 1
        elif len(parts) == 2:
            return param, int(parts[0]), int(parts[1]), 1
        elif len(parts) == 3:
            return param, int(parts[0]), int(parts[1]), int(parts[2])
        else:
            raise ValueError(
                f"Invalid range format {range_str!r}: too many ':' separators"
            )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid range format {range_str!r}: {exc}") from exc


def _file_type(value: str) -> dict[str, str]:
    """Argparse type converter for CWL File inputs.

    URI schemes (``LFN:``, ``SB:``) go into ``location`` per the CWL spec;
    local filesystem paths go into ``path``.
    """
    if value.startswith(("LFN:", "SB:")):
        return {"class": "File", "location": value}
    return {"class": "File", "path": value}


def parse_cli_args(cwl_inputs: list[dict[str, Any]], args: list[str]) -> dict[str, Any]:
    """Parse CLI arguments against CWL workflow input declarations.

    Builds an argparse parser dynamically from the CWL ``inputs`` list.
    Returns a dict of {input_id: value}, omitting inputs that were not
    provided (i.e. still ``None`` / empty list after parsing).
    """
    if not args:
        return {}

    parser = argparse.ArgumentParser(add_help=False)

    for inp in cwl_inputs:
        inp_id: str = inp["id"]
        inp_type = inp["type"]
        flag = f"--{inp_id}"

        if inp_type == "boolean":
            parser.add_argument(flag, dest=inp_id, action="store_true", default=None)
        elif inp_type == "File":
            parser.add_argument(flag, dest=inp_id, type=_file_type, default=None)
        elif inp_type == "File[]":
            parser.add_argument(
                flag,
                dest=inp_id,
                type=_file_type,
                action="append",
                default=None,
            )
        else:
            # Complex types (enums, records, unions) are dicts in the CWL.
            # Treat them as opaque strings on the CLI — proper typed values
            # belong in an inputs.yml; this path just allows overriding by
            # name without crashing on unhashable dict keys.
            type_map: dict[str, type] = {"string": str, "int": int, "float": float}
            py_type = type_map.get(inp_type, str) if isinstance(inp_type, str) else str
            parser.add_argument(flag, dest=inp_id, type=py_type, default=None)

    namespace = parser.parse_args(args)
    return {k: v for k, v in vars(namespace).items() if v is not None}
