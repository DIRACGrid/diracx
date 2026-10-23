"""Validate a packed CWL process with cwl-utils (DX-ADR-007).

Loading the document with cwl-utils' parser checks every component of it
against the CWL v1.2 schema. The packed document is what parcels run, so
validating it validates exactly what will execute.

The parser reads nothing but the packed document: a reference to any other
file or URL is refused rather than fetched on the server.

Step inputs wired to outputs of an incompatible type are not checked yet:
cwl-utils' static checker misses them, and rejects valid workflows whose
enums are named differently, until a release includes
common-workflow-language/cwl-utils#480.
"""

from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from cwl_utils.parser import LoadingOptions, load_document_by_uri
from schema_salad.exceptions import ValidationException
from schema_salad.fetcher import DefaultFetcher


class InvalidProcessError(ValueError):
    """A process is not valid CWL v1.2."""


def validate_packed(packed: dict[str, Any]) -> None:
    """Check a packed CWL document against the CWL v1.2 schema.

    Args:
        packed: Document made by ``diracx.core.cwl.pack``.

    Raises:
        InvalidProcessError: With cwl-utils' explanation of what is wrong.
    """
    # The $graph fragments resolve against the document's own URI, so the
    # parser is given a file to load. Resolved, as the parser resolves
    # symbolic links (macOS's /var) in the URIs it compares against.
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory).resolve() / "process.cwl"
        path.write_text(json.dumps(packed))
        options = LoadingOptions(fetcher=_only(path.as_uri())())
        # One component at a time: loaded whole, the parser reports any
        # error as the $graph array failing to match, without the reason
        for entry in packed["$graph"]:
            try:
                load_document_by_uri(path.as_uri() + entry["id"], options)
            except Exception as e:
                message = _tidy(str(e), directory, entry["id"])
                raise InvalidProcessError(message) from None


def _only(allowed: str):
    """A fetcher for cwl-utils that reads ``allowed`` and nothing else."""

    class OnlyFetcher(DefaultFetcher):
        def __init__(self) -> None:
            super().__init__({}, None)

        def fetch_text(self, url: str, content_types: list[str] | None = None) -> str:
            if url.split("#", 1)[0] != allowed:
                raise ValidationException(f"Refusing to fetch {url}")
            return super().fetch_text(url, content_types)

        def check_exists(self, url: str) -> bool:
            # The parser also asks about bare vocabulary terms ("Workflow"),
            # which are not locations
            if urlsplit(url).scheme and url.split("#", 1)[0] != allowed:
                return False
            return super().check_exists(url)

    return OnlyFetcher


def _tidy(message: str, directory: str, entry_id: str) -> str:
    """Name the component, not the temporary file, and drop line wrapping."""
    message = re.sub(
        r"`process\.cwl(#[^`]*)?`", lambda m: f"`{m.group(1) or entry_id}`", message
    )
    message = re.sub(
        r"\S*" + re.escape(Path(directory).name) + r"/process\.cwl[:\d]*", "", message
    )
    return re.sub(r"\s+", " ", message).strip()
