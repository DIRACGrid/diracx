"""Validate a packed CWL process with cwltool (DX-ADR-007).

Loading the document checks it against the CWL v1.2 schema; constructing the
workflow runs cwltool's static checker, which catches step inputs wired to
outputs of an incompatible type. The packed document is what parcels run, so
validating it validates exactly what will execute.

cwltool reads nothing but the packed document: a reference to any other file
or URL is refused rather than fetched on the server. Its JavaScript checks are
off: they start ``node``, or pull and run a ``node`` container if there is
none, and only lint expressions, which parcels evaluate when they run.
"""

from __future__ import annotations

import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Any

from cwltool.context import LoadingContext
from cwltool.load_tool import load_tool
from cwltool.workflow import default_make_tool
from schema_salad.exceptions import ValidationException
from schema_salad.fetcher import DefaultFetcher

# cwltool reports every validation problem it also raises, at ERROR level
logging.getLogger("cwltool").setLevel(logging.CRITICAL)
logging.getLogger("salad").setLevel(logging.CRITICAL)


class InvalidProcessError(ValueError):
    """A process is not valid CWL v1.2."""


def validate_packed(packed: dict[str, Any]) -> None:
    """Check a packed CWL document as a stock runner would.

    Args:
        packed: Document made by ``diracx.core.cwl.pack``.

    Raises:
        InvalidProcessError: With cwltool's explanation of what is wrong.
    """
    # A document loaded from memory gets a random base URI against which the
    # $graph fragments do not resolve, so cwltool is given a file to load.
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "process.cwl"
        path.write_text(json.dumps(packed))
        context = LoadingContext(
            {
                "do_update": False,
                "construct_tool_object": default_make_tool,
                "fetcher_constructor": _only(path.as_uri()),
                "disable_js_validation": True,
            }
        )
        try:
            load_tool(path.as_uri() + "#main", context)
        except Exception as e:
            raise InvalidProcessError(_tidy(str(e), directory)) from None


def _only(allowed: str):
    """A fetcher constructor for cwltool that reads ``allowed`` and nothing else."""

    class OnlyFetcher(DefaultFetcher):
        def fetch_text(self, url: str, content_types: list[str] | None = None) -> str:
            if url.split("#", 1)[0] != allowed:
                raise ValidationException(f"Refusing to fetch {url}")
            return super().fetch_text(url, content_types)

        def check_exists(self, url: str) -> bool:
            return url.split("#", 1)[0] == allowed and super().check_exists(url)

    return OnlyFetcher


def _tidy(message: str, directory: str) -> str:
    """Drop the temporary path and cwltool's line wrapping from a message."""
    message = re.sub(
        r"\S*" + re.escape(Path(directory).name) + r"/process\.cwl:?", "", message
    )
    return re.sub(r"\s+", " ", message).strip()
