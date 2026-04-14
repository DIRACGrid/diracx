"""Utilities shared by the sync and async pilots operator patches."""

from __future__ import annotations

__all__ = [
    "SearchKwargs",
    "make_search_body",
    "SummaryKwargs",
    "make_summary_body",
    "RegisterPilotsKwargs",
    "make_register_pilots_body",
]

import json
from io import BytesIO
from typing import IO, Any, Literal, TypedDict, Unpack, cast

from diracx.core.models.pilot import PilotStatus
from diracx.core.models.search import SearchSpec


class ResponseExtra(TypedDict, total=False):
    content_type: str
    headers: dict[str, str]
    params: dict[str, str]
    cls: Any


# ------------------ Search ------------------


class SearchBody(TypedDict, total=False):
    parameters: list[str] | None
    search: list[SearchSpec] | None
    sort: list[str] | None


class SearchExtra(ResponseExtra, total=False):
    page: int
    per_page: int


class SearchKwargs(SearchBody, SearchExtra): ...


class UnderlyingSearchArgs(ResponseExtra, total=False):
    # FIXME: The autorest-generated operation expects IO[bytes] despite its
    # signature advertising IO[bytes] | bytes.
    body: IO[bytes]


def make_search_body(**kwargs: Unpack[SearchKwargs]) -> UnderlyingSearchArgs:
    body: SearchBody = {}
    for key in SearchBody.__optional_keys__:
        if key not in kwargs:
            continue
        key = cast(Literal["parameters", "search", "sort"], key)
        value = kwargs.pop(key)
        if value is not None:
            body[key] = value
    result: UnderlyingSearchArgs = {"body": BytesIO(json.dumps(body).encode("utf-8"))}
    result.update(cast(SearchExtra, kwargs))
    return result


# ------------------ Summary ------------------


class SummaryBody(TypedDict, total=False):
    grouping: list[str]
    search: list[SearchSpec]


class SummaryKwargs(SummaryBody, ResponseExtra): ...


class UnderlyingSummaryArgs(ResponseExtra, total=False):
    # FIXME: The autorest-generated operation expects IO[bytes] despite its
    # signature advertising IO[bytes] | bytes.
    body: IO[bytes]


def make_summary_body(**kwargs: Unpack[SummaryKwargs]) -> UnderlyingSummaryArgs:
    body: SummaryBody = {}
    for key in SummaryBody.__optional_keys__:
        if key not in kwargs:
            continue
        key = cast(Literal["grouping", "search"], key)
        value = kwargs.pop(key)
        if value is not None:
            body[key] = value
    result: UnderlyingSummaryArgs = {"body": BytesIO(json.dumps(body).encode("utf-8"))}
    result.update(cast(ResponseExtra, kwargs))
    return result


# ------------------ Register pilots ------------------


class RegisterPilotsBody(TypedDict, total=False):
    pilot_stamps: list[str]
    vo: str
    grid_type: str
    grid_site: str
    destination_site: str
    pilot_references: dict[str, str]
    pilot_status: PilotStatus


class RegisterPilotsKwargs(RegisterPilotsBody, ResponseExtra): ...


class UnderlyingRegisterPilotsArgs(ResponseExtra, total=False):
    # FIXME: The autorest-generated operation expects IO[bytes] despite its
    # signature advertising IO[bytes] | bytes.
    body: IO[bytes]


def make_register_pilots_body(
    **kwargs: Unpack[RegisterPilotsKwargs],
) -> UnderlyingRegisterPilotsArgs:
    body: RegisterPilotsBody = {}
    for key in RegisterPilotsBody.__optional_keys__:
        if key not in kwargs:
            continue
        key = cast(
            Literal[
                "pilot_stamps",
                "vo",
                "grid_type",
                "grid_site",
                "destination_site",
                "pilot_references",
                "pilot_status",
            ],
            key,
        )
        value = kwargs.pop(key)
        if value is not None:
            body[key] = value
    result: UnderlyingRegisterPilotsArgs = {
        "body": BytesIO(json.dumps(body).encode("utf-8"))
    }
    result.update(cast(ResponseExtra, kwargs))
    return result
