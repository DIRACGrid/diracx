"""Utilities which are common to the sync and async jobs operator patches."""

from __future__ import annotations

__all__ = [
    "make_search_body",
    "SearchKwargs",
    "make_summary_body",
    "SummaryKwargs",
]

import json
from io import BytesIO
from typing import Any, IO, TypedDict, Unpack, cast, Literal

from diracx.core.models import SearchSpec


class ResponseExtra(TypedDict, total=False):
    content_type: str
    headers: dict[str, str]
    params: dict[str, str]
    cls: Any


class SearchBody(TypedDict, total=False):
    parameters: list[str] | None
    search: list[SearchSpec] | None
    sort: list[str] | None


class SearchExtra(ResponseExtra, total=False):
    page: int
    per_page: int


class SearchKwargs(SearchBody, SearchExtra): ...


class UnderlyingSearchArgs(ResponseExtra, total=False):
    # FIXME: The autorest-generated has a bug that it expected IO[bytes] despite
    # the code being generated to support IO[bytes] | bytes.
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


class SummaryBody(TypedDict, total=False):
    grouping: list[str]
    search: list[str]


class SummaryKwargs(SummaryBody, ResponseExtra): ...


class UnderlyingSummaryArgs(ResponseExtra, total=False):
    # FIXME: The autorest-generated has a bug that it expected IO[bytes] despite
    # the code being generated to support IO[bytes] | bytes.
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
