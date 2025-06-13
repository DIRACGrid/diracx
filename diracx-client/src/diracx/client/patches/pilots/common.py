"""Utilities which are common to the sync and async pilots operator patches."""

from __future__ import annotations

__all__ = [
    "make_search_body",
    "SearchKwargs",
    "make_summary_body",
    "SummaryKwargs",
    "AddPilotStampsKwargs",
    "make_add_pilot_stamps_body",
    "UpdatePilotFieldsKwargs",
    "make_update_pilot_fields_body"
]

import json
from io import BytesIO
from typing import Any, IO, TypedDict, Unpack, cast, Literal

from diracx.core.models import SearchSpec, PilotStatus, PilotFieldsMapping


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

# ------------------ Summary ------------------

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

# ------------------ AddPilotStamps ------------------

class AddPilotStampsBody(TypedDict, total=False):
    pilot_stamps: list[str]
    grid_type: str
    grid_site: str
    pilot_references: dict[str, str]
    pilot_status: PilotStatus
    vo: str

class AddPilotStampsKwargs(AddPilotStampsBody, ResponseExtra): ...

class UnderlyingAddPilotStampsArgs(ResponseExtra, total=False):
    # FIXME: The autorest-generated has a bug that it expected IO[bytes] despite
    # the code being generated to support IO[bytes] | bytes.
    body: IO[bytes]

def make_add_pilot_stamps_body(**kwargs: Unpack[AddPilotStampsKwargs]) -> UnderlyingAddPilotStampsArgs:
    body: AddPilotStampsBody = {}
    for key in AddPilotStampsBody.__optional_keys__:
        if key not in kwargs:
            continue
        key = cast(Literal["pilot_stamps", "grid_type", "grid_site", "pilot_references", "pilot_status", "vo"], key)
        value = kwargs.pop(key)
        if value is not None:
            body[key] = value
    result: UnderlyingAddPilotStampsArgs = {"body": BytesIO(json.dumps(body).encode("utf-8"))}
    result.update(cast(ResponseExtra, kwargs))
    return result

# ------------------ UpdatePilotFields ------------------

class UpdatePilotFieldsBody(TypedDict, total=False):
    pilot_stamps_to_fields_mapping: list[PilotFieldsMapping]

class UpdatePilotFieldsKwargs(UpdatePilotFieldsBody, ResponseExtra): ...

class UnderlyingUpdatePilotFields(ResponseExtra, total=False):
    # FIXME: The autorest-generated has a bug that it expected IO[bytes] despite
    # the code being generated to support IO[bytes] | bytes.
    body: IO[bytes]

def make_update_pilot_fields_body(**kwargs: Unpack[UpdatePilotFieldsKwargs]) -> UnderlyingUpdatePilotFields:
    body: UpdatePilotFieldsBody = {}
    for key in UpdatePilotFieldsBody.__optional_keys__:
        if key not in kwargs:
            continue
        key = cast(Literal["pilot_stamps_to_fields_mapping"], key)
        value = kwargs.pop(key)
        if value is not None:
            body[key] = value
    result: UnderlyingUpdatePilotFields = {"body": BytesIO(json.dumps(body).encode("utf-8"))}
    result.update(cast(ResponseExtra, kwargs))
    return result
