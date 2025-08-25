"""Utilities which are common to the sync and async jobs operator patches."""

from __future__ import annotations

from diracx.client._generated.models._models import JobMetaData

__all__ = [
    "make_search_body",
    "SearchKwargs",
    "make_summary_body",
    "SummaryKwargs",
    "prepare_body_for_patch",
]

import json
from io import BytesIO, IOBase
from typing import Any, IO, Dict, TypedDict, Union, Unpack, cast, Literal

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
    search: list[SearchSpec]


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


def prepare_body_for_patch(
    body: Union[Dict[str | int, JobMetaData | Dict[str, Any]], IO[bytes], bytes]
) -> Union[Dict[str, JobMetaData], IO[bytes], bytes]:
    """Return a body suitable for the generated serializer.

    - If bytes/IO: pass through (caller handles serialization).
    - If dict: coerce each value to JobMetaData and inject extras.
    - Accepts pythonic keys (e.g., 'job_type') or wire keys (e.g., 'JobType').
    """
    if isinstance(body, (IOBase, bytes)):
        return body
    if not isinstance(body, dict):
        raise TypeError("body must be a dict[job_id -> JobMetaData|dict] or IO[bytes]")

    attr_map = JobMetaData._attribute_map  # type: ignore[attr-defined]
    py_names = set(attr_map.keys())
    wire_to_py = {spec["key"]: py for py, spec in attr_map.items()}

    out: Dict[str, JobMetaData] = {}
    for job_id, metadata in body.items():
        if isinstance(metadata, JobMetaData):
            obj = metadata
        elif isinstance(metadata, dict):
            init_kwargs: Dict[str, Any] = {}
            extras: Dict[str, Any] = {}
            for k, v in metadata.items():
                # known with no alias
                if k in py_names:
                    init_kwargs[k] = v
                # known with alias
                elif k in wire_to_py:
                    init_kwargs[wire_to_py[k]] = v
                # unknown -> additional_properties
                else:
                    extras[k] = v
            obj = JobMetaData(**init_kwargs)
            if extras:
                obj.additional_properties = extras
        else:
            raise TypeError(f"metadata for job {job_id!r} must be dict or JobMetaData, not {type(metadata).__name__}")

        out[str(job_id)] = obj

    return out
