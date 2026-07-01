"""Search query models used by DIRACX.

This module defines the request models and operators used for filtering,
sorting, and summarizing search results.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel
from typing_extensions import TypedDict


class ScalarSearchOperator(StrEnum):
    """Supported operators for scalar search filters."""

    EQUAL = "eq"
    NOT_EQUAL = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    LIKE = "like"
    NOT_LIKE = "not like"
    REGEX = "regex"


class VectorSearchOperator(StrEnum):
    """Supported operators for vector search filters."""

    IN = "in"
    NOT_IN = "not in"


class ScalarSearchSpec(TypedDict):
    """Specification for a scalar-based search filter."""

    parameter: str
    operator: ScalarSearchOperator
    value: str | int


class VectorSearchSpec(TypedDict):
    """Specification for a vector-based search filter."""

    parameter: str
    operator: VectorSearchOperator
    values: list[str] | list[int]


SearchSpec = ScalarSearchSpec | VectorSearchSpec


class SortDirection(StrEnum):
    """Supported directions for sort clauses."""

    ASC = "asc"
    DESC = "desc"


# TODO: TypedDict vs pydantic?
class SortSpec(TypedDict):
    """Specification for a sort field and direction."""

    parameter: str
    direction: SortDirection


class SummaryParams(BaseModel):
    """Parameters for summarizing search results by grouping."""

    grouping: list[str]
    search: list[SearchSpec] = []
    # TODO: Add more validation


class SearchParams(BaseModel):
    """Request parameters for a search operation."""

    parameters: list[str] | None = None
    search: list[SearchSpec] = []
    sort: list[SortSpec] = []
    distinct: bool = False
    # TODO: Add more validation
