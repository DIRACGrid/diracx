"""Typed models for constructing job search and sorting requests."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel
from typing_extensions import TypedDict


class ScalarSearchOperator(StrEnum):
    """Operators for comparing scalar search values."""

    EQUAL = "eq"
    NOT_EQUAL = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    LIKE = "like"
    NOT_LIKE = "not like"
    REGEX = "regex"


class VectorSearchOperator(StrEnum):
    """Operators for comparing values against a collection."""

    IN = "in"
    NOT_IN = "not in"


class ScalarSearchSpec(TypedDict):
    """Search condition applied to a scalar parameter.

    Attributes:
        parameter: Parameter to search.
        operator: Comparison operator to apply.
        value: Value to compare with the parameter.
    """

    parameter: str
    operator: ScalarSearchOperator
    value: str | int


class VectorSearchSpec(TypedDict):
    """Search condition applied to a collection of values.

    Attributes:
        parameter: Parameter to search.
        operator: Collection comparison operator to apply.
        values: Values to compare with the parameter.
    """

    parameter: str
    operator: VectorSearchOperator
    values: list[str] | list[int]


SearchSpec = ScalarSearchSpec | VectorSearchSpec


class SortDirection(StrEnum):
    """Directions in which search results can be sorted."""

    ASC = "asc"
    DESC = "desc"


# TODO: TypedDict vs pydantic?
class SortSpec(TypedDict):
    """Sort configuration for a search parameter.

    Attributes:
        parameter: Parameter by which to sort.
        direction: Direction in which to sort the parameter.
    """

    parameter: str
    direction: SortDirection


class SummaryParams(BaseModel):
    """Parameters for grouping and summarizing search results.

    Attributes:
        grouping: Parameters used to group the search results.
        search: Search conditions to apply before grouping.
    """

    grouping: list[str]
    search: list[SearchSpec] = []
    # TODO: Add more validation


class SearchParams(BaseModel):
    """Parameters for searching, sorting, and selecting job results.

    Attributes:
        parameters: Parameters to include in the search results.
        search: Search conditions to apply.
        sort: Sort configurations for the results.
        distinct: Whether duplicate results should be removed.
    """

    parameters: list[str] | None = None
    search: list[SearchSpec] = []
    sort: list[SortSpec] = []
    distinct: bool = False
    # TODO: Add more validation
