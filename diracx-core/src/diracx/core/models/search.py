"""Typed models for constructing job search and sorting requests."""

from __future__ import annotations

from enum import StrEnum

from pydantic import ConfigDict, with_config
from typing_extensions import TypedDict

from ._base import BaseModel


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


@with_config(ConfigDict(use_attribute_docstrings=True))
class ScalarSearchSpec(TypedDict):
    """Search condition applied to a scalar parameter."""

    parameter: str
    """Parameter to search."""
    operator: ScalarSearchOperator
    """Comparison operator to apply."""
    value: str | int
    """Value to compare with the parameter."""


@with_config(ConfigDict(use_attribute_docstrings=True))
class VectorSearchSpec(TypedDict):
    """Search condition applied to a collection of values."""

    parameter: str
    """Parameter to search."""
    operator: VectorSearchOperator
    """Collection comparison operator to apply."""
    values: list[str] | list[int]
    """Values to compare with the parameter."""


SearchSpec = ScalarSearchSpec | VectorSearchSpec


class SortDirection(StrEnum):
    """Directions in which search results can be sorted."""

    ASC = "asc"
    DESC = "desc"


# TODO: TypedDict vs pydantic?
@with_config(ConfigDict(use_attribute_docstrings=True))
class SortSpec(TypedDict):
    """Sort configuration for a search parameter."""

    parameter: str
    """Parameter by which to sort."""
    direction: SortDirection
    """Direction in which to sort the parameter."""


class SummaryParams(BaseModel):
    """Parameters for grouping and summarizing search results."""

    grouping: list[str]
    """Parameters used to group the search results."""
    search: list[SearchSpec] = []
    """Search conditions to apply before grouping."""
    # TODO: Add more validation


class SearchParams(BaseModel):
    """Parameters for searching, sorting, and selecting job results."""

    parameters: list[str] | None = None
    """Parameters to include in the search results."""
    search: list[SearchSpec] = []
    """Search conditions to apply."""
    sort: list[SortSpec] = []
    """Sort configurations for the results."""
    distinct: bool = False
    """Whether duplicate results should be removed."""
    # TODO: Add more validation
