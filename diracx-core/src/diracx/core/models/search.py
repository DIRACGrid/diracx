from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel
from typing_extensions import TypedDict


class ScalarSearchOperator(StrEnum):
    EQUAL = "eq"
    NOT_EQUAL = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    LIKE = "like"
    NOT_LIKE = "not like"
    REGEX = "regex"


class VectorSearchOperator(StrEnum):
    IN = "in"
    NOT_IN = "not in"


class ScalarSearchSpec(TypedDict):
    parameter: str
    operator: ScalarSearchOperator
    value: str | int


class VectorSearchSpec(TypedDict):
    parameter: str
    operator: VectorSearchOperator
    values: list[str] | list[int]


SearchSpec = ScalarSearchSpec | VectorSearchSpec


class SortDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


# TODO: TypedDict vs pydantic?
class SortSpec(TypedDict):
    parameter: str
    direction: SortDirection


class SummaryParams(BaseModel):
    grouping: list[str]
    search: list[SearchSpec] = []
    # TODO: Add more validation


class SearchParams(BaseModel):
    parameters: list[str] | None = None
    search: list[SearchSpec] = []
    sort: list[SortSpec] = []
    distinct: bool = False
    # TODO: Add more validation
