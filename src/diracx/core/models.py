from __future__ import annotations

from enum import Enum
from typing import Literal, TypedDict


class ScalarSearchOperator(str, Enum):
    EQUAL = "eq"
    NOT_EQUAL = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    LIKE = "like"


class VectorSearchOperator(str, Enum):
    IN = "in"
    NOT_IN = "not in"


# TODO: TypedDict vs pydnatic?
class SortSpec(TypedDict):
    parameter: str
    direction: Literal["asc"] | Literal["dsc"]


class ScalarSearchSpec(TypedDict):
    parameter: str
    operator: ScalarSearchOperator
    value: str


class VectorSearchSpec(TypedDict):
    parameter: str
    operator: VectorSearchOperator
    values: list[str]


SearchSpec = ScalarSearchSpec | VectorSearchSpec
