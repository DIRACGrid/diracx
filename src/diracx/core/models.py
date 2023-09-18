from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal, TypedDict

from pydantic import BaseModel, Field

from diracx.core.utils import JobStatus


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


class JobStatusUpdate(BaseModel):
    status: JobStatus | None = Field(
        default=None,
        alias="Status",
    )
    minor_status: str | None = Field(
        default=None,
        alias="MinorStatus",
        serialization_alias="minorStatus",
    )
    application_status: str | None = Field(
        default=None,
        alias="ApplicationStatus",
        serialization_alias="applicationStatus",
    )
    status_source: str = Field(
        alias="StatusSource",
        default="Unknown",
    )


class LimitedJobStatusReturn(BaseModel):
    status: JobStatus = Field(alias="Status")
    minor_status: str = Field(alias="MinorStatus")
    application_status: str = Field(alias="ApplicationStatus")


class JobStatusReturn(LimitedJobStatusReturn):
    status_time: datetime = Field(alias="StatusTime")
    status_source: str = Field(alias="StatusSource")


class SetJobStatusReturn(BaseModel):
    status: JobStatus | None = Field(alias="Status")
    minor_status: str | None = Field(alias="MinorStatus")
    application_status: str | None = Field(alias="ApplicationStatus")
    heartbeat_time: datetime | None = Field(alias="HeartBeatTime")
    start_exec_time: datetime | None = Field(alias="StartExecTime")
    end_exec_time: datetime | None = Field(alias="EndExecTime")
    last_update_time: datetime | None = Field(alias="LastUpdateTime")


SearchSpec = ScalarSearchSpec | VectorSearchSpec
