"""Enhanced submission models for DIRAC CWL integration."""

from __future__ import annotations

from typing import Any, Optional

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    Workflow,
)
from pydantic import BaseModel, ConfigDict, field_serializer, model_validator

from diracx.core.models.cwl import JobHint

# -----------------------------------------------------------------------------
# Job models
# -----------------------------------------------------------------------------


class JobInputModel(BaseModel):
    """Input data and sandbox files for a job execution."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sandbox: list[str] | None
    cwl: dict[str, Any]

    @field_serializer("cwl")
    def serialize_cwl(self, value):
        """Serialize CWL object to dictionary.

        :param value: CWL object to serialize.
        :return: Serialized CWL dictionary.
        """
        return save(value)


class BaseJobModel(BaseModel):
    """Base class for Job definition."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool

    @field_serializer("task")
    def serialize_task(self, value):
        """Serialize CWL task object to dictionary.

        :param value: CWL task object to serialize.
        :return: Serialized task dictionary.
        :raises TypeError: If value is not a valid CWL task type.
        """
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

    @model_validator(mode="before")
    @classmethod
    def validate_hints(cls, values):
        """Validate dirac:Job hint in the task.

        :param values: Model values dictionary.
        :return: Validated values dictionary.
        """
        task = values.get("task")
        JobHint.from_cwl(task)
        return values


class JobSubmissionModel(BaseJobModel):
    """Job definition sent to the router."""

    inputs: list[JobInputModel] | None = None


class JobModel(BaseJobModel):
    """Job definition sent to the job wrapper."""

    input: Optional[JobInputModel] = None
