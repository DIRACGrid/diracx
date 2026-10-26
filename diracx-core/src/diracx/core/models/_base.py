"""Base class shared by the DiracX API models."""

from __future__ import annotations

from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict


class BaseModel(_BaseModel):
    """Base class for models exposed through the DiracX API.

    Field docstrings are used as the field descriptions in the OpenAPI schema.
    """

    model_config = ConfigDict(use_attribute_docstrings=True)
