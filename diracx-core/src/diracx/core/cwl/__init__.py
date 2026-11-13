"""Common Workflow Language utilities shared by the client and the services."""

from __future__ import annotations

__all__ = [
    "Component",
    "NotSelfContainedError",
    "check_self_contained",
    "decompose",
    "entry_identity",
    "format_reference",
    "subgraph",
]

from .identity import (
    Component,
    NotSelfContainedError,
    check_self_contained,
    decompose,
    entry_identity,
    format_reference,
    subgraph,
)
