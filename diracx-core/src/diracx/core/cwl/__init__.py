"""Common Workflow Language utilities shared by the client and the services."""

from __future__ import annotations

__all__ = [
    "DIRAC_NAMESPACE",
    "Component",
    "NotSelfContainedError",
    "check_self_contained",
    "decompose",
    "entry_identity",
    "format_reference",
    "split_dirac_hints",
    "subgraph",
]

from .hints import DIRAC_NAMESPACE, split_dirac_hints
from .identity import (
    Component,
    NotSelfContainedError,
    check_self_contained,
    decompose,
    entry_identity,
    format_reference,
    subgraph,
)
