"""DIRAC CWL Executor package.

This package provides custom executors for running CWL workflows with DIRAC-specific
functionality, including replica map management.
"""

# Install the mypyc compatibility hook before any cwltool import.
# executor.py imports cwltool.executors which transitively imports
# cwltool.command_line_tool — the hook must be in place first.
from __future__ import annotations

from ._mypyc_compat import install as _install_pure_python_hook

_install_pure_python_hook()

from .executor import DiracExecutor, dirac_executor_factory  # noqa: E402

__all__ = ["DiracExecutor", "dirac_executor_factory"]
