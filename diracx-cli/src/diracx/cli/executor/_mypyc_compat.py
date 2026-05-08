"""Force pure-Python import for mypyc-compiled cwltool modules.

When cwltool is installed from a binary wheel that bundles mypyc-compiled
extensions, ``CommandLineTool.job()`` resolves ``self.make_path_mapper()`` as
a direct C call, bypassing Python's MRO. Subclass overrides such as
:class:`diracx.cli.executor.tool.DiracCommandLineTool` then silently lose
effect.

This module installs a :mod:`sys.meta_path` finder that forces ``.py`` source
loading for the specific cwltool submodules where we need subclass dispatch
to work. Pure-Python cwltool installs (e.g. conda-forge, sdist installs) need
nothing — the hook is skipped entirely.

Must be invoked before any ``cwltool`` submodule import.
"""

from __future__ import annotations

import importlib.abc
import importlib.util
import logging
import os
import sys
from typing import NamedTuple

logger = logging.getLogger("dirac-cwl-runner")

# cwltool submodules that must be loaded from .py for subclass dispatch.
_FORCE_PURE_PYTHON: frozenset[str] = frozenset({"cwltool.command_line_tool"})

# Extension suffixes used by mypyc-compiled wheels.
_COMPILED_SUFFIXES: tuple[str, ...] = (".so", ".pyd")


class _PurePythonFinder(importlib.abc.MetaPathFinder):
    """Meta-path finder that forces ``.py`` over compiled extensions."""

    def find_spec(self, fullname, path, target=None):
        del target  # required by the MetaPathFinder protocol; unused here
        if fullname not in _FORCE_PURE_PYTHON or path is None:
            return None

        module_name = fullname.rsplit(".", 1)[-1]
        for search_path in path:
            py_file = os.path.join(search_path, module_name + ".py")
            if os.path.isfile(py_file):
                return importlib.util.spec_from_file_location(
                    fullname, py_file, submodule_search_locations=None
                )

        # The hook was installed because a compiled extension exists; if the
        # .py source has gone missing, falling back to the default loader would
        # silently re-introduce the dispatch bug. Raise instead.
        raise ImportError(
            f"_PurePythonFinder: no .py source for {fullname} in {list(path)}; "
            f"cannot restore Python dispatch for cwltool subclass overrides"
        )


class _Layout(NamedTuple):
    cwltool_dir: str | None
    has_compiled: bool
    has_source: bool


def _inspect_cwltool() -> _Layout:
    """Inspect the installed cwltool layout without importing submodules."""
    spec = importlib.util.find_spec("cwltool")
    if spec is None or not spec.submodule_search_locations:
        return _Layout(None, False, False)

    cwltool_dir = next(iter(spec.submodule_search_locations))
    try:
        entries = os.listdir(cwltool_dir)
    except OSError:
        return _Layout(cwltool_dir, False, False)

    has_compiled = False
    has_source = False
    for fullname in _FORCE_PURE_PYTHON:
        module_name = fullname.rsplit(".", 1)[-1]
        if (module_name + ".py") in entries:
            has_source = True
        if any(
            entry.startswith(module_name + ".") and entry.endswith(_COMPILED_SUFFIXES)
            for entry in entries
        ):
            has_compiled = True

    return _Layout(cwltool_dir, has_compiled, has_source)


def install() -> None:
    """Install the pure-Python import hook iff a mypyc-compiled cwltool is present.

    Branches:

    * cwltool not on sys.path → no-op (the executor import will fail later
      with a clearer error than anything we'd raise here).
    * cwltool is pure-Python → no-op; subclass dispatch already works.
    * cwltool is mypyc-compiled with .py sources alongside → install finder.
    * cwltool is mypyc-compiled without .py sources → raise; subclass overrides
      cannot be restored, and silently running broken dispatch is worse.

    Logs the chosen branch so the runtime mode is visible at startup.
    """
    layout = _inspect_cwltool()

    if layout.cwltool_dir is None:
        logger.debug("cwltool not found on sys.path; skipping mypyc compat hook")
        return

    if layout.has_compiled and not layout.has_source:
        raise RuntimeError(
            f"cwltool at {layout.cwltool_dir} is mypyc-compiled but ships no "
            f".py source for {sorted(_FORCE_PURE_PYTHON)}; subclass overrides "
            f"of make_path_mapper cannot be restored. Install a wheel that "
            f"includes the .py sources, or a pure-Python build of cwltool."
        )

    if not layout.has_compiled:
        logger.info(
            "cwltool is pure-Python (%s); mypyc compat hook not needed",
            layout.cwltool_dir,
        )
        return

    if not any(isinstance(f, _PurePythonFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _PurePythonFinder())

    # Logging may not be configured yet (this runs at package import time);
    # also write to stderr so the runtime mode is always visible.
    msg = (
        f"[diracx] mypyc-compiled cwltool detected at {layout.cwltool_dir}; "
        f"forcing pure-Python import for {sorted(_FORCE_PURE_PYTHON)}"
    )
    logger.info(msg)
    print(msg, file=sys.stderr)
