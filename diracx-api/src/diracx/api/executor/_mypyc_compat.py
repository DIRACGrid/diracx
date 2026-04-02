"""Force pure Python import for specific mypyc-compiled cwltool modules.

mypyc compiles cwltool modules to .so files. The compiled CommandLineTool.job()
resolves self.make_path_mapper() as a direct C call, bypassing Python's MRO.
This prevents both monkey-patching and subclassing from working.

By forcing the .py source to load instead of .so, normal Python dispatch is
restored and subclass method overrides work correctly.
"""

from __future__ import annotations

import importlib.abc
import importlib.util
import logging
import os
import sys

logger = logging.getLogger("dirac-cwl-run")

# Modules that must be loaded from .py for method dispatch to work
_FORCE_PURE_PYTHON = frozenset({"cwltool.command_line_tool"})


class _PurePythonFinder(importlib.abc.MetaPathFinder):
    """Meta path finder that forces .py over .so for specific modules."""

    def find_spec(self, fullname, path, target=None):
        """Find a pure Python (.py) module spec, ignoring mypyc .so files."""
        if fullname not in _FORCE_PURE_PYTHON or path is None:
            return None

        # path is the parent package's __path__, provided by Python for
        # submodule imports (e.g. cwltool.__path__ for cwltool.command_line_tool)
        module_name = fullname.rsplit(".", 1)[-1]

        for search_path in path:
            py_file = os.path.join(search_path, module_name + ".py")
            if os.path.isfile(py_file):
                logger.debug(
                    "Forcing pure Python import: %s from %s", fullname, py_file
                )
                return importlib.util.spec_from_file_location(
                    fullname,
                    py_file,
                    submodule_search_locations=None,
                )
        return None


def install():
    """Install the import hook. Must be called before any cwltool import."""
    if not any(isinstance(f, _PurePythonFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _PurePythonFinder())
