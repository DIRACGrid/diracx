"""Ensure cwltool subclass dispatch works after the executor is imported.

cwltool may be installed as a mypyc-compiled binary wheel. When it is, the
executor's ``__init__.py`` must install a meta-path finder *before* any
``cwltool`` import, so that ``cwltool.command_line_tool`` loads from ``.py``
instead of ``.so`` — otherwise ``DiracCommandLineTool``'s subclass override
of ``make_path_mapper`` is bypassed by direct C dispatch.

The runtime invariant we care about is the outcome: after importing
``diracx.cli.executor``, ``cwltool.command_line_tool`` is backed by a ``.py``
file. Whether the finder was needed (mypyc-compiled cwltool) or not
(pure-Python cwltool) is an implementation detail of ``install()``.
"""

from __future__ import annotations

import subprocess
import sys


def test_cwltool_command_line_tool_loaded_as_pure_python():
    """After importing the executor, cwltool.command_line_tool must be a .py module.

    Runs in a subprocess to guarantee a clean import state. Covers both
    layouts:
    * pure-Python cwltool — the .py file is loaded natively;
    * mypyc-compiled cwltool — the meta-path finder must redirect to the .py
      sibling that the wheel ships alongside the .so.
    """
    test_script = (
        "import sys\n"
        "import diracx.cli.executor  # noqa: F401  triggers install()\n"
        "import cwltool.command_line_tool as clt\n"
        "assert clt.__file__ and clt.__file__.endswith('.py'), (\n"
        "    f'cwltool.command_line_tool was loaded from {clt.__file__!r}, '\n"
        "    f'not a .py source — subclass dispatch will be broken'\n"
        ")\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", test_script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"cwltool.command_line_tool not loaded from .py source.\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
