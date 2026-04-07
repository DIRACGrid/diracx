"""Ensure the mypyc compatibility patch is installed before cwltool imports.

cwltool is mypyc-compiled. The executor's __init__.py must install the
_PurePythonFinder meta path hook BEFORE any cwltool import, so that
cwltool.command_line_tool loads from .py instead of .so. This enables
subclassing CommandLineTool (DiracCommandLineTool).

If this test fails, the CWL executor will break at runtime due to
mypyc class subclassing errors.
"""

from __future__ import annotations

import subprocess
import sys


def test_mypyc_patch_installed_before_cwltool():
    """Importing diracx.cli.executor must install the mypyc patch before cwltool loads.

    Runs in a subprocess to guarantee a clean import state.
    """
    test_script = (
        "import sys\n"
        "import diracx.cli.executor\n"
        "# The mypyc patch must be active\n"
        "finder_names = [type(f).__name__ for f in sys.meta_path]\n"
        "assert '_PurePythonFinder' in finder_names, (\n"
        "    f'_PurePythonFinder not in sys.meta_path: {finder_names}'\n"
        ")\n"
        "# cwltool must be loaded (the executor imports it)\n"
        "cwl_mods = [m for m in sys.modules if m.startswith('cwltool')]\n"
        "assert cwl_mods, 'cwltool was not loaded by diracx.cli.executor'\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", test_script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"mypyc compatibility patch not properly installed.\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
