"""Ensure cwltool is never imported by the job wrapper module.

cwltool is mypyc-compiled and must be patched (via _mypyc_compat)
before first import. The job wrapper runs cwltool via subprocess
(dirac-cwl-run), so it must never import cwltool directly or
transitively. If this test fails, the CWL executor will break at
runtime due to mypyc class subclassing errors.
"""

from __future__ import annotations

import subprocess
import sys


def test_cwltool_not_imported_before_executor_patch():
    """Importing diracx.api must not load cwltool before the executor's mypyc patch.

    The executor's __init__.py applies the mypyc compatibility patch before
    importing cwltool. If any other module in diracx.api imports cwltool
    first, the patch arrives too late and the executor will fail with
    mypyc class subclassing errors.

    Runs in a subprocess to guarantee a clean import state.
    """
    test_script = (
        "import sys\n"
        "import diracx.api\n"
        "mods = [m for m in sys.modules if m.startswith('cwltool')]\n"
        "assert not mods, f'Importing diracx.api loaded cwltool: {mods}'\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", test_script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"cwltool was imported before the mypyc compatibility patch.\n"
        f"This will break the CWL executor at runtime.\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
