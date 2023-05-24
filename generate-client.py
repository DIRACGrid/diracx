#!/usr/bin/env python
import subprocess
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

import diracx.routers


def main():
    c = TestClient(diracx.routers.app)
    r = c.get("/openapi.json")
    r.raise_for_status()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        openapi_spec = tmpdir / "openapi.json"
        openapi_spec.write_text(r.text)

        output_folder = Path(__file__).parent / "src" / "diracx"

        cmd = [
            "autorest",
            "--python",
            f"--input-file={openapi_spec}",
            "--models-mode=msrest",
            "--namespace=client",
            f"--output-folder={output_folder}",
        ]
        # This is required to be able to work offline
        cmd += ["--use=@autorest/python@6.4.11"]
        subprocess.run(cmd, check=True)

    cmd = ["pre-commit", "run", "--all-files"]
    print("Running pre-commit...")
    subprocess.run(cmd, check=False, cwd=Path(__file__).parent)
    print("Re-running pre-commit...")
    subprocess.run(cmd, check=True, cwd=Path(__file__).parent)


if __name__ == "__main__":
    main()
