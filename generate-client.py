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
        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
