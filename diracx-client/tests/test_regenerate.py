import subprocess
from pathlib import Path

import git

import diracx.client


def test_regenerate_client(test_client, tmp_path):
    """Regenerate the AutoREST client and run pre-commit checks on it

    This test is skipped by default, and can be enabled by passing
    --regenerate-client to pytest. It is intended to be run manually
    when the API changes.

    The reason this is a test is that it is the only way to get access to the
    test_client fixture, which is required to get the OpenAPI spec.

    WARNING: This test will modify the source code of the client!
    """
    r = test_client.get("/api/openapi.json")
    r.raise_for_status()

    openapi_spec = tmp_path / "openapi.json"
    openapi_spec.write_text(r.text)

    output_folder = Path(diracx.client.__file__).parent.parent
    assert (output_folder / "client").is_dir()
    repo_root = output_folder.parent.parent.parent
    assert (repo_root / "diracx-client" / "src").is_dir()
    assert (repo_root / ".git").is_dir()
    repo = git.Repo(repo_root)
    if repo.is_dirty(path=repo_root / "src" / "diracx" / "client"):
        raise AssertionError(
            "Client is currently in a modified state, skipping regeneration"
        )

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
    subprocess.run(cmd, check=False, cwd=repo_root)
    print("Re-running pre-commit...")
    subprocess.run(cmd, check=True, cwd=repo_root)
    if repo.is_dirty(path=repo_root / "src" / "diracx" / "client"):
        raise AssertionError("Client was regenerated with changes")
