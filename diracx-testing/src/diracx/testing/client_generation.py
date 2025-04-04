from __future__ import annotations

__all__ = [
    "regenerate_client",
    "AUTOREST_VERSION",
]

import ast
import importlib.util
import subprocess
from pathlib import Path

import git

AUTOREST_VERSION = "6.13.7"


def extract_static_all(path):
    tree = ast.parse(path.read_text(), filename=path)

    name_to_module = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            # Skip wildcard imports (like 'from ... import *')
            for alias in node.names:
                if alias.name == "*":
                    continue
                # Use the alias if available, otherwise the original name.
                local_name = alias.asname if alias.asname else alias.name
                name_to_module[local_name] = node.module

    # Look for the first top-level assignment to __all__
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not (isinstance(target, ast.Name) and target.id == "__all__"):
                continue
            return {
                name: name_to_module.get(name) for name in ast.literal_eval(node.value)
            }
    raise NotImplementedError("__all__ not found")


def fixup_models_init(generated_dir, extension_name):
    """Workaround for https://github.com/python/mypy/issues/15300."""
    models_init_path = generated_dir / "models" / "__init__.py"
    # Enums cannot be extended, so we don't need to patch them
    object_names = {
        name
        for name, module in extract_static_all(models_init_path).items()
        if module != "_enums"
    }

    patch_module = "diracx.client._generated.models._patch"
    spec = importlib.util.find_spec(patch_module)
    if spec is None or spec.origin is None:
        raise ImportError(f"Cannot locate {patch_module} package")
    missing = set(extract_static_all(Path(spec.origin))) - set(object_names)
    missing_formatted = "\n".join(f'            "{name}",' for name in missing)

    with models_init_path.open("a") as fh:
        fh.write(
            "if TYPE_CHECKING:\n"
            "    __all__.extend(\n"
            "        [\n"
            f"{missing_formatted}"
            "        ]\n"
            "    )\n"
        )


def regenerate_client(openapi_spec: Path, client_module: str):
    """Regenerate the AutoREST client and run pre-commit checks on it.

    This test is skipped by default, and can be enabled by passing
    --regenerate-client to pytest. It is intended to be run manually
    when the API changes.

    The reason this is a test is that it is the only way to get access to the
    test_client fixture, which is required to get the OpenAPI spec.

    WARNING: This test will modify the source code of the client!
    """
    spec = importlib.util.find_spec(client_module)
    if spec is None:
        raise ImportError("Cannot locate client_module package")
    if spec.origin is None:
        raise ImportError(
            "Cannot locate client_module package, did you forget the __init__.py?"
        )
    client_root = Path(spec.origin).parent

    assert client_root.is_dir()
    assert client_root.name == "client"
    assert (client_root / "_generated").is_dir()
    extension_name = client_root.parent.name

    repo_root = client_root.parents[3]
    if extension_name == "gubbins":
        # Gubbins is special because it has a different structure due to being
        # in a subdirectory of diracx
        repo_root = repo_root.parents[1]
    assert (repo_root / ".git").is_dir()
    repo = git.Repo(repo_root)
    generated_dir = client_root / "_generated"
    if repo.is_dirty(path=generated_dir):
        raise AssertionError(
            "Client is currently in a modified state, skipping regeneration"
        )

    cmd = [
        "autorest",
        "--python",
        f"--input-file={openapi_spec}",
        "--models-mode=msrest",
        "--namespace=_generated",
        f"--output-folder={client_root}",
    ]

    # This is required to be able to work offline
    # TODO: if offline, find the version already installed
    # and use it
    # cmd += [f"--use=@autorest/python@{AUTOREST_VERSION}"]

    # ruff: disable=S603
    subprocess.run(cmd, check=True)

    if extension_name != "diracx":
        # For now we don't support extending the models in extensions. To make
        # this clear manually remove the automatically generated _patch.py file
        # and fixup the __init__.py file to use the diracx one.
        (generated_dir / "models" / "_patch.py").unlink()
        models_init_path = generated_dir / "models" / "__init__.py"
        models_init = models_init_path.read_text()
        assert models_init.count("from ._patch import") == 4
        models_init = models_init.replace(
            "from ._patch import",
            "from diracx.client._generated.models._patch import",
        )
        models_init_path.write_text(models_init)

    fixup_models_init(generated_dir, extension_name)

    cmd = ["pre-commit", "run", "--all-files"]
    print("Running pre-commit...")
    subprocess.run(cmd, check=False, cwd=repo_root)
    print("Re-running pre-commit...")
    proc = subprocess.run(cmd, check=False, cwd=repo_root)
    if proc.returncode == 0 and not repo.is_dirty(path=generated_dir):
        return
    # Show the diff to aid debugging
    print(repo.git.diff(generated_dir))
    if proc.returncode != 0:
        raise AssertionError("Pre-commit failed")
    raise AssertionError("Client was regenerated with changes")
