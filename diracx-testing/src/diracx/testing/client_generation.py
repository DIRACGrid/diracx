from __future__ import annotations

import ast
import importlib.util
import subprocess
from pathlib import Path

import git


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


def make_patch(patch_path, parts, object_names, extension_name):
    patch_module = ".".join(["client", "_patches"] + list(parts))

    lines = ["from __future__ import annotations", "", "__all__ = ["]
    lines += [f'    "{name}",' for name in object_names]
    lines += ["]", ""]
    for name, module in object_names.items():
        gen_module = ".".join([extension_name, "client", "_generated"] + list(parts))
        if module:
            gen_module += "." + module
        lines += [
            f"from {gen_module} import (",
            f"    {name} as _{name}",
            ")",
        ]
        lines += [
            "try:",
            f"    from diracx.{patch_module} import (  # type: ignore[attr-defined]",
            f"        {name} as _{name}Patch",
            "    )",
            "except ImportError:",
            "",
            f"    class _{name}Patch:  # type: ignore[no-redef]",
            "        pass",
            "",
        ]
        if extension_name != "diracx":
            lines += [
                "try:",
                f"    from {extension_name}.{patch_module} import (  # type: ignore[attr-defined]",
                f"        {name} as _{name}PatchExt",
                "    )",
                "except ImportError:",
                "",
                f"    class _{name}PatchExt:  # type: ignore[no-redef]",
                "        pass",
                "",
            ]

    lines += [""]
    for name in object_names:
        subclasses = [f"_{name}Patch", f"_{name}"]
        if extension_name != "diracx":
            subclasses = [f"_{name}PatchExt"] + subclasses
        lines += [
            f"class {name}({', '.join(subclasses)}):",
            "    pass",
            "",
        ]
    lines += [
        "",
        "def patch_sdk():",
        "    pass",
    ]
    if parts == ("models",):
        spec = importlib.util.find_spec(f"diracx.{patch_module}")
        if spec is None or spec.origin is None:
            raise ImportError(f"Cannot locate diracx.{patch_module} package")
        missing = set(extract_static_all(Path(spec.origin))) - set(object_names)
        missing_formatted = "\n".join(f'            "{name}",' for name in missing)

        # Add any extra models which are not in the generated code and therefore
        # evaded the patching process
        # TODO: Does this need to also support extensions?
        lines += [
            "",
            "from typing import TYPE_CHECKING",
            "from diracx.client._patches.models import (",
        ]
        lines += [f"    {name}," for name in missing]
        lines += [
            ")",
            "",
        ]

        # Workaround for https://github.com/python/mypy/issues/15300
        init_path = patch_path.parent / "__init__.py"
        with init_path.open("a") as fh:
            fh.write(
                "if TYPE_CHECKING:\n"
                "    __all__.extend(\n"
                "        [\n"
                f"{missing_formatted}"
                "        ]\n"
                "    )\n"
            )
        lines += [
            "if TYPE_CHECKING:",
            "    __all__.extend(\n"
            "        [\n"
            f"{missing_formatted}"
            "        ]\n"
            "    )\n"
            "else:",
            "    from diracx.client._patches.models import __all__ as _patch_all",
            "",
            "    __all__.extend(_patch_all)",
            "    __all__ = sorted(set(__all__))",
        ]
    patch_path.write_text("\n".join(lines) + "\n")


def regenerate_client(openapi_spec: Path, client_root: Path):
    """Regenerate the AutoREST client and run pre-commit checks on it.

    This test is skipped by default, and can be enabled by passing
    --regenerate-client to pytest. It is intended to be run manually
    when the API changes.

    The reason this is a test is that it is the only way to get access to the
    test_client fixture, which is required to get the OpenAPI spec.

    WARNING: This test will modify the source code of the client!
    """
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

    for path in generated_dir.rglob("__init__.py"):
        objects = extract_static_all(path)
        # Enums cannot be extended, so we don't need to patch them
        objects = {
            name: module for name, module in objects.items() if module != "_enums"
        }
        parts = path.parent.relative_to(generated_dir).parts
        make_patch(path.parent / "_patch.py", parts, objects, extension_name)

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
