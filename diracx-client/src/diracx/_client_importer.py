"""Register a meta path finder to merger diracx.client with extensions.

This file is intended to be used with _diracx_client_importer.pth which causes
it to be registered as part of Python's startup process. This is needed as we
don't know if diracx.client or yourextenstion.client will be imported first and
resolving this ambiguity results in circular imports if this is handled within
the diracx.client module itself.
"""

from __future__ import annotations

import importlib.abc
import importlib.util
import sys
import types
from importlib.metadata import entry_points
from pathlib import Path


class DiracXPathFinder(importlib.abc.MetaPathFinder):
    """A meta path finder that aliases the diracx.client module to an extension.

    The DiracXPathFinder is responsible for aliasing the diracx.client module to
    an extension. This is done by returning a custom loader that aliases the
    module to the extension's module. The loader is responsible for loading the
    extension's module and setting its __name__ to the alias.

    The DiracXPathFinder also patches the module with the effect of the
    extension's _patch.py file. This is done by returning a custom loader that
    loads the extension's _patch.py file and applies the patches to the module.
    See DiracXPatchLoader for more information.
    """

    patched_modules = [
        "diracx.client._generated",
        "diracx.client._generated.operations",
        "diracx.client._generated.aio",
        "diracx.client._generated.aio.operations",
        "diracx.client._generated.models",
    ]
    public_modules = [
        "diracx.client.aio",
        "diracx.client.models",
        "diracx.client.sync",
    ]
    client_modules = ("diracx.client",)

    @classmethod
    def _build_cache(cls, fullname):
        if not hasattr(cls, "_extension_name_cache"):
            try:
                installed_extension = get_extension_name(fullname)
            except RetryableError:
                return None
            # If we have an extension, we need to update the module names to
            # include the extension's modules.
            if installed_extension not in [None, "diracx"]:
                cls.patched_modules += [
                    f"{installed_extension}.{module.split('.', 1)[1]}"
                    for module in cls.patched_modules
                ]
                cls.public_modules += [
                    f"{installed_extension}.{module.split('.', 1)[1]}"
                    for module in cls.public_modules
                ]
                cls.client_modules = ("diracx.client", f"{installed_extension}.client")
            cls._extension_name_cache = installed_extension
        return cls._extension_name_cache

    @classmethod
    def find_spec(cls, fullname, path, target=None):
        # We only want to intercept the .client modules, defer building the
        # cache until we're confident we need it.
        if not (".client." in fullname or fullname.endswith(".client")):
            return None
        installed_extension = cls._build_cache(fullname)
        if not fullname.startswith(cls.client_modules):
            return None
        if not installed_extension:
            raise ClientDefinitionError("DiracX is not installed.")
        return cls._find_spec(fullname, installed_extension)

    @classmethod
    def _find_spec(cls, fullname, installed_extension):
        generated_dirs = (
            "diracx.client._generated",
            f"{installed_extension}.client._generated",
        )
        if fullname.startswith(generated_dirs) and not fullname.endswith("._patch"):
            # The auto-generated files should always come from the installed extension
            new_name = f"{installed_extension}.{fullname.split('.', 1)[1]}"

        elif fullname in cls.public_modules:
            # Override the public modules of the client
            new_name = f"{installed_extension}.{fullname.split('.', 1)[1]}"

        elif fullname.endswith("._patch") and installed_extension != "diracx":
            # When importing we always want to take patches from plain DiracX
            # The patches from extensions are applied in the DiracXPatchLoader
            diracx_name = f"diracx.{fullname.split('.', 1)[1]}"
            spec = find_spec_ignoring_meta_path_finder(diracx_name)
            spec.loader = RenamingModuleLoader(spec.loader, diracx_name, fullname)
            return spec

        else:
            # This module has nothing to do with DiracX so return None
            return None

        spec = find_spec_ignoring_meta_path_finder(new_name)
        spec.name = fullname
        if fullname in cls.patched_modules and installed_extension != "diracx":
            # Find the ModuleSpec for the extension's corresponding _patch.py
            patch_name = f"{new_name}._patch"
            try:
                patch_spec = find_spec_ignoring_meta_path_finder(patch_name)
            except ClientDefinitionError:
                return None
            else:
                if new_name.endswith(".models"):
                    raise NotImplementedError(
                        "We don't support patching models in extensions"
                    )
            spec.loader = DiracXPatchLoader(
                spec.loader, patch_spec, installed_extension, fullname, new_name
            )
        else:
            spec.loader = DiracXAliasLoader(
                spec.loader, installed_extension, fullname, new_name
            )
        return spec


class RenamingModuleLoader(importlib.abc.Loader):
    def __init__(self, loader, real_name, apparent_name):
        self._loader = loader
        self._real_name = real_name
        self._apparent_name = apparent_name
        self._already_execed = False

    def create_module(self, spec):
        """Create the module using the real module loader.

        Standard boilerplate for the Python import machinery.
        """
        return self._loader.create_module(spec)

    def exec_module(self, module):
        sys.modules[self._apparent_name] = sys.modules[self._real_name]
        self._loader.exec_module(module)


class DiracXAliasLoader(importlib.abc.Loader):
    """A loader that aliases a module to another module."""

    def __init__(self, original_loader, installed_extension, original_name, new_name):
        self.original_loader = original_loader
        self.installed_extension = installed_extension
        self.original_name = original_name
        self.new_name = new_name
        suffix = original_name.split(".", 1)[1]
        if original_name.startswith("diracx."):
            self.other_name = f"{self.installed_extension}.{suffix}"
        else:
            self.other_name = f"diracx.{suffix}"

    def create_module(self, spec):
        """Create the module using the real module loader.

        Standard boilerplate for the Python import machinery.
        """
        return self.original_loader.create_module(spec)

    def exec_module(self, module):
        """Import a module, overriding its __name__ with the alias."""
        # Make it so the __name__ attribute of the module is correct
        # This is needed to make the Loader work correctly
        module.__name__ = self.new_name
        # It is VERY important to populate the sys.modules cache with the alias
        # else you will end up with multiple copies of the module in memory.
        assert (
            self.installed_extension == "diracx" or self.other_name not in sys.modules
        )
        sys.modules[self.other_name] = module
        self.original_loader.exec_module(module)


class DiracXPatchLoader(DiracXAliasLoader):
    """A loader that patches a module with the effect of an extension's _patch.py.

    Ordinarily, with autorest the _patch.py file would be imported by the
    autogenerated module itself. This works well for the plain DiracX client
    but not for extensions. With extensions we end up having two copies of the
    autorest generated module, one in the extension and one in the plain DiracX.
    The extension's module is roughly a superset of the plain DiracX module,
    which means we can substitute the plain DiracX module with the extension's
    module at runtime. Hypothetically extensions could change the interface in
    incompatible ways however there should be no real reason to do this and it
    is not supported by DiracX. If a extension does wish to dramatically change
    the behavior of a route, they should duplicate it and make the server fail
    with a clear error if the original route is called.

    In extensions we want to be able to support the following method resolution
    order (assuming all possible patches are being applied):

    1. gubbins.client._generated.aio.operations._patch.MyOperationClass
    2. diracx.client._generated.aio.operations._patch.MyOperationClass
    3. gubbins.client._generated.aio.operations._operations.MyOperationClass
    4. builtins.object

    To achieve this, we:

    1. Alias diracx.client._generated.aio.operations._* to actually be
       gubbins.client._generated.aio.operations._*, with exception of the
       _patch.py file.
    2. When diracx.client._generated.aio.operations.__init__ is imported, it
       will import and apply patches using `from . import _patch`. (This is the
       standard autorest behavior). We use the DiracXPatchLoader to load the
       _patch.py file from the plain DiracX client
       (i.e. diracx.client._generated.aio.operations._patch).
    3. To overlay the patches from the extension the import of __init__ is
       intercepted (i.e. gubbins.client._generated.aio.operations.__init__) and
       the DiracXPatchLoader is used to load the _patch.py file from the
       extension. This is done by monkeypatching
       gubbins.client._generated.aio.operations._operations with the objects
       which are exposed by gubbins.client._generated.aio.operations.__init__.
    4. We then trigger the import of the real extension patch module (i.e.
       gubbins.client._generated.aio.operations._patch) which will import the
       classes from _operations which are already the combined classes from
       autorest and the _patch.py from the plain DiracX client.
    4. Finally we can restore the original objects in _operations and manually
       mutate the gubbins.client._generated.aio.operations.__module__ to overlay
       the contents of gubbins.client._generated.aio.operations._patch.
    """

    def __init__(
        self, original_loader, patch_spec, installed_extension, original_name, new_name
    ):
        super().__init__(original_loader, installed_extension, original_name, new_name)
        self.patch_spec = patch_spec

    def exec_module(self, module):
        """Import an autorest generated module with two layers of patches."""
        # Import the real module and set its __name__ to the alias. This will
        # then corrospond to the __init__.py file + the _patch.py file from
        # plain DiracX. When calling self.original_loader.exec_module it will:
        # 1. Import the real module (e.g. aio/__init__.py)
        # 2. Import any submodules (e.g. _operations)
        # 3. Import the patch module from plain DiracX (e.g. _patch.py,
        #    overriden by the DiracXPathFinder)
        # 4. Replace the submodules module's objects with anything in ._patch.__all__
        super().exec_module(module)

        # Find which module should be monkey patched
        if self.new_name.endswith(".operations"):
            to_monkey_patch_name = f"{self.new_name}._operations"
        elif self.new_name.endswith(".models"):
            to_monkey_patch_name = f"{self.new_name}._models"
        elif self.new_name.endswith((".aio", "._generated")):
            to_monkey_patch_name = f"{self.new_name}._client"
        else:
            raise NotImplementedError(f"Unknown module to patch: {self.new_name}")
        to_monkey_patch = importlib.import_module(to_monkey_patch_name)

        # Backup the original objects and replace them with the patched objects
        backups = {}
        for obj_name in module.__all__:
            obj = getattr(module, obj_name)
            if hasattr(to_monkey_patch, obj_name):
                backups[obj_name] = getattr(to_monkey_patch, obj_name)
            to_monkey_patch.__dict__[obj_name] = obj

        # Import the patch module so it takes the monkey patched objects and
        # adds it's own modifications.
        patch_module = self.patch_spec.loader.create_module(self.patch_spec)
        if patch_module is None:
            patch_module = types.ModuleType(self.patch_spec.name)
            patch_module.__file__ = self.patch_spec.origin
            patch_module.__loader__ = self.patch_spec.loader
            patch_module.__package__ = self.patch_spec.parent
        self.patch_spec.loader.exec_module(patch_module)

        # Restore the original objects into the monkey patched module
        for obj_name in module.__all__:
            if obj_name in backups:
                to_monkey_patch.__dict__[obj_name] = backups[obj_name]
            else:
                del to_monkey_patch.__dict__[obj_name]

        # Patch the real module with the effect of the extension's _patch.py
        for obj_name in patch_module.__all__:
            if obj_name not in module.__all__:
                module.__all__.append(obj_name)
            module.__dict__[obj_name] = patch_module.__dict__[obj_name]
        patch_module.__dict__["patch_sdk"]()


class ClientDefinitionError(Exception):
    """Raised when a diracx.client or a extension thereof is malformed."""


class RetryableError(Exception):
    """Raised when we're in an invalid state but we shouldn't fail catastrophically."""


def find_spec_ignoring_meta_path_finder(name, target=None):
    """Find a module's spec while ignoring the DiracXPathFinder.

    This is needed to avoid infinite recursion when finding a module's spec.
    The process is split into two steps:
    1. Find the path of the module using importlib.util.find_spec. This is
       needed to account for redefinition of the module's __path__ that is done
       by the DiracXAliasLoader.
    2. Iterate over all registered meta path finders and find the module's spec
       using the path found in step 1. Once the spec is found, return it.

    As the modules imported by the DiracXPathFinder are always expected to be
    present in extensions which provide a custom client, this function raises
    a ClientDefinitionError if the module is not found.
    """
    client_name = f"{name.split('.')[0]}.client"
    spec = importlib.util.find_spec(client_name)
    if spec is None or len(spec.submodule_search_locations) != 1:
        raise ClientDefinitionError(f"Failed to handle {client_name}.client: {spec}")
    path = Path(spec.submodule_search_locations[0])
    for submodule_name in name.split(".")[2:-1]:
        path /= submodule_name
    path = [str(path)]

    for finder in sys.meta_path:
        if isinstance(finder, DiracXPathFinder):
            continue
        spec = finder.find_spec(name, path, target)
        if spec is not None:
            return spec
    raise ClientDefinitionError(f"Could not find {name}")


def get_extension_name(fullname: str) -> str | None:
    """Yield extension module names in order of priority.

    NOTE: This function is duplicated in diracx._client_importer to avoid
    importing diracx in the MetaPathFinder as part of unrelated imports
    (e.g. http.client).
    """
    selected = entry_points().select(group="diracx")
    if selected is None:
        raise NotImplementedError(
            "No entry points found for group 'diracx'. Do you have it installed?"
        )
    extensions = set()
    for entry_point in selected.select(name="extension"):
        extensions.add(entry_point.module)
    if len(extensions) == 0:
        return None
    if len(extensions) == 1:
        return extensions.pop()
    if len(extensions) > 2:
        if fullname.startswith(tuple(extensions)):
            raise NotImplementedError(
                f"Expect to find either diracx or diracx + 1 extension: {extensions=}"
            )
        else:
            # We're in an invalid state however the user hasn't yet tried to use
            # DiracX so raise a RetryableError to avoid completely breaking
            # the current Python installation.
            raise RetryableError()
    installed_extension = min(extensions, key=lambda x: x == "diracx")
    # We need to check if the extension provides a .client module, ignoring
    # the DiracXPathFinder to avoid infinite recursion
    parent_spec = importlib.util.find_spec(installed_extension)
    if parent_spec is None:
        raise ClientDefinitionError(f"Failed to find spec for {installed_extension}!")
    client_name = f"{installed_extension}.client"
    for finder in sys.meta_path:
        if isinstance(finder, DiracXPathFinder):
            continue
        spec = finder.find_spec(client_name, parent_spec.submodule_search_locations)
        if spec is not None:
            return installed_extension
    # We didn't find a client module, so fall back to the default client
    return "diracx"


sys.meta_path.insert(0, DiracXPathFinder())
