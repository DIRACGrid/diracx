import os
import sys
import importlib

from importlib.abc import MetaPathFinder
from importlib.machinery import SourceFileLoader, ModuleSpec


class DiracxLoader(SourceFileLoader):

    def create_module(self, spec):
        if spec.name in sys.modules:
            return sys.modules[spec.name]

    def exec_module(self, module): ...


class DiracxPathFinder(MetaPathFinder):
    """
    This MetaPathFinder modifies the import such that the patches
    from vanila diracx are looked at first.
    """

    diracx_extensions = os.environ.get("DIRACX_EXTENSIONS", "diracx").split(",")

    @classmethod
    def find_spec(cls, fullname, path, target=None):
        for i, extension in enumerate(cls.diracx_extensions, start=1):
            # If we are trying to load the patch from an extension
            # make sure it does not exist in the lower levels first
            if any(
                [
                    fullname.startswith(prefix)
                    for prefix in [
                        f"{extension}.client.generated.operations._patch",
                        f"{extension}.client.generated.models._patch",
                        f"{extension}.client.generated.aio.operations._patch",
                    ]
                ]
            ):
                for lower_extension in cls.diracx_extensions[i:][::-1]:
                    try:
                        patched_name = fullname.replace(extension, lower_extension)
                        overwritten = importlib.util.find_spec(patched_name)

                        spec = ModuleSpec(
                            patched_name, DiracxLoader(patched_name, path)
                        )
                        return spec
                        if patched_name in sys.modules:
                            return sys.modules[patched_name].__spec__

                        overwritten = importlib.util.find_spec(patched_name)

                        # overwritten = spec_from_loader(patched_name, DiracxLoader(filepath))
                        return overwritten
                    except Exception as e:
                        pass

        return None


def initialize_client():

    # insert a DiracxPathFinder instance at the start of the meta_path list
    if not isinstance(sys.meta_path[0], DiracxPathFinder):

        sys.meta_path.insert(0, DiracxPathFinder())

        # Reload all the client module that could potentially have been
        # already loaded
        # This was needed when the generated code was at the top
        # level of the module.
        # In principle, this is not needed anymore so I comment it out,
        # but in case it ends up being needed, I keep it there, as it is rather
        # tricky
        # importlib.invalidate_caches()
        # diracx_extensions = os.environ.get("DIRACX_EXTENSIONS", "diracx").split(",")
        # for top_module in diracx_extensions:
        #     for module_name, module in sys.modules.copy().items():
        #         if (
        #             (f"{top_module}.client" in module_name)
        #             and module_name
        #             not in (
        #                 f"{top_module}.client.generated",
        #                 f"{top_module}.client.generated._patch",
        #             )
        #             and "_patch" in module_name
        #         ):
        #             importlib.reload(module)
