"""Custom CWL tool classes for DIRAC executor.

Provides DiracCommandLineTool (a CommandLineTool subclass that supports custom
path mappers) and dirac_make_tool (a factory for cwltool's construct_tool_object).
"""
# ruff: noqa: N803

from __future__ import annotations

import logging

from cwltool.command_line_tool import CommandLineTool
from cwltool.context import LoadingContext
from cwltool.pathmapper import PathMapper
from cwltool.process import Process
from cwltool.utils import CWLObjectType
from cwltool.workflow import default_make_tool
from ruamel.yaml.comments import CommentedMap

from .pathmapper import DiracPathMapper

logger = logging.getLogger("dirac-cwl-runner")


class DiracCommandLineTool(CommandLineTool):
    """CommandLineTool that uses DiracPathMapper for LFN resolution.

    Overrides make_path_mapper to create a DiracPathMapper when a replica_map
    is available on the runtimeContext (set by DiracExecutor.run_jobs),
    falling back to the default PathMapper otherwise.
    """

    # NOTE: cwltool's base method is @staticmethod, but overriding as an
    # instance method matters under mypyc-compiled cwltool: instance dispatch
    # goes through the MRO descriptor protocol, while @staticmethod calls on
    # `self` may be inlined as a direct C call to the base implementation.
    # Toil uses the same instance-method pattern in src/toil/cwl/cwltoil.py.
    def make_path_mapper(
        self,
        reffiles: list[CWLObjectType],
        stagedir: str,
        runtimeContext,
        separateDirs: bool,
    ) -> PathMapper:
        """Create a PathMapper, using DiracPathMapper when a replica map is available."""
        replica_map = getattr(runtimeContext, "replica_map", None)
        if replica_map is not None:
            return DiracPathMapper(
                reffiles,
                runtimeContext.basedir,
                stagedir,
                separateDirs,
                replica_map=replica_map,
            )
        return PathMapper(reffiles, runtimeContext.basedir, stagedir, separateDirs)


def dirac_make_tool(
    toolpath_object: CommentedMap, loadingContext: LoadingContext
) -> Process:
    """Create a CWL tool instance, using DiracCommandLineTool for CommandLineTools.

    Delegate to cwltool's default_make_tool for everything else (Workflow,
    ExpressionTool, etc.).
    """
    if (
        isinstance(toolpath_object, dict)
        and toolpath_object.get("class") == "CommandLineTool"
    ):
        return DiracCommandLineTool(toolpath_object, loadingContext)
    return default_make_tool(toolpath_object, loadingContext)
