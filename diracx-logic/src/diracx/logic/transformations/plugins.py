"""Contracts of the Transformation System plugins (DX-ADR-006).

Feeders and packers are plain async generator functions discovered through
entry points. Each kind fixes a set of keyword parameters (its contract) and
takes its arguments as a ``PluginArgs`` model read from a JSON column. This
module writes those contracts down as ``Protocol``s, loads a plugin by name and
checks it against its contract, and validates the arguments it is called with.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar, cast, get_type_hints

from pydantic import ConfigDict, JsonValue

from diracx.core.exceptions import ConfigurationError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.models._base import BaseModel
from diracx.core.models.transformation import (
    FeederProposal,
    InputStatus,
    PackerInput,
    PackerProposal,
)


class PluginArgs(BaseModel):
    """Base class of every plugin's arguments model.

    Arguments come from a JSON column written at submission and rewritten by
    hooks, so an unknown key is an error rather than silently ignored: a typo
    must not fall back to a default. Instances are immutable so a plugin cannot
    alter the arguments it was given.
    """

    model_config = ConfigDict(
        use_attribute_docstrings=True, extra="forbid", frozen=True
    )


# Contravariant: a plugin accepting any ``PluginArgs`` can stand in for one
# that expects a specific subclass, not the other way round.
ArgsT_contra = TypeVar("ArgsT_contra", bound=PluginArgs, contravariant=True)


class Feeder(Protocol[ArgsT_contra]):
    """Signature every feeder must have (DX-ADR-006)."""

    def __call__(
        self,
        *,
        feeder_args: ArgsT_contra,
        feeder_state: dict[str, JsonValue],
        scouting: bool,
    ) -> AsyncIterator[FeederProposal]:
        """Yield inputs, bookmark updates and an exhaustion signal.

        Args:
            feeder_args: The transformation's validated ``FeederArgs``.
            feeder_state: The bookmark last stored with ``SetFeederState``.
            scouting: Whether the workgraph is in its scouting phase.
        """
        ...


class Packer(Protocol[ArgsT_contra]):
    """Signature every packer must have (DX-ADR-006)."""

    def __call__(
        self,
        *,
        packer_args: ArgsT_contra,
        inputs: AsyncIterator[PackerInput],
        flush: bool,
        feeder_active: bool,
        input_overview: dict[InputStatus, int],
    ) -> AsyncIterator[PackerProposal]:
        """Yield parcels, rejected inputs and delayed inputs.

        Args:
            packer_args: The transformation's validated ``PackerArgs``.
            inputs: The claimed ``Unassigned`` inputs.
            flush: Whether to pack groups that would otherwise be held back.
            feeder_active: Whether more inputs may still arrive.
            input_overview: Number of the transformation's inputs per status.
        """
        ...


PluginT = TypeVar("PluginT")


@dataclass(frozen=True)
class PluginKind(Generic[PluginT]):
    """Static description of one extension point.

    Generic over the plugin's ``Protocol`` so ``load_plugin`` returns the
    right contract type for the kind it is given.

    Attributes:
        name: Human-readable kind, used in error messages.
        group: Entry-point group the plugins are registered under.
        args_param: Name of the parameter that receives the arguments model.
        contract_params: Other keyword parameters the core always passes.
    """

    name: str
    group: DiracEntryPoint
    args_param: str
    contract_params: frozenset[str]


FEEDER_KIND: PluginKind[Feeder[Any]] = PluginKind(
    name="feeder",
    group=DiracEntryPoint.FEEDERS,
    args_param="feeder_args",
    contract_params=frozenset({"feeder_state", "scouting"}),
)
PACKER_KIND: PluginKind[Packer[Any]] = PluginKind(
    name="packer",
    group=DiracEntryPoint.PACKERS,
    args_param="packer_args",
    contract_params=frozenset({"inputs", "flush", "feeder_active", "input_overview"}),
)

_KEYWORD_KINDS = (
    inspect.Parameter.KEYWORD_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _check_contract(
    kind: PluginKind[Any], name: str, plugin: Callable[..., Any]
) -> None:
    """Raise if ``plugin`` cannot be called with the contract's keywords."""
    params = inspect.signature(plugin).parameters
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return
    missing = sorted(
        p
        for p in kind.contract_params | {kind.args_param}
        if p not in params or params[p].kind not in _KEYWORD_KINDS
    )
    if missing:
        raise ConfigurationError(
            f"{kind.name.capitalize()} {name!r} does not accept the keyword "
            f"parameter(s) {', '.join(missing)} required by the {kind.name} contract"
        )


def load_plugin(
    kind: PluginKind[PluginT], name: str
) -> tuple[PluginT, type[PluginArgs]]:
    """Find a plugin and the model of its arguments.

    The highest priority extension registering ``name`` under the kind's
    entry-point group wins. The arguments model is the annotation of the
    plugin's ``kind.args_param`` parameter.

    Args:
        kind: Extension point to load from (``FEEDER_KIND``, ``PACKER_KIND``).
        name: Entry-point name stored in the transformation row.

    Returns:
        The plugin, typed as the kind's contract (``Feeder`` or ``Packer``),
        and its arguments model.

    Raises:
        ConfigurationError: If nothing is registered under ``name``, the plugin
            is not an async generator function, it does not accept the
            contract's keyword parameters, or its arguments parameter is not
            annotated with a ``PluginArgs`` subclass.
    """
    entry_points = select_from_extension(group=kind.group, name=name)
    if not entry_points:
        raise ConfigurationError(
            f"No {kind.name} named {name!r} is registered under {kind.group.value!r}"
        )
    plugin = entry_points[0].load()
    if not inspect.isasyncgenfunction(plugin):
        raise ConfigurationError(
            f"{kind.name.capitalize()} {name!r} must be an async generator function"
        )
    _check_contract(kind, name, plugin)
    args_model = get_type_hints(plugin).get(kind.args_param)
    if not (isinstance(args_model, type) and issubclass(args_model, PluginArgs)):
        raise ConfigurationError(
            f"{kind.name.capitalize()} {name!r} must annotate {kind.args_param} "
            "with a PluginArgs subclass"
        )
    # The runtime checks above stand in for what mypy cannot see through an
    # entry point.
    return cast(PluginT, plugin), args_model


def validate_plugin_args(
    kind: PluginKind[Any], name: str, raw: Mapping[str, Any] | None
) -> PluginArgs:
    """Validate a plugin's stored arguments against its model.

    Meant for every writer and reader of an ``…Args`` column: submission,
    hooks that rewrite the arguments, and the sweep that calls the plugin.

    Args:
        kind: Extension point the plugin belongs to.
        name: Entry-point name of the plugin.
        raw: Contents of the arguments column; ``None`` means no arguments.

    Returns:
        The validated, immutable arguments.

    Raises:
        ConfigurationError: If the plugin cannot be loaded.
        pydantic.ValidationError: If ``raw`` does not match the model.
    """
    _, args_model = load_plugin(kind, name)
    return args_model.model_validate(raw or {})
