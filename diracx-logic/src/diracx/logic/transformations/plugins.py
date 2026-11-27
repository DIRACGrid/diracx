"""Loading of the Transformation System plugins (DX-ADR-006).

The ``Feeder`` and ``Packer`` contracts live in
``diracx.core.models.transformation`` so extensions can type-check their
plugins against them. This module loads a plugin by name, checks at runtime
what mypy cannot see through an entry point, and validates the arguments the
plugin is called with.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast, get_type_hints

from diracx.core.exceptions import ConfigurationError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.models.transformation import INJECTED, Feeder, Packer, PluginArgs

PluginT = TypeVar("PluginT")


@dataclass(frozen=True)
class PluginKind(Generic[PluginT]):
    """Static description of one extension point.

    Generic over the plugin's ``Protocol`` so ``load_plugin`` returns the
    right contract type for the kind it is given. The contract is that
    Protocol: the arguments parameter plus the core parameters. The runtime
    checks read both from it, so they cannot drift from what mypy checks.
    Injected dependencies are never part of it, since each plugin declares its
    own.

    Attributes:
        name: Kind of plugin, used in error messages and to name the arguments
            parameter (``<name>_args``).
        group: Entry-point group the plugins are registered under.
        protocol: The kind's contract, ``Feeder`` or ``Packer``.
    """

    name: str
    group: DiracEntryPoint
    protocol: type

    def __post_init__(self) -> None:
        if self.args_param not in self._protocol_params():
            raise TypeError(
                f"{self.protocol.__name__} has no {self.args_param!r} parameter"
            )

    def _protocol_params(self) -> frozenset[str]:
        """Return every parameter the Protocol declares.

        That is the arguments parameter plus the core parameters; injected
        dependencies never appear, since each plugin declares its own.
        """
        params = inspect.signature(self.protocol.__call__).parameters
        return frozenset(params) - {"self"}

    @property
    def args_param(self) -> str:
        """Name of the parameter that receives the arguments model."""
        return f"{self.name}_args"

    @property
    def core_params(self) -> frozenset[str]:
        """Parameters whose values the core computes at each call.

        The Protocol's parameters other than the arguments parameter, such as
        ``feeder_state`` or ``flush``: the same set for every plugin of a kind.
        """
        return self._protocol_params() - {self.args_param}


FEEDER_KIND: PluginKind[Feeder[Any]] = PluginKind(
    name="feeder", group=DiracEntryPoint.FEEDERS, protocol=Feeder
)
PACKER_KIND: PluginKind[Packer[Any]] = PluginKind(
    name="packer", group=DiracEntryPoint.PACKERS, protocol=Packer
)

_KEYWORD_KINDS = (
    inspect.Parameter.KEYWORD_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _check_contract(
    kind: PluginKind[Any], name: str, plugin: Callable[..., Any]
) -> None:
    """Raise if ``plugin`` cannot be called with exactly the contract's keywords.

    Every contract keyword must be accepted (a ``**kwargs`` accepts them all)
    and must not default to ``INJECTED``, since the core always passes it. Any
    other parameter must have a default since the core never passes it:
    ``INJECTED`` for what the core supplies, a plain value otherwise.
    """
    params = inspect.signature(plugin).parameters
    expected = kind.core_params | {kind.args_param}
    has_var_keyword = any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
    )
    missing = sorted(
        p
        for p in expected
        if not has_var_keyword
        and (p not in params or params[p].kind not in _KEYWORD_KINDS)
    )
    if missing:
        raise ConfigurationError(
            f"{kind.name.capitalize()} {name!r} does not accept the keyword "
            f"parameter(s) {', '.join(missing)} required by the {kind.name} contract"
        )
    marked = sorted(
        p for p in expected if p in params and params[p].default is INJECTED
    )
    if marked:
        raise ConfigurationError(
            f"{kind.name.capitalize()} {name!r} marks the contract parameter(s) "
            f"{', '.join(marked)} as INJECTED; the core always passes them"
        )
    unexpected = sorted(
        p.name
        for p in params.values()
        if p.name not in expected
        and p.default is inspect.Parameter.empty
        and p.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    )
    if unexpected:
        raise ConfigurationError(
            f"{kind.name.capitalize()} {name!r} requires the parameter(s) "
            f"{', '.join(unexpected)}, which the {kind.name} contract never passes; "
            "give each a default, INJECTED if the core should supply it"
        )


def injected_parameters(plugin: Callable[..., Any]) -> dict[str, type]:
    """Return the parameters of ``plugin`` that default to ``INJECTED``.

    Args:
        plugin: A loaded feeder or packer.

    Returns:
        Each injected parameter's name and the annotated type the core must
        supply an instance of.

    Raises:
        ConfigurationError: If an injected parameter is not keyword-capable or
            is not annotated with a class.
    """
    hints = get_type_hints(plugin)
    injected: dict[str, type] = {}
    for param in inspect.signature(plugin).parameters.values():
        if param.default is not INJECTED:
            continue
        annotation = hints.get(param.name)
        if param.kind not in _KEYWORD_KINDS or not isinstance(annotation, type):
            raise ConfigurationError(
                f"Injected parameter {param.name!r} of {plugin.__qualname__} must "
                "be a keyword parameter annotated with the class to inject"
            )
        injected[param.name] = annotation
    return injected


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
            contract's keyword parameters, it requires a parameter the contract
            never passes, an ``INJECTED`` parameter has no class annotation, or
            its arguments parameter is not annotated with a ``PluginArgs``
            subclass.
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
    injected_parameters(plugin)
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
