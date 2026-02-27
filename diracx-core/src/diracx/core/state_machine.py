"""Generic declarative state machine with typed StrEnum states.

Subclass and define ``states`` and ``transitions`` as class attributes.
Validation runs automatically at class creation via ``__init_subclass__``.

Example::

    class ProductionStatus(StrEnum):
        NEW = "New"
        ACTIVE = "Active"
        CLEANED = "Cleaned"


    class ProductionStateMachine(StateMachine[ProductionStatus]):
        states = ProductionStatus
        transitions = {
            ProductionStatus.NEW: [ProductionStatus.ACTIVE, ProductionStatus.CLEANED],
            ProductionStatus.ACTIVE: [ProductionStatus.CLEANED],
            ProductionStatus.CLEANED: [],
        }


    ProductionStateMachine.validate_transition(
        ProductionStatus.NEW, ProductionStatus.ACTIVE
    )  # OK
"""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar, Generic, TypeVar, cast

S = TypeVar("S", bound=StrEnum)


class StateMachine(Generic[S]):
    """Base class for declarative state machines."""

    states: ClassVar[type[StrEnum]]
    transitions: ClassVar[dict[StrEnum, list[StrEnum]]]

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Skip validation on intermediate classes that don't define both attrs
        if not hasattr(cls, "states") or not hasattr(cls, "transitions"):
            return
        # Every state must appear as a key in transitions
        for state in cls.states:
            if state not in cls.transitions:
                raise TypeError(
                    f"{cls.__name__}: state '{state}' missing from transitions"
                )
        # Every transition target must be a valid state
        for source, targets in cls.transitions.items():
            for target in targets:
                if target not in cls.states:
                    raise TypeError(
                        f"{cls.__name__}: transition target '{target}' "
                        f"from '{source}' is not a valid state"
                    )

    @classmethod
    def validate_transition(cls, current: S, proposed: S) -> None:
        """Raise ``ValueError`` if the transition is not allowed."""
        allowed = cls.transitions.get(current, [])
        if proposed not in allowed:
            raise ValueError(
                f"Cannot transition from '{current}' to '{proposed}'. "
                f"Allowed: {[str(s) for s in allowed]}"
            )

    @classmethod
    def get_valid_transitions(cls, current: S) -> list[S]:
        """Return valid next states from *current*."""
        return cast(list[S], list(cls.transitions.get(current, [])))

    @classmethod
    def is_terminal(cls, state: S) -> bool:
        """Return ``True`` if *state* has no outgoing transitions."""
        return len(cls.transitions.get(state, [])) == 0
