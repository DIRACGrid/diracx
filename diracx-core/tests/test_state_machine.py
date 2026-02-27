from __future__ import annotations

from enum import StrEnum

import pytest

from diracx.core.state_machine import StateMachine


class TrafficLight(StrEnum):
    RED = "Red"
    YELLOW = "Yellow"
    GREEN = "Green"


class TrafficLightSM(StateMachine[TrafficLight]):
    states = TrafficLight
    transitions = {
        TrafficLight.RED: [TrafficLight.GREEN],
        TrafficLight.GREEN: [TrafficLight.YELLOW],
        TrafficLight.YELLOW: [TrafficLight.RED],
    }


class TestSubclassValidation:
    def test_missing_state_in_transitions(self):
        """Defining a subclass with a state missing from transitions raises TypeError."""

        class Incomplete(StrEnum):
            A = "A"
            B = "B"

        with pytest.raises(TypeError, match="state 'B' missing from transitions"):

            class BadSM(StateMachine[Incomplete]):
                states = Incomplete
                transitions = {
                    Incomplete.A: [Incomplete.B],
                    # Incomplete.B is missing
                }

    def test_invalid_transition_target(self):
        """A transition target that is not in the states enum raises TypeError."""

        class Src(StrEnum):
            A = "A"
            B = "B"

        class Other(StrEnum):
            X = "X"

        with pytest.raises(TypeError, match="transition target 'X'.*is not a valid"):

            class BadSM(StateMachine[Src]):
                states = Src
                transitions = {
                    Src.A: [Other.X],  # type: ignore[dict-item]
                    Src.B: [],
                }

    def test_valid_subclass_creation(self):
        """A correctly defined subclass is created without errors."""
        # TrafficLightSM was already created at module level — this is the test
        assert TrafficLightSM.states is TrafficLight

    def test_intermediate_class_skipped(self):
        """A subclass that doesn't define states/transitions is allowed."""

        class Intermediate(StateMachine):
            pass  # no states or transitions yet

        # Should not raise


class TestValidateTransition:
    def test_valid_transition(self):
        TrafficLightSM.validate_transition(TrafficLight.RED, TrafficLight.GREEN)

    def test_invalid_transition(self):
        with pytest.raises(
            ValueError, match="Cannot transition from 'Red' to 'Yellow'"
        ):
            TrafficLightSM.validate_transition(TrafficLight.RED, TrafficLight.YELLOW)

    def test_self_transition_rejected(self):
        with pytest.raises(ValueError, match="Cannot transition"):
            TrafficLightSM.validate_transition(TrafficLight.RED, TrafficLight.RED)


class TestGetValidTransitions:
    def test_returns_correct_targets(self):
        assert TrafficLightSM.get_valid_transitions(TrafficLight.RED) == [
            TrafficLight.GREEN
        ]

    def test_returns_empty_for_unknown(self):
        """Querying a state not in the dict returns empty list."""

        class Tiny(StrEnum):
            ONLY = "Only"

        class TinySM(StateMachine[Tiny]):
            states = Tiny
            transitions = {Tiny.ONLY: []}

        assert TinySM.get_valid_transitions(Tiny.ONLY) == []


class TestIsTerminal:
    def test_non_terminal(self):
        assert not TrafficLightSM.is_terminal(TrafficLight.RED)

    def test_terminal(self):
        class Status(StrEnum):
            ACTIVE = "Active"
            DONE = "Done"

        class SM(StateMachine[Status]):
            states = Status
            transitions = {
                Status.ACTIVE: [Status.DONE],
                Status.DONE: [],
            }

        assert SM.is_terminal(Status.DONE)
        assert not SM.is_terminal(Status.ACTIVE)


class TestAllTerminal:
    """Edge case: all states are terminal (no transitions at all)."""

    def test_all_terminal(self):
        class Frozen(StrEnum):
            A = "A"
            B = "B"

        class FrozenSM(StateMachine[Frozen]):
            states = Frozen
            transitions = {
                Frozen.A: [],
                Frozen.B: [],
            }

        assert FrozenSM.is_terminal(Frozen.A)
        assert FrozenSM.is_terminal(Frozen.B)
        assert FrozenSM.get_valid_transitions(Frozen.A) == []
