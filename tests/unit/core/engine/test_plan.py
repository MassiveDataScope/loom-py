from __future__ import annotations

import dataclasses

import pytest

from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.plan import (
    ComputeStep,
    ExecutionPlan,
    ExistsStep,
    InputBinding,
    LoadStep,
    ParamBinding,
    RuleStep,
)
from loom.core.errors import RuleViolation
from loom.core.use_case.markers import LookupKind, SourceKind

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _noop_compute(cmd: object, fields_set: frozenset[str]) -> object:
    return cmd


def _noop_rule(cmd: object, fields_set: frozenset[str]) -> None:
    pass


class _FakeUseCase:
    pass


# ---------------------------------------------------------------------------
# ParamBinding
# ---------------------------------------------------------------------------


class TestParamBinding:
    def test_fields_accessible(self) -> None:
        pb = ParamBinding(name="user_id", annotation=int)
        assert pb.name == "user_id"
        assert pb.annotation is int

    def test_is_frozen(self) -> None:
        pb = ParamBinding(name="user_id", annotation=int)
        with pytest.raises(dataclasses.FrozenInstanceError):
            pb.name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# InputBinding
# ---------------------------------------------------------------------------


class TestInputBinding:
    def test_fields_accessible(self) -> None:
        class FakeCommand:
            pass

        ib = InputBinding(name="cmd", command_type=FakeCommand)
        assert ib.name == "cmd"
        assert ib.command_type is FakeCommand

    def test_is_frozen(self) -> None:
        class FakeCommand:
            pass

        ib = InputBinding(name="cmd", command_type=FakeCommand)
        with pytest.raises(dataclasses.FrozenInstanceError):
            ib.name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# LoadStep
# ---------------------------------------------------------------------------


class TestLoadStep:
    def test_fields_accessible(self) -> None:
        class User:
            pass

        ls = LoadStep(
            name="user",
            entity_type=User,
            source_kind=SourceKind.PARAM,
            source_name="user_id",
            lookup_kind=LookupKind.BY_ID,
            against="id",
        )
        assert ls.name == "user"
        assert ls.entity_type is User
        assert ls.source_name == "user_id"

    def test_is_frozen(self) -> None:
        class User:
            pass

        ls = LoadStep(
            name="user",
            entity_type=User,
            source_kind=SourceKind.PARAM,
            source_name="user_id",
            lookup_kind=LookupKind.BY_ID,
            against="id",
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            ls.source_name = "other"  # type: ignore[misc]


class TestExistsStep:
    def test_fields_accessible(self) -> None:
        class User:
            pass

        es = ExistsStep(
            name="exists",
            entity_type=User,
            source_kind=SourceKind.PARAM,
            source_name="email",
            against="email",
        )
        assert es.name == "exists"
        assert es.entity_type is User
        assert es.source_name == "email"


# ---------------------------------------------------------------------------
# ComputeStep
# ---------------------------------------------------------------------------


class TestComputeStep:
    def test_stores_fn_reference(self) -> None:
        step = ComputeStep(fn=_noop_compute)
        assert step.fn is _noop_compute

    def test_is_frozen(self) -> None:
        step = ComputeStep(fn=_noop_compute)
        with pytest.raises(dataclasses.FrozenInstanceError):
            step.fn = _noop_rule  # type: ignore[misc]


# ---------------------------------------------------------------------------
# RuleStep
# ---------------------------------------------------------------------------


class TestRuleStep:
    def test_stores_fn_reference(self) -> None:
        step = RuleStep(fn=_noop_rule)
        assert step.fn is _noop_rule

    def test_is_frozen(self) -> None:
        step = RuleStep(fn=_noop_rule)
        with pytest.raises(dataclasses.FrozenInstanceError):
            step.fn = _noop_rule  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ExecutionPlan
# ---------------------------------------------------------------------------


class TestExecutionPlan:
    def _make_plan(self) -> ExecutionPlan:
        class FakeCommand:
            pass

        class User:
            pass

        return ExecutionPlan(
            use_case_type=_FakeUseCase,
            param_bindings=(ParamBinding("user_id", int),),
            input_binding=InputBinding("cmd", FakeCommand),
            load_steps=(
                LoadStep(
                    "user",
                    User,
                    source_kind=SourceKind.PARAM,
                    source_name="user_id",
                    lookup_kind=LookupKind.BY_ID,
                    against="id",
                ),
            ),
            exists_steps=(),
            compute_steps=(ComputeStep(_noop_compute),),
            rule_steps=(RuleStep(_noop_rule),),
        )

    def test_fields_accessible(self) -> None:
        plan = self._make_plan()
        assert plan.use_case_type is _FakeUseCase
        assert len(plan.param_bindings) == 1
        assert plan.input_binding is not None
        assert len(plan.load_steps) == 1
        assert len(plan.compute_steps) == 1
        assert len(plan.rule_steps) == 1

    def test_is_frozen(self) -> None:
        plan = self._make_plan()
        with pytest.raises(dataclasses.FrozenInstanceError):
            plan.use_case_type = object  # type: ignore[misc]

    def test_no_input_binding_allowed(self) -> None:
        plan = ExecutionPlan(
            use_case_type=_FakeUseCase,
            param_bindings=(),
            input_binding=None,
            load_steps=(),
            exists_steps=(),
            compute_steps=(),
            rule_steps=(),
        )
        assert plan.input_binding is None

    def test_step_tuples_are_immutable(self) -> None:
        plan = self._make_plan()
        with pytest.raises(dataclasses.FrozenInstanceError):
            plan.compute_steps += (ComputeStep(_noop_compute),)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# RuntimeEvent
# ---------------------------------------------------------------------------


class TestRuntimeEvent:
    def test_minimal_event(self) -> None:
        event = RuntimeEvent(kind=EventKind.EXEC_START, use_case_name="MyUseCase")
        assert event.kind == EventKind.EXEC_START
        assert event.use_case_name == "MyUseCase"
        assert event.step_name is None
        assert event.duration_ms is None
        assert event.status is None
        assert event.error is None

    def test_full_event(self) -> None:
        err = RuleViolation("field", "msg")
        event = RuntimeEvent(
            kind=EventKind.EXEC_ERROR,
            use_case_name="MyUseCase",
            step_name="Load User",
            duration_ms=5.2,
            status="failure",
            error=err,
        )
        assert event.step_name == "Load User"
        assert event.duration_ms == pytest.approx(5.2)
        assert event.status == "failure"
        assert event.error is err

    def test_is_frozen(self) -> None:
        event = RuntimeEvent(kind=EventKind.EXEC_DONE, use_case_name="X")
        with pytest.raises(dataclasses.FrozenInstanceError):
            event.use_case_name = "Y"  # type: ignore[misc]

    def test_event_kind_values(self) -> None:
        assert EventKind.COMPILE_START.value == "compile_start"
        assert EventKind.COMPILE_DONE.value == "compile_done"
        assert EventKind.EXEC_START.value == "exec_start"
        assert EventKind.STEP_DONE.value == "step_done"
        assert EventKind.EXEC_DONE.value == "exec_done"
        assert EventKind.EXEC_ERROR.value == "exec_error"
