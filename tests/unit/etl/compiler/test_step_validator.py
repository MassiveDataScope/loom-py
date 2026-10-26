"""Tests for compiler._validators.validate_step."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from loom.etl.compiler import _validators
from loom.etl.compiler import _validators_step as step_validator
from loom.etl.compiler._plan import AgentBinding, ConfigValueBinding, SourceBinding, TargetBinding
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.declarative.target._table import ReplaceSpec

StepCompilationContext = step_validator.StepCompilationContext
validate_step = step_validator.validate_step


def _build_context() -> StepCompilationContext:
    source_spec = TableSourceSpec(
        alias="orders",
        table_ref=TableRef("raw.orders"),
    )
    return StepCompilationContext(
        step_type=type("DummyStep", (), {}),
        params_type=type("DummyParams", (), {}),
        source_bindings=(SourceBinding(alias="orders", spec=source_spec),),
        target_binding=TargetBinding(spec=ReplaceSpec(table_ref=TableRef("staging.orders"))),
        config_bindings=(ConfigValueBinding(alias="token", key="api.token", value_type=str),),
    )


def test_validate_step_calls_all_validators_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _build_context()
    calls: list[str] = []

    def _validate_signature(
        step_type: type[Any],
        params_type: type[Any],
        source_bindings: tuple[SourceBinding, ...],
        config_bindings: tuple[ConfigValueBinding, ...],
        agent_bindings: tuple[AgentBinding, ...],
    ) -> None:
        assert step_type is ctx.step_type
        assert params_type is ctx.params_type
        assert source_bindings == ctx.source_bindings
        assert config_bindings == ctx.config_bindings
        assert agent_bindings == ctx.agent_bindings
        calls.append("signature")

    def _validate_upsert(step_type: type[Any], spec: Any) -> None:
        assert step_type is ctx.step_type
        assert spec is ctx.target_binding.spec
        calls.append("upsert")

    def _validate_historify(step_type: type[Any], spec: Any) -> None:
        assert step_type is ctx.step_type
        assert spec is ctx.target_binding.spec
        calls.append("historify")

    def _validate_params(
        step_type: type[Any],
        params_type: type[Any],
        source_bindings: tuple[SourceBinding, ...],
        target_binding: TargetBinding,
    ) -> None:
        assert step_type is ctx.step_type
        assert params_type is ctx.params_type
        assert source_bindings == ctx.source_bindings
        assert target_binding == ctx.target_binding
        calls.append("params")

    def _validate_files(
        step_type: type[Any],
        params_type: type[Any],
        source_bindings: tuple[SourceBinding, ...],
        target_binding: TargetBinding,
    ) -> None:
        assert (step_type, params_type, source_bindings, target_binding) == (
            ctx.step_type,
            ctx.params_type,
            ctx.source_bindings,
            ctx.target_binding,
        )
        calls.append("files")

    monkeypatch.setattr(step_validator, "validate_execute_signature", _validate_signature)
    monkeypatch.setattr(step_validator, "validate_upsert_spec", _validate_upsert)
    monkeypatch.setattr(step_validator, "validate_historify_spec", _validate_historify)
    monkeypatch.setattr(step_validator, "validate_param_exprs", _validate_params)
    monkeypatch.setattr(step_validator, "validate_file_path_templates", _validate_files)

    validate_step(ctx)

    assert calls == ["signature", "upsert", "historify", "params", "files"]


def test_validate_step_has_one_implementation() -> None:
    assert _validators.validate_step is step_validator.validate_step


def test_step_compilation_context_is_frozen() -> None:
    ctx = _build_context()

    with pytest.raises((FrozenInstanceError, AttributeError)):
        ctx.params_type = int  # type: ignore[misc]
