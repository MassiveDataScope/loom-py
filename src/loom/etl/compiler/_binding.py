"""Source and target binding resolution for ETL step compilation.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Protocol, runtime_checkable

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    AgentBinding,
    ConfigValueBinding,
    SourceBinding,
    TargetBinding,
)
from loom.etl.pipeline._step import ETLStep


@runtime_checkable
class _TargetLike(Protocol):
    def _to_spec(self) -> Any: ...


@runtime_checkable
class _GroupedSourcesLike(Protocol):
    def _to_specs(self) -> tuple[Any, ...]: ...


class _SourceFormKey(Enum):
    NONE = "none"
    INLINE = "inline"
    GROUPED = "grouped"


def resolve_source_bindings(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    """Build source bindings from the step class declaration.

    Handles all three source forms: ``NONE``, ``INLINE``, and ``GROUPED``.

    Args:
        step_type: Concrete ``ETLStep`` subclass.

    Returns:
        Tuple of :class:`~loom.etl.compiler._plan.SourceBinding` instances.

    Raises:
        ETLCompilationError: When the sources declaration is invalid.
    """
    source_form = _to_source_form_key(step_type._source_form)
    match source_form:
        case _SourceFormKey.NONE:
            return ()
        case _SourceFormKey.INLINE:
            return _bindings_from_inline(step_type)
        case _SourceFormKey.GROUPED:
            return _bindings_from_grouped(step_type)
    raise AssertionError(f"Unhandled source form: {step_type._source_form!r}")  # pragma: no cover


def resolve_target_binding(step_type: type[ETLStep[Any]]) -> TargetBinding:
    """Build the target binding from the step class declaration.

    Args:
        step_type: Concrete ``ETLStep`` subclass.

    Returns:
        :class:`~loom.etl.compiler._plan.TargetBinding` with the normalized spec.

    Raises:
        ETLCompilationError: When ``target`` is missing or has an invalid type.
    """
    target = step_type.target
    if target is None:
        raise ETLCompilationError.missing_target(step_type)
    if not isinstance(target, _TargetLike):
        raise ETLCompilationError.invalid_target_type(step_type)
    return TargetBinding(spec=target._to_spec())


def resolve_config_bindings(step_type: type[ETLStep[Any]]) -> tuple[ConfigValueBinding, ...]:
    """Build config value bindings from the step's ``FromConfig`` attributes.

    Args:
        step_type: Concrete ``ETLStep`` subclass.

    Returns:
        Tuple of :class:`~loom.etl.compiler._plan.ConfigValueBinding`, in
        declaration order.
    """
    return tuple(
        ConfigValueBinding(alias=alias, key=value.key, value_type=value.value_type)
        for alias, value in step_type._config_values.items()
    )


def resolve_agent_bindings(step_type: type[ETLStep[Any]]) -> tuple[AgentBinding, ...]:
    """Build agent bindings from the step's ``WithAgent`` attributes.

    Args:
        step_type: Concrete ``ETLStep`` subclass.

    Returns:
        Tuple of :class:`~loom.etl.compiler._plan.AgentBinding`, in
        declaration order.
    """
    return tuple(
        AgentBinding(alias=alias, name=agent.name, output_type=agent.output, max_usd=agent.max_usd)
        for alias, agent in step_type._agents.items()
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _bindings_from_inline(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    return tuple(
        SourceBinding(alias=alias, spec=src._to_spec(alias))
        for alias, src in step_type._inline_sources.items()
    )


def _bindings_from_grouped(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    grouped = step_type.sources
    if not isinstance(grouped, _GroupedSourcesLike):
        raise ETLCompilationError.invalid_sources_type(step_type)
    return tuple(SourceBinding(alias=spec.alias, spec=spec) for spec in grouped._to_specs())


def _normalize_source_form(raw: Any) -> _SourceFormKey:
    """Backward-compatible alias kept for imports/tests."""
    return _to_source_form_key(raw)


def _to_source_form_key(raw: Any) -> _SourceFormKey:
    """Normalize source-form declarations across enum identities/reloads."""
    if isinstance(raw, _SourceFormKey):
        return raw
    value = raw.value if isinstance(raw, Enum) else raw
    if isinstance(value, str):
        try:
            return _SourceFormKey(value)
        except ValueError as exc:
            raise AssertionError(f"Unhandled source form: {raw!r}") from exc
    raise AssertionError(f"Unhandled source form: {raw!r}")
