from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Protocol, Self, TypeVar

from loom.core.command.base import Command
from loom.core.errors.errors import RuleViolation
from loom.core.use_case._predicates import (
    is_present as _is_present,
)
from loom.core.use_case._predicates import (
    predicate_is_present as _predicate_is_present,
)
from loom.core.use_case._predicates import (
    resolve_from_command as _resolve_from_command,
)
from loom.core.use_case.field_ref import FieldExpr, FieldRef

__all__ = [
    "Rule",
    "RuleFn",
]

CommandT = TypeVar("CommandT", bound=Command)


class RuleFn(Protocol):
    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,
    ) -> None: ...


class RuleBuilderFn(RuleFn, Protocol):
    def from_command(self, *sources: FieldRef) -> Self: ...
    def from_params(self, *names: str) -> Self: ...
    def when_present(self, predicate: FieldRef | FieldExpr) -> Self: ...


@dataclass(frozen=True, slots=True)
class _RuleSpec:
    evaluator: Callable[..., Any]
    field: str | None = None
    command_sources: tuple[FieldRef, ...] = ()
    param_names: tuple[str, ...] = ()
    include_command: bool = False
    include_fields_set: bool = False
    predicate: FieldRef | FieldExpr | None = None

    def from_command(self, *sources: FieldRef) -> _RuleSpec:
        if not sources:
            # Full command mode: provide both command and fields_set.
            return replace(self, command_sources=(), include_command=True, include_fields_set=True)
        return replace(
            self,
            command_sources=tuple(sources),
            include_command=False,
            include_fields_set=False,
        )

    def from_params(self, *names: str) -> _RuleSpec:
        if not names:
            raise ValueError("Rule.from_params(...) requires at least one parameter name.")
        return replace(self, param_names=(*self.param_names, *names))

    def when_present(self, predicate: FieldRef | FieldExpr) -> _RuleSpec:
        return replace(self, predicate=predicate)

    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,
    ) -> None:
        if self.predicate is not None and not _predicate_is_present(
            command, fields_set, self.predicate
        ):
            return

        args: list[Any] = []
        if self.include_command:
            args.append(command)
        if self.include_fields_set:
            args.append(fields_set)
        for source in self.command_sources:
            args.append(_resolve_from_command(command, source))

        if self.param_names:
            if context is None:
                raise ValueError("Rule.from_params(...) requires runtime context.")
            if missing := [n for n in self.param_names if n not in context]:
                raise ValueError(f"Missing runtime context keys for rule: {', '.join(missing)}")
            args.extend(context[n] for n in self.param_names)

        self._raise_outcome(self.evaluator(*args), command)

    def _raise_outcome(self, result: object, command: Command) -> None:
        if isinstance(result, RuleViolation):
            raise result
        if isinstance(result, str):
            raise RuleViolation(self._resolved_field(command), result)
        if result is not None:
            raise TypeError(
                f"Rule.check on field '{self._resolved_field(command)}': the evaluator "
                f"returned {type(result).__name__}; return the violation message (str), "
                "a RuleViolation, or None when the value is valid."
            )

    def _resolved_field(self, command: Command) -> str:
        candidates = (
            self.field,
            self.command_sources[0].leaf if self.command_sources else None,
            self.param_names[0] if self.param_names else None,
            self.predicate.leaf if isinstance(self.predicate, FieldRef) else None,
        )
        return next((c for c in candidates if c is not None), type(command).__name__)


@dataclass(frozen=True, slots=True)
class _RequirePresentSpec:
    target: FieldRef
    field: str
    message: str
    predicate: FieldRef | FieldExpr | None = None

    def when_present(self, predicate: FieldRef | FieldExpr) -> _RequirePresentSpec:
        return replace(self, predicate=predicate)

    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,  # noqa: ARG002
    ) -> None:
        if self.predicate is not None and not _predicate_is_present(
            command, fields_set, self.predicate
        ):
            return
        if not _is_present(command, fields_set, self.target):
            raise RuleViolation(self.field, self.message)


class Rule:
    """Rule DSL namespace."""

    @staticmethod
    def check(
        *targets: FieldRef,
        via: Callable[..., str | RuleViolation | None],
    ) -> RuleBuilderFn:
        """Validate fields with ``via``, which receives the targets' values.

        ``via`` returns the violation message (``str``), a ``RuleViolation``, or
        ``None`` when the values are valid. Any other result, a ``bool``
        included, raises ``TypeError``.
        """
        if not targets:
            raise ValueError("Rule.check(...) requires at least one target.")
        return _RuleSpec(
            evaluator=via,
            field=targets[0].leaf,
            command_sources=tuple(targets),
        )

    @staticmethod
    def forbid(
        condition: Callable[..., bool],
        *,
        message: str,
    ) -> RuleBuilderFn:
        def _violation(*args: Any) -> str | None:
            # Only boolean True signals failure, not any truthy value.
            return message if condition(*args) is True else None

        return _RuleSpec(
            evaluator=_violation,
            include_command=True,
            include_fields_set=True,
        )

    @staticmethod
    def require_present(
        target: FieldRef,
        *,
        field: str | None = None,
        message: str | None = None,
    ) -> RuleFn:
        resolved_field = field or target.leaf
        resolved_message = message or f"{target.leaf} is required"
        return _RequirePresentSpec(
            target=target,
            field=resolved_field,
            message=resolved_message,
        )
