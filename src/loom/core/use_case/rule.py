from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Protocol, Self, TypeVar

from loom.core.command.base import Command
from loom.core.errors import RuleViolation
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
class Rule:
    """A command validation rule: an evaluator and where its arguments come from.

    Build rules with :meth:`check`, :meth:`forbid` and :meth:`require_present`.

    Args:
        evaluator: Returns the violation message (``str``), a ``RuleViolation``,
            or ``None`` when the arguments are valid.
        field: Field a ``str`` violation is reported on.
        command_sources: Command fields whose values are passed to ``evaluator``.
        param_names: Runtime context keys whose values are passed after them.
        include_command: Pass the command as the first argument.
        include_fields_set: Pass the set of fields the caller provided.
        predicate: The rule runs only when this field or expression is present.
    """

    evaluator: Callable[..., Any]
    field: str | None = None
    command_sources: tuple[FieldRef, ...] = ()
    param_names: tuple[str, ...] = ()
    include_command: bool = False
    include_fields_set: bool = False
    predicate: FieldRef | FieldExpr | None = None

    @classmethod
    def check(
        cls,
        *targets: FieldRef,
        via: Callable[..., str | RuleViolation | None],
    ) -> Rule:
        """Validate fields with ``via``, which receives the targets' values.

        ``via`` returns the violation message (``str``), a ``RuleViolation``, or
        ``None`` when the values are valid. Any other result, a ``bool``
        included, raises ``TypeError``.

        Raises:
            ValueError: When no target is given.
        """
        if not targets:
            raise ValueError("Rule.check(...) requires at least one target.")
        return cls(evaluator=via, field=targets[0].leaf, command_sources=tuple(targets))

    @classmethod
    def forbid(cls, condition: Callable[..., bool], *, message: str) -> Rule:
        """Report ``message`` when ``condition(command, fields_set)`` is ``True``."""

        def _violation(*args: Any) -> str | None:
            return message if condition(*args) is True else None

        return cls(evaluator=_violation, include_command=True, include_fields_set=True)

    @classmethod
    def require_present(
        cls,
        target: FieldRef,
        *,
        field: str | None = None,
        message: str | None = None,
    ) -> Rule:
        """Report a violation when ``target`` has no value in the command."""
        absent = message or f"{target.leaf} is required"

        def _violation(command: Command, fields_set: frozenset[str], *_: Any) -> str | None:
            return None if _is_present(command, fields_set, target) else absent

        return cls(
            evaluator=_violation,
            field=field or target.leaf,
            include_command=True,
            include_fields_set=True,
        )

    def from_command(self, *sources: FieldRef) -> Rule:
        """Pass these command fields' values, or the whole command when none is given."""
        if not sources:
            return replace(self, command_sources=(), include_command=True, include_fields_set=True)
        return replace(
            self,
            command_sources=tuple(sources),
            include_command=False,
            include_fields_set=False,
        )

    def from_params(self, *names: str) -> Rule:
        """Also pass these runtime context values.

        Raises:
            ValueError: When no name is given.
        """
        if not names:
            raise ValueError("Rule.from_params(...) requires at least one parameter name.")
        return replace(self, param_names=(*self.param_names, *names))

    def when_present(self, predicate: FieldRef | FieldExpr) -> Rule:
        """Run only when ``predicate`` is present in the command."""
        return replace(self, predicate=predicate)

    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,
    ) -> None:
        """Evaluate the rule against one command.

        Raises:
            RuleViolation: When the evaluator reports a violation.
            TypeError: When the evaluator returns anything else than a message,
                a ``RuleViolation`` or ``None``.
            ValueError: When a runtime context key the rule reads is missing.
        """
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

        result = self.evaluator(*args)
        if result is not None:
            self._raise_outcome(result, command)

    def _raise_outcome(self, result: object, command: Command) -> None:
        if isinstance(result, RuleViolation):
            raise result
        if isinstance(result, str):
            raise RuleViolation(self._resolved_field(command), result)
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
