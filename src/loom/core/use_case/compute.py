from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, TypeVar

import msgspec

from loom.core.command.base import Command
from loom.core.use_case._predicates import (
    predicate_is_present as _predicate_is_present,
)
from loom.core.use_case._predicates import (
    resolve_from_command as _resolve_from_command,
)
from loom.core.use_case.field_ref import FieldExpr, FieldRef

CommandT = TypeVar("CommandT", bound=Command)


ComputeFn = Callable[[CommandT, frozenset[str]], CommandT]


@dataclass(frozen=True, slots=True)
class _ComputeSpec:
    target: FieldRef
    command_sources: tuple[FieldRef, ...] = ()
    param_names: tuple[str, ...] = ()
    include_command: bool = False
    via: Callable[..., Any] | None = None
    predicate: FieldRef | FieldExpr | None = None

    def from_params(self, *names: str) -> _ComputeSpec:
        if not names:
            raise ValueError("Compute.from_params(...) requires at least one param name.")
        return replace(self, param_names=(*self.param_names, *names))

    def when_present(self, predicate: FieldRef | FieldExpr) -> _ComputeSpec:
        return replace(self, predicate=predicate)

    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,
    ) -> Command:
        if self.predicate is not None and not _predicate_is_present(
            command, fields_set, self.predicate
        ):
            return command

        args: list[Any] = []
        if self.include_command:
            args.append(command)
        for source in self.command_sources:
            args.append(_resolve_from_command(command, source))

        if self.param_names:
            if context is None:
                raise ValueError("Compute.from_params(...) requires runtime context.")
            if missing := [n for n in self.param_names if n not in context]:
                raise ValueError(f"Missing runtime context keys for compute: {', '.join(missing)}")
            args.extend(context[n] for n in self.param_names)

        if self.via is None:
            if len(args) != 1:
                raise ValueError(
                    "Compute without via requires exactly one source. "
                    "Use from_command(F(...)) or provide via=..."
                )
            next_value = args[0]
        else:
            next_value = self.via(*args)

        return msgspec.structs.replace(command, **{self.target.leaf: next_value})


class _ComputeSetBuilder:
    __slots__ = ("_target",)

    def __init__(self, target: FieldRef) -> None:
        self._target = target

    def from_command(
        self,
        *sources: FieldRef,
        via: Callable[..., Any] | None = None,
    ) -> _ComputeSpec:
        if not sources:
            if via is None:
                raise ValueError(
                    "Compute.from_command() without fields requires via=... "
                    "to derive the target value."
                )
            return _ComputeSpec(
                target=self._target,
                include_command=True,
                via=via,
            )

        if len(sources) > 1 and via is None:
            raise ValueError("A via callable is required when multiple command fields are used.")

        return _ComputeSpec(
            target=self._target,
            command_sources=tuple(sources),
            via=via,
        )


class Compute:
    """Compute DSL namespace.

    Example:
        ``Compute.set(F(UpdateUser).slug).from_command(F(UpdateUser).name, via=slugify)``
    """

    @staticmethod
    def set(target: FieldRef) -> _ComputeSetBuilder:
        return _ComputeSetBuilder(target)
