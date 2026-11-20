from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Protocol

from loom.core.engine.plan import ExecutionPlan
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn


class Compilable(Protocol):
    """Structural protocol for types that can be compiled and executed.

    Both :class:`~loom.core.use_case.use_case.UseCase` and
    :class:`~loom.core.job.job.Job` satisfy this protocol by duck typing.
    :class:`~loom.core.engine.executor.UseCaseExecutor` depends on this
    abstraction, not on either concrete class — satisfying OCP and DIP.

    The compiler inspects the class once at startup and stores an immutable
    :class:`~loom.core.engine.plan.ExecutionPlan`.  The executor drives
    execution purely from that plan; no per-request reflection occurs.

    Implementors must declare:

    - ``__execution_plan__``: populated by the compiler at startup.
    - ``computes``: transformations applied before rule checks.
    - ``rules``: validations applied after computes.
    - ``execute()``: core logic; may be ``async def`` or plain ``def``.
      The executor detects the kind via ``inspect.iscoroutinefunction``.
    """

    __execution_plan__: ClassVar[ExecutionPlan | None]
    computes: ClassVar[Sequence[ComputeFn[Any]]]
    rules: ClassVar[Sequence[RuleFn]]

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute core logic.

        May be ``async def`` or plain ``def``.  The executor checks
        ``inspect.iscoroutinefunction`` and awaits only when needed.
        """
        ...
