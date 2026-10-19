from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn

if TYPE_CHECKING:
    from loom.core.engine.plan import ExecutionPlan

ResultT = TypeVar("ResultT")


class Job(ABC, Generic[ResultT]):
    """Base class for all distributable units of work.

    ``Job`` is independent of ``UseCase`` — it does not inherit from it.
    The compiler and factory operate on ``__execution_plan__`` and
    ``execute()`` by duck typing, so they support ``Job`` without changes,
    except that the compiler refuses a ``Query()`` parameter on a ``Job``: a
    job is dispatched with primitive parameters.

    Subclass and implement ``execute`` with any typed signature.  The method
    may be ``async def`` or a plain ``def``; the Celery runner detects the
    kind via ``inspect.iscoroutinefunction`` at registration time.

    Class attributes ``computes`` and ``rules`` declare the pre-execution
    pipeline, identical in semantics to ``UseCase``.  Routing ClassVars
    (``__queue__``, ``__retries__``, etc.) control how Celery dispatches
    and executes the task.  They can be overridden per subclass or at
    runtime via ``JobConfig`` without affecting sibling ``Job`` classes.

    Attributes:
        __execution_plan__: Compiled plan, populated once by the compiler.
        computes: Compute transformations applied before rule checks.
        rules: Rule validations applied after computes.
        __queue__: Celery queue name for this job. Defaults to ``"default"``.
        __retries__: Maximum number of automatic retries on failure.
        __countdown__: Delay in seconds before the task is first executed.
        __timeout__: Soft time limit in seconds; ``None`` means no limit.
        __priority__: Celery task priority (0 = lowest, 9 = highest for most
            brokers).

    Example::

        class SendWelcomeEmailJob(Job[None]):
            __queue__ = "email"
            __retries__ = 3

            def __init__(self, mailer: MailerPort) -> None:
                self._mailer = mailer

            async def execute(self, user_id: int, email: str) -> None:
                await self._mailer.send_welcome(user_id, email)
    """

    __execution_plan__: ClassVar[ExecutionPlan | None] = None
    computes: ClassVar[Sequence[ComputeFn[Any]]] = ()
    rules: ClassVar[Sequence[RuleFn]] = ()

    __queue__: ClassVar[str] = "default"
    __retries__: ClassVar[int] = 0
    __countdown__: ClassVar[int] = 0
    __timeout__: ClassVar[int | None] = None
    __priority__: ClassVar[int] = 0

    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the job logic.

        Override with an explicit typed signature.  The compiler inspects
        this method once at startup to build the ``ExecutionPlan``.

        The method may be ``async def`` or a plain ``def``.  Celery runners
        use ``inspect.iscoroutinefunction`` to choose the right execution
        path.

        Returns:
            The result of the job.  Type is fixed by the ``ResultT``
            generic parameter of the subclass.
        """
        ...
