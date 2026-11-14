from __future__ import annotations

import inspect
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from loom.core.execution import enter, leave
from loom.core.identity import ANONYMOUS, reset_identity, set_identity

if TYPE_CHECKING:
    from loom.core.use_case.factory import UseCaseFactory


@runtime_checkable
class JobCallback(Protocol):
    """Protocol for job lifecycle hooks.

    Implement this to react to job completion or failure.  Both methods
    may be plain ``def`` or ``async def`` — the runner detects the kind
    via ``inspect.iscoroutinefunction`` at execution time.

    Implementations are instantiated by the factory, so constructor
    dependencies are injected via DI like any other service.

    Example::

        class AuditCallback:
            def __init__(self, audit_repo: AuditRepository) -> None:
                self._repo = audit_repo

            async def on_success(
                self, job_id: str, result: Any, **context: Any
            ) -> None:
                await self._repo.record_success(job_id, result)

            async def on_failure(
                self, job_id: str, exc_type: str, exc_msg: str, **context: Any
            ) -> None:
                await self._repo.record_failure(job_id, exc_type, exc_msg)
    """

    def on_success(self, job_id: str, result: Any, **context: Any) -> Any:
        """Called when the job completes successfully.

        Args:
            job_id: UUID of the completed job.
            result: Value returned by ``Job.execute()``.
            **context: Original dispatch payload forwarded as keyword args.
        """
        ...

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> Any:
        """Called when the job raises an unrecoverable exception.

        Args:
            job_id: UUID of the failed job.
            exc_type: Fully-qualified exception class name.
            exc_msg: String representation of the exception.
            **context: Original dispatch payload forwarded as keyword args.
        """
        ...


class NullJobCallback:
    """No-op callback for tests and contexts that do not need lifecycle hooks.

    Satisfies the ``JobCallback`` protocol without any side effects.

    Example::

        service = InlineJobService(factory, executor, callback=NullJobCallback())
    """

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        """Accept success notification and do nothing."""

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        """Accept failure notification and do nothing."""


@contextmanager
def callback_execution() -> Iterator[None]:
    """Run a job callback's construction and call in an execution with no caller (D7).

    A callback reacts to a job; it does not act for the job's caller, whose
    identity reaching it through the callback link is backlog item L. So the
    execution runs as no one: a constructor ``Caller()`` and a handle (which
    reads the execution identity at call time, D2) raise
    :class:`~loom.core.errors.Unauthenticated`. The ambient identity is
    :data:`~loom.core.identity.ANONYMOUS`, as in any execution with no caller
    (F3): neither row-level security nor a job the callback dispatches sees
    the caller of the execution whose drain runs it. Both are restored after.

    Enter it in the thread or coroutine that builds and calls the callback:
    a coroutine handed to another event loop does not carry it.
    """
    identity_token = set_identity(ANONYMOUS)
    execution_token = enter(None)
    try:
        yield
    finally:
        try:
            leave(execution_token)
        finally:
            reset_identity(identity_token)


async def run_on_success(
    factory: UseCaseFactory,
    callback_type: type[Any],
    job_id: str,
    result: Any,
    context: dict[str, Any],
) -> None:
    """Build ``callback_type`` and call its ``on_success``, both in a :func:`callback_execution`.

    Args:
        factory: Builds the callback with its dependencies.
        callback_type: The :class:`JobCallback` class to build.
        job_id: Identifier of the job that succeeded.
        result: Value the job returned.
        context: The dispatch payload, forwarded as keyword arguments.
    """
    with callback_execution():
        callback = factory.build(callback_type)
        if inspect.iscoroutinefunction(callback.on_success):
            await callback.on_success(job_id=job_id, result=result, **context)
        else:
            callback.on_success(job_id=job_id, result=result, **context)


async def run_on_failure(
    factory: UseCaseFactory,
    callback_type: type[Any],
    job_id: str,
    exc_type: str,
    exc_msg: str,
    context: dict[str, Any],
) -> None:
    """Build ``callback_type`` and call its ``on_failure``, both in a :func:`callback_execution`.

    Args:
        factory: Builds the callback with its dependencies.
        callback_type: The :class:`JobCallback` class to build.
        job_id: Identifier of the job that failed.
        exc_type: Class name of the exception the job raised.
        exc_msg: Its message.
        context: The dispatch payload, forwarded as keyword arguments.
    """
    with callback_execution():
        callback = factory.build(callback_type)
        if inspect.iscoroutinefunction(callback.on_failure):
            await callback.on_failure(job_id=job_id, exc_type=exc_type, exc_msg=exc_msg, **context)
        else:
            callback.on_failure(job_id=job_id, exc_type=exc_type, exc_msg=exc_msg, **context)
