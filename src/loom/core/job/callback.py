from __future__ import annotations

import inspect
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from loom.core.execution import enter, leave
from loom.core.identity import current_caller

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
    """Run a job callback's construction and call in an execution acting as its dispatcher (D7).

    A callback is work its job's dispatch caused, so it acts as the caller
    acting where it runs: the inline dispatch installs the identity captured
    at dispatch, and the Celery callback worker the one its link envelope
    carries. A constructor ``Caller()``, a handle (which reads the execution
    identity at call time, D2), row-level security and a job the callback
    dispatches all see it; with no caller captured they fail closed. It is
    refused once that caller's delegation has expired.

    Enter it in the thread or coroutine that builds and calls the callback:
    a coroutine handed to another event loop does not carry it.
    """
    execution_token = enter(current_caller(), "job callback")
    try:
        yield
    finally:
        leave(execution_token)


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
