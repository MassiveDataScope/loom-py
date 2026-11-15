from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from loom.core.execution import ExecutionScope
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


_HOOK_ARGUMENTS = frozenset({"job_id", "result", "exc_type", "exc_msg"})


class CallbackOutcome(StrEnum):
    """How a job ended, naming the :class:`JobCallback` hook it calls."""

    SUCCESS = "success"
    FAILURE = "failure"

    def hook(self, callback: object) -> Callable[..., Any]:
        """Return the hook of *callback*, an instance or its class, that this outcome calls.

        Args:
            callback: A :class:`JobCallback` or its class.

        Returns:
            Its ``on_success`` or ``on_failure``.
        """
        return getattr(callback, f"on_{self}")

    @staticmethod
    def refuse_reserved(payload: Mapping[str, object]) -> None:
        """Refuse a dispatch payload naming an argument a callback hook receives.

        The payload reaches the hooks as keyword arguments beside ``job_id``,
        ``result``, ``exc_type`` and ``exc_msg``.

        Args:
            payload: The payload of a dispatch that attaches a callback.

        Raises:
            ValueError: Naming each reserved key the payload holds.
        """
        reserved = sorted(_HOOK_ARGUMENTS.intersection(payload))
        if reserved:
            raise ValueError(
                f"A job payload with a callback cannot hold {', '.join(reserved)}: "
                "the callback hooks receive these arguments"
            )


async def notify(
    factory: UseCaseFactory, callback_type: type[Any], outcome: CallbackOutcome, **arguments: Any
) -> None:
    """Build *callback_type* and call its hook for *outcome* in one execution as the caller acting.

    With no caller, a constructor ``Caller()`` and a handle fail closed.

    Args:
        factory: Builds the callback with its dependencies.
        callback_type: The :class:`JobCallback` class to build.
        outcome: How the job ended.
        **arguments: The hook's keyword arguments: ``job_id``, then
            ``result`` or ``exc_type`` and ``exc_msg``, then the dispatch
            payload.

    Raises:
        DelegationExpired: Past the caller's deadline.
    """
    with ExecutionScope(current_caller(), what=callback_type.__qualname__):
        reply = outcome.hook(factory.build(callback_type))(**arguments)
        if reply is not None and inspect.isawaitable(reply):
            await reply
