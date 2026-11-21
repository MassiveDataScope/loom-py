from __future__ import annotations

import inspect
from collections.abc import Callable, Collection, Mapping, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from loom.core.execution import ExecutionScope
from loom.core.identity import current_caller

if TYPE_CHECKING:
    from loom.core.use_case.factory import UseCaseFactory


class JobCallback:
    """Base of job lifecycle hooks; both hooks do nothing until overridden.

    Subclass it in a package listed in ``app.packages`` and override the
    hook, or hooks, to run: the scan finds the class, and the dispatcher
    names it in ``dispatch_after_commit(Job, on_success=..., on_failure=...)``.  Each hook
    may be plain ``def`` or ``async def``.  An abstract intermediate class
    inherits ``JobCallback`` and ``ABC``; it is not collected when it declares
    an abstract method.

    Subclasses are instantiated by the factory, so constructor dependencies
    are injected via DI like any other service.

    Example::

        class AuditCallback(JobCallback):
            def __init__(self, audit_repo: AuditRepository) -> None:
                self._repo = audit_repo

            async def on_success(
                self, job_id: str, result: Any, **context: Any
            ) -> None:
                await self._repo.record_success(job_id, result)
    """

    def on_success(self, job_id: str, result: Any, **context: Any) -> Any:
        """Called when the job completes successfully.

        Args:
            job_id: UUID of the completed job.
            result: Value returned by ``Job.execute()``.
            **context: Original dispatch payload forwarded as keyword args.
        """

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> Any:
        """Called when the job raises an unrecoverable exception.

        Args:
            job_id: UUID of the failed job.
            exc_type: The exception's ``__qualname__``.
            exc_msg: String representation of the exception.
            **context: Original dispatch payload forwarded as keyword args.
        """


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
        hook: Callable[..., Any] = getattr(callback, f"on_{self}")
        return hook


class DispatchableCallbacks:
    """The callbacks a dispatch may attach, and the payload it may carry with one.

    Args:
        discovered: The callback classes the scan found.
    """

    def __init__(self, discovered: Collection[type[JobCallback]]) -> None:
        self._discovered = frozenset(discovered)

    @classmethod
    def any(cls) -> DispatchableCallbacks:
        """The callbacks of a service built by hand: any class may be attached."""
        return ANY_CALLBACKS

    def refuse(
        self, payload: Mapping[str, object], *callback_types: type[JobCallback] | None
    ) -> None:
        """Refuse a dispatch attaching an undiscovered callback or a payload naming a hook argument.

        The payload reaches the hooks as keyword arguments beside ``job_id``,
        ``result``, ``exc_type`` and ``exc_msg``.

        Args:
            payload: The payload of the dispatch.
            *callback_types: The callbacks the dispatch attaches; ``None`` for none.

        Raises:
            ValueError: Naming a callback the scan did not find, or each reserved
                key the payload holds when a callback is attached.
        """
        attached = [callback_type for callback_type in callback_types if callback_type is not None]
        if not attached:
            return
        self._refuse_undiscovered(attached)
        reserved = sorted(_HOOK_ARGUMENTS.intersection(payload))
        if reserved:
            raise ValueError(
                f"A job payload with a callback cannot hold {', '.join(reserved)}: "
                "the callback hooks receive these arguments"
            )

    def _refuse_undiscovered(self, attached: Sequence[type[JobCallback]]) -> None:
        for callback_type in attached:
            if callback_type not in self._discovered:
                raise ValueError(
                    f"Job callback {callback_type.__module__}.{callback_type.__qualname__} was "
                    "not discovered: inherit from JobCallback in a package listed in app.packages"
                )


class _AnyCallbacks(DispatchableCallbacks):
    def __init__(self) -> None:
        super().__init__(())

    def _refuse_undiscovered(self, attached: Sequence[type[JobCallback]]) -> None:
        return None


ANY_CALLBACKS = _AnyCallbacks()
"""The callbacks of a service built by hand: any class may be attached."""


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
