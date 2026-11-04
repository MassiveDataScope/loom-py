"""The caller identity survives the broker hop, or the job fails closed.

A context variable does not cross a process boundary, so the identity travels
inside the job envelope. These tests pin both ends of that wire contract: what
``dispatch`` puts in, what the worker takes out, and what happens to an envelope
minted before the contract existed.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.celery.runner import _make_job_task
from loom.celery.service import CeleryJobService
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import Unauthenticated
from loom.core.identity import ANONYMOUS, Identity, current_identity, reset_identity, set_identity
from loom.core.job.config import JobSettings
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.use_case.markers import Caller

_SUBJECT = "user-1"
_ROLES = ("role_a",)
_ATTRIBUTES = {"email": "ada@example.com"}
_MECHANISM = "jwt"
_IDENTITY_KEY = "identity"

CALLER = Identity(
    subject=_SUBJECT,
    roles=_ROLES,
    attributes=_ATTRIBUTES,
    mechanism=_MECHANISM,
)


class _ReportJob(Job[str]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


@pytest.fixture(autouse=True)
def _clean_pending() -> Any:
    """Isolate the pending-dispatch context variable for every test."""
    clear_pending_dispatches()
    yield
    clear_pending_dispatches()


def _mock_celery_app() -> MagicMock:
    app = MagicMock()
    app.task = MagicMock(side_effect=lambda **kw: lambda fn: fn)
    app.conf.task_always_eager = False
    return app


async def _dispatch_kwargs(identity: Identity | None) -> dict[str, Any]:
    """Dispatch one job under *identity* and return the ``send_task`` kwargs."""
    app = MagicMock()
    service = CeleryJobService(app)
    token = set_identity(identity) if identity is not None else None
    try:
        service.dispatch(_ReportJob, payload={})
        await flush_pending_dispatches()
    finally:
        if token is not None:
            reset_identity(token)
    return dict(app.send_task.call_args.kwargs["kwargs"])


# ---------------------------------------------------------------------------
# Dispatch side
# ---------------------------------------------------------------------------


async def test_dispatch_puts_the_caller_in_the_envelope() -> None:
    """The identity is captured at dispatch, when the caller is still known."""
    envelope = await _dispatch_kwargs(CALLER)
    assert envelope[_IDENTITY_KEY] == {
        "subject": _SUBJECT,
        "roles": list(_ROLES),
        "attributes": dict(_ATTRIBUTES),
        "mechanism": _MECHANISM,
    }


async def test_dispatch_without_a_caller_sends_no_identity() -> None:
    """An unauthenticated dispatch must not invent a caller for the worker."""
    envelope = await _dispatch_kwargs(None)
    assert envelope[_IDENTITY_KEY] is None


async def test_an_anonymous_caller_sends_no_identity() -> None:
    """Anonymity is the absence of a caller, encoded as such."""
    envelope = await _dispatch_kwargs(ANONYMOUS)
    assert envelope[_IDENTITY_KEY] is None


# ---------------------------------------------------------------------------
# Worker side
# ---------------------------------------------------------------------------


class _RecordingExecutor:
    """Captures what the worker hands to the executor, plus the live context."""

    def __init__(self) -> None:
        self.identity: Identity | None = None
        self.context_identity: Identity | None = None

    async def execute(self, instance: Any, **kwargs: Any) -> str:
        self.identity = kwargs.get("identity")
        self.context_identity = current_identity()
        return "done"


def _run_task(envelope_identity: dict[str, Any] | None) -> _RecordingExecutor:
    executor = _RecordingExecutor()
    factory = MagicMock()
    factory.build = MagicMock(return_value=_ReportJob())
    runtime = MagicMock()
    runtime.run = MagicMock(side_effect=lambda coro, **kw: _drain(coro))
    task = _make_job_task(
        _mock_celery_app(),
        _ReportJob,
        factory,
        executor,  # type: ignore[arg-type]
        runtime,
        settings=JobSettings(),
    )
    task(MagicMock(), payload={}, identity=envelope_identity)
    return executor


def _drain(coro: Any) -> Any:
    """Run a coroutine to completion on a private event loop."""
    return asyncio.run(coro)


def test_the_worker_hands_the_envelope_caller_to_the_executor() -> None:
    """Round trip: the identity dispatched is the identity the job executes with."""
    executor = _run_task({"subject": _SUBJECT, "roles": list(_ROLES), "mechanism": _MECHANISM})
    assert executor.identity == Identity(subject=_SUBJECT, roles=_ROLES, mechanism=_MECHANISM)


def test_the_worker_publishes_the_caller_in_the_context() -> None:
    """Jobs dispatching further jobs must propagate the same caller onward."""
    executor = _run_task({"subject": _SUBJECT})
    assert executor.context_identity is not None
    assert executor.context_identity.subject == _SUBJECT


def test_the_worker_restores_the_previous_context_afterwards() -> None:
    """Worker processes reuse threads: a leaked identity would cross tasks."""
    _run_task({"subject": _SUBJECT})
    assert current_identity() is ANONYMOUS


def test_an_envelope_without_identity_delivers_none_to_the_executor() -> None:
    """An envelope predating the contract carries no caller, and does not fake one."""
    executor = _run_task(None)
    assert executor.identity is None


# ---------------------------------------------------------------------------
# Fail-closed on the legacy envelope
# ---------------------------------------------------------------------------


async def test_a_job_declaring_a_caller_fails_closed_on_a_legacy_envelope() -> None:
    """The job refuses to run rather than execute as an unknown caller."""
    compiler = UseCaseCompiler()
    compiler.compile(_ReportJob)
    executor = RuntimeExecutor(compiler)
    job = _ReportJob()

    with pytest.raises(Unauthenticated, match="_ReportJob"):
        await executor.execute(job)
