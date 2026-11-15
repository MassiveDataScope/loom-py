from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.celery.service import CeleryJobService
from loom.core.job.callback import CallbackOutcome, JobCallback, NullJobCallback
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService, JobService
from loom.testing.jobs import RecordingJobService

# ---------------------------------------------------------------------------
# NullJobCallback satisfies the protocol
# ---------------------------------------------------------------------------


class TestJobCallback:
    def test_null_callback_satisfies_protocol(self) -> None:
        assert isinstance(NullJobCallback(), JobCallback)

    def test_null_callback_on_success_does_not_raise(self) -> None:
        cb = NullJobCallback()
        cb.on_success("job-1", result=42, user_id=99)

    def test_null_callback_on_failure_does_not_raise(self) -> None:
        cb = NullJobCallback()
        cb.on_failure("job-1", exc_type="ValueError", exc_msg="boom", user_id=99)

    def test_null_callback_on_success_returns_none(self) -> None:
        assert NullJobCallback().on_success("j", result=None) is None

    def test_null_callback_on_failure_returns_none(self) -> None:
        assert NullJobCallback().on_failure("j", exc_type="E", exc_msg="m") is None

    # ---------------------------------------------------------------------------
    # Async implementations also satisfy the protocol (structural check)
    # ---------------------------------------------------------------------------

    def test_async_on_success_satisfies_protocol(self) -> None:
        class AsyncCallback:
            async def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
                await asyncio.sleep(0)

            async def on_failure(
                self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any
            ) -> None:
                await asyncio.sleep(0)

        assert isinstance(AsyncCallback(), JobCallback)

    def test_partial_implementation_does_not_satisfy_protocol(self) -> None:
        class MissingOnFailure:
            def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
                # Intentionally empty: the test asserts missing on_failure.
                return None

        assert not isinstance(MissingOnFailure(), JobCallback)

    # ---------------------------------------------------------------------------
    # Custom sync callback
    # ---------------------------------------------------------------------------

    def test_custom_sync_callback_satisfies_protocol(self) -> None:
        calls: list[str] = []

        class RecordingCallback:
            def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
                calls.append(f"success:{job_id}")

            def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
                calls.append(f"failure:{job_id}")

        cb = RecordingCallback()
        assert isinstance(cb, JobCallback)
        cb.on_success("j1", result=1)
        cb.on_failure("j2", exc_type="E", exc_msg="m")
        assert calls == ["success:j1", "failure:j2"]


class _AcmeJob(Job[None]):
    def execute(self) -> None:
        return None


SERVICES = pytest.mark.parametrize(
    "service",
    [
        pytest.param(lambda: InlineJobService(MagicMock(), MagicMock()), id="inline"),
        pytest.param(lambda: CeleryJobService(MagicMock()), id="celery"),
        pytest.param(RecordingJobService, id="recording"),
    ],
)
"""Every job service that accepts callbacks."""


@SERVICES
class TestReservedCallbackArguments:
    """A payload naming an argument a callback hook receives is refused at dispatch."""

    @pytest.mark.parametrize("key", ["job_id", "result", "exc_type", "exc_msg"])
    @pytest.mark.parametrize("hook", ["on_success", "on_failure"])
    def test_a_dispatch_with_a_callback_refuses_the_key(
        self, service: Callable[[], JobService], key: str, hook: str
    ) -> None:
        jobs = service()

        with pytest.raises(ValueError, match=f"cannot hold {key}"):
            jobs.dispatch(_AcmeJob, payload={key: "acme"}, **{hook: NullJobCallback})

    def test_a_dispatch_without_a_callback_accepts_the_key(
        self, service: Callable[[], JobService]
    ) -> None:
        jobs = service()

        jobs.dispatch(_AcmeJob, payload={"job_id": "acme"})


class TestRefuseReserved:
    def test_names_every_reserved_key_the_payload_holds(self) -> None:
        with pytest.raises(ValueError, match="cannot hold exc_msg, job_id: "):
            CallbackOutcome.refuse_reserved({"job_id": 1, "exc_msg": "m", "acme": 2})
