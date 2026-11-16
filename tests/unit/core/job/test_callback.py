from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.celery.service import CeleryJobService
from loom.core.job.callback import CallbackOutcome, JobCallback, refuse_reserved_payload
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService, JobService
from loom.testing.jobs import RecordingJobService


class _OnlySuccess(JobCallback):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
        self.calls.append(f"success:{job_id}")


class _AsyncHooks(JobCallback):
    async def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
        await asyncio.sleep(0)

    async def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
        await asyncio.sleep(0)


class TestJobCallback:
    def test_the_base_on_success_returns_none(self) -> None:
        assert JobCallback().on_success("j", result=42, user_id=99) is None

    def test_the_base_on_failure_returns_none(self) -> None:
        assert JobCallback().on_failure("j", exc_type="E", exc_msg="m", user_id=99) is None

    @pytest.mark.parametrize("outcome", list(CallbackOutcome))
    def test_the_base_hooks_are_plain_functions_so_they_cost_no_executor_hop(
        self, outcome: CallbackOutcome
    ) -> None:
        assert not inspect.iscoroutinefunction(outcome.hook(JobCallback))

    def test_a_subclass_overriding_one_hook_keeps_the_other_a_no_op(self) -> None:
        callback = _OnlySuccess()

        callback.on_success("j1", result=1)

        assert callback.calls == ["success:j1"]
        assert callback.on_failure("j2", exc_type="E", exc_msg="m") is None

    @pytest.mark.parametrize(
        ("callback_type", "outcome", "is_async"),
        [
            (_OnlySuccess, CallbackOutcome.SUCCESS, False),
            (_OnlySuccess, CallbackOutcome.FAILURE, False),
            (_AsyncHooks, CallbackOutcome.SUCCESS, True),
            (_AsyncHooks, CallbackOutcome.FAILURE, True),
        ],
    )
    def test_the_runner_detects_a_sync_or_async_override(
        self, callback_type: type[JobCallback], outcome: CallbackOutcome, is_async: bool
    ) -> None:
        assert inspect.iscoroutinefunction(outcome.hook(callback_type)) is is_async


class _AcmeCallback(JobCallback):
    pass


class _AcmeJob(Job[None]):
    def execute(self) -> None:
        return None


SERVICES = pytest.mark.parametrize(
    "service",
    [
        pytest.param(lambda: InlineJobService(MagicMock(), MagicMock()), id="inline"),
        pytest.param(lambda: CeleryJobService(MagicMock(), callbacks=[_AcmeCallback]), id="celery"),
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
            jobs.dispatch(_AcmeJob, payload={key: "acme"}, **{hook: _AcmeCallback})

    def test_a_dispatch_without_a_callback_accepts_the_key(
        self, service: Callable[[], JobService]
    ) -> None:
        jobs = service()

        jobs.dispatch(_AcmeJob, payload={"job_id": "acme"})


class TestRefuseReserved:
    def test_names_every_reserved_key_the_payload_holds(self) -> None:
        with pytest.raises(ValueError, match="cannot hold exc_msg, job_id: "):
            refuse_reserved_payload({"job_id": 1, "exc_msg": "m", "acme": 2})
