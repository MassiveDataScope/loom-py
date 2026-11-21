from __future__ import annotations

import asyncio
import inspect
from typing import Any

import pytest

from loom.core.job.callback import CallbackOutcome, DispatchableCallbacks, JobCallback
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.testing import RecordingJobTransport


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


_DISCOVERED = DispatchableCallbacks([_AcmeCallback])


class _AcmeUndiscoveredCallback(JobCallback):
    pass


@pytest.fixture
def jobs() -> JobService:
    return JobService(RecordingJobTransport(), callbacks=_DISCOVERED)


class TestReservedCallbackArguments:
    """A payload naming an argument a callback hook receives is refused at dispatch."""

    @pytest.mark.parametrize("key", ["job_id", "result", "exc_type", "exc_msg"])
    @pytest.mark.parametrize("hook", ["on_success", "on_failure"])
    def test_a_dispatch_with_a_callback_refuses_the_key(
        self, jobs: JobService, key: str, hook: str
    ) -> None:
        with pytest.raises(ValueError, match=f"cannot hold {key}"):
            jobs.dispatch_after_commit(_AcmeJob, payload={key: "acme"}, **{hook: _AcmeCallback})

    def test_a_dispatch_without_a_callback_accepts_the_key(self, jobs: JobService) -> None:
        jobs.dispatch_after_commit(_AcmeJob, payload={"job_id": "acme"})


class TestUndiscoveredCallbacks:
    @pytest.mark.parametrize("hook", ["on_success", "on_failure"])
    def test_a_service_built_from_composition_refuses_a_class_the_scan_did_not_find(
        self, jobs: JobService, hook: str
    ) -> None:
        with pytest.raises(ValueError, match="_AcmeUndiscoveredCallback was not discovered"):
            jobs.dispatch_after_commit(_AcmeJob, **{hook: _AcmeUndiscoveredCallback})

    def test_a_hand_built_service_accepts_any_class(self) -> None:
        DispatchableCallbacks.any().refuse({}, _AcmeUndiscoveredCallback)

    def test_a_dispatch_with_no_callback_is_never_refused(self) -> None:
        _DISCOVERED.refuse({"job_id": 1}, None, None)


class TestRefuseReserved:
    def test_names_every_reserved_key_the_payload_holds(self) -> None:
        with pytest.raises(ValueError, match="cannot hold exc_msg, job_id: "):
            _DISCOVERED.refuse({"job_id": 1, "exc_msg": "m", "acme": 2}, _AcmeCallback)
