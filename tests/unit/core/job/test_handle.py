from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any

import pytest
from celery import Celery

from loom.core.errors import JobFailedError, JobTimeoutError
from loom.core.job.handle import InlineOutcome, JobGroup, JobHandle


@pytest.fixture
def results() -> Celery:
    """A Celery application whose result backend lives in memory."""
    return Celery(broker="memory://", backend="cache+memory://")


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _inline_handle(result: Any, job_id: str = "test-id") -> JobHandle[Any]:
    outcome = InlineOutcome()
    outcome.succeed(result)
    return JobHandle(job_id=job_id, queue="default", dispatched_at=_now(), _inline_outcome=outcome)


# ---------------------------------------------------------------------------
# JobHandle — inline mode
# ---------------------------------------------------------------------------


class TestJobHandle:
    @pytest.mark.parametrize(
        "result",
        [
            pytest.param(42, id="wait_inline_returns_result"),
            pytest.param("hello", id="wait_inline_returns_string_result"),
        ],
    )
    def test_wait_inline_returns_result(self, result: object) -> None:
        handle = _inline_handle(result)
        assert handle.wait() == result

    def test_wait_inline_returns_none_result(self) -> None:
        handle = _inline_handle(None)
        assert handle.wait() is None

    # ---------------------------------------------------------------------------
    # JobHandle — Celery (async_result) mode
    # ---------------------------------------------------------------------------

    def test_wait_async_result_returns_what_the_result_backend_holds(self, results: Celery) -> None:
        results.backend.store_result("x", 99, "SUCCESS")
        handle = JobHandle(
            job_id="x", queue="q", dispatched_at=_now(), _async_result=results.AsyncResult("x")
        )

        assert handle.wait(timeout=5.0) == 99

    def test_wait_async_timeout_raises_job_timeout_error(self, results: Celery) -> None:
        handle = JobHandle(
            job_id="tid",
            queue="q",
            dispatched_at=_now(),
            _async_result=results.AsyncResult("never-stored"),
        )

        with pytest.raises(JobTimeoutError) as exc_info:
            handle.wait(timeout=0.05)

        assert exc_info.value.job_id == "tid"
        assert exc_info.value.timeout == pytest.approx(0.05)

    def test_wait_async_failure_raises_job_failed_error(self, results: Celery) -> None:
        results.backend.store_result("fid", ValueError("something went wrong"), "FAILURE")
        handle = JobHandle(
            job_id="fid", queue="q", dispatched_at=_now(), _async_result=results.AsyncResult("fid")
        )

        with pytest.raises(JobFailedError) as exc_info:
            handle.wait()

        assert exc_info.value.job_id == "fid"
        assert isinstance(exc_info.value.cause, ValueError)

    # ---------------------------------------------------------------------------
    # JobHandle — identity and immutability
    # ---------------------------------------------------------------------------

    def test_job_handle_is_frozen(self) -> None:
        handle = _inline_handle(1)
        with pytest.raises(dataclasses.FrozenInstanceError):
            handle.job_id = "other"  # type: ignore[misc]

    def test_job_handle_equality_by_job_id_and_queue(self) -> None:
        ts = _now()
        h1 = JobHandle(job_id="a", queue="q", dispatched_at=ts)
        h2 = JobHandle(job_id="a", queue="q", dispatched_at=ts)
        assert h1 == h2

    # ---------------------------------------------------------------------------
    # JobGroup
    # ---------------------------------------------------------------------------

    def test_wait_all_returns_results_in_order(self) -> None:
        h1 = _inline_handle(10, "j1")
        h2 = _inline_handle(20, "j2")
        group = JobGroup(handles=(h1, h2))

        assert group.wait_all() == [10, 20]

    def test_wait_all_single_handle(self) -> None:
        group = JobGroup(handles=(_inline_handle("ok"),))
        assert group.wait_all() == ["ok"]

    def test_then_raises_not_implemented(self) -> None:
        group = JobGroup(handles=(_inline_handle(1),))
        with pytest.raises(NotImplementedError):
            group.then(object)

    # ---------------------------------------------------------------------------
    # Error types
    # ---------------------------------------------------------------------------

    def test_job_timeout_error_message_with_timeout(self) -> None:
        err = JobTimeoutError("abc", timeout=10.0)
        assert "abc" in str(err)
        assert "10.0" in str(err)
        assert err.code == "job_timeout"

    def test_job_timeout_error_message_without_timeout(self) -> None:
        err = JobTimeoutError("abc")
        assert err.timeout is None

    def test_job_failed_error_carries_cause(self) -> None:
        cause = RuntimeError("boom")
        err = JobFailedError("xyz", cause=cause)
        assert err.cause is cause
        assert err.code == "job_failed"
        assert "xyz" in str(err)
