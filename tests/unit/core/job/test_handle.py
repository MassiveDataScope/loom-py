from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any

import pytest

from loom.core.errors import JobFailedError, JobTimeoutError
from loom.core.job.handle import InlineOutcome, JobGroup, JobHandle


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
