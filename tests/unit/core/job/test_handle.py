from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.core.errors import JobFailedError, JobTimeoutError
from loom.core.job.handle import JobGroup, JobHandle


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _inline_handle(result: Any, job_id: str = "test-id") -> JobHandle[Any]:
    holder: list[Any] = [result]
    return JobHandle(
        job_id=job_id, queue="default", dispatched_at=_now(), _inline_result_holder=holder
    )


# ---------------------------------------------------------------------------
# JobHandle — inline mode
# ---------------------------------------------------------------------------


def test_wait_inline_returns_result() -> None:
    handle = _inline_handle(42)
    assert handle.wait() == 42


def test_wait_inline_returns_none_result() -> None:
    handle = _inline_handle(None)
    assert handle.wait() is None


def test_wait_inline_returns_string_result() -> None:
    handle = _inline_handle("hello")
    assert handle.wait() == "hello"


# ---------------------------------------------------------------------------
# JobHandle — Celery (async_result) mode
# ---------------------------------------------------------------------------


def test_wait_async_result_delegates_to_get() -> None:
    mock_result = MagicMock()
    mock_result.get.return_value = 99
    handle = JobHandle(job_id="x", queue="q", dispatched_at=_now(), _async_result=mock_result)

    assert handle.wait(timeout=5.0) == 99
    mock_result.get.assert_called_once_with(timeout=5.0)


def test_wait_async_timeout_raises_job_timeout_error() -> None:
    class TimeoutError(Exception):  # simulates celery.exceptions.TimeoutError
        pass

    mock_result = MagicMock()
    mock_result.get.side_effect = TimeoutError()
    handle = JobHandle(job_id="tid", queue="q", dispatched_at=_now(), _async_result=mock_result)

    with pytest.raises(JobTimeoutError) as exc_info:
        handle.wait(timeout=2.0)

    assert exc_info.value.job_id == "tid"
    assert exc_info.value.timeout == pytest.approx(2.0)


def test_wait_async_failure_raises_job_failed_error() -> None:
    mock_result = MagicMock()
    mock_result.get.side_effect = ValueError("something went wrong")
    handle = JobHandle(job_id="fid", queue="q", dispatched_at=_now(), _async_result=mock_result)

    with pytest.raises(JobFailedError) as exc_info:
        handle.wait()

    assert exc_info.value.job_id == "fid"
    assert isinstance(exc_info.value.cause, ValueError)


# ---------------------------------------------------------------------------
# JobHandle — identity and immutability
# ---------------------------------------------------------------------------


def test_job_handle_is_frozen() -> None:
    handle = _inline_handle(1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        handle.job_id = "other"  # type: ignore[misc]


def test_job_handle_equality_by_job_id_and_queue() -> None:
    ts = _now()
    h1 = JobHandle(job_id="a", queue="q", dispatched_at=ts)
    h2 = JobHandle(job_id="a", queue="q", dispatched_at=ts)
    assert h1 == h2


# ---------------------------------------------------------------------------
# JobGroup
# ---------------------------------------------------------------------------


def test_wait_all_returns_results_in_order() -> None:
    h1 = _inline_handle(10, "j1")
    h2 = _inline_handle(20, "j2")
    group = JobGroup(handles=(h1, h2))

    assert group.wait_all() == [10, 20]


def test_wait_all_single_handle() -> None:
    group = JobGroup(handles=(_inline_handle("ok"),))
    assert group.wait_all() == ["ok"]


def test_then_raises_not_implemented() -> None:
    group = JobGroup(handles=(_inline_handle(1),))
    with pytest.raises(NotImplementedError):
        group.then(object)


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------


def test_job_timeout_error_message_with_timeout() -> None:
    err = JobTimeoutError("abc", timeout=10.0)
    assert "abc" in str(err)
    assert "10.0" in str(err)
    assert err.code == "job_timeout"


def test_job_timeout_error_message_without_timeout() -> None:
    err = JobTimeoutError("abc")
    assert err.timeout is None


def test_job_failed_error_carries_cause() -> None:
    cause = RuntimeError("boom")
    err = JobFailedError("xyz", cause=cause)
    assert err.cause is cause
    assert err.code == "job_failed"
    assert "xyz" in str(err)
