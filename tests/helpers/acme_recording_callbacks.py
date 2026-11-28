"""Job callbacks writing to a sink, listed as one module to be found by the scan."""

from __future__ import annotations

from typing import Any

from loom.core.job.callback import JobCallback

MODULE = __name__
"""The entry that makes the scan find the callbacks below."""


class AcmeCallbackSink:
    """What the recording callbacks write to."""

    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[str] = []


class AcmeRecordSuccessCallback(JobCallback):
    def __init__(self, sink: AcmeCallbackSink) -> None:
        self._sink = sink

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        self._sink.successes.append(f"{job_id}:{result}:{context.get('name', '')}")


class AcmeRecordFailureCallback(JobCallback):
    def __init__(self, sink: AcmeCallbackSink) -> None:
        self._sink = sink

    def on_failure(self, job_id: str, exc_type: str, exc_code: str | None, **context: Any) -> None:
        self._sink.failures.append(f"{job_id}:{exc_type}:{context.get('name', '')}:{exc_code}")
