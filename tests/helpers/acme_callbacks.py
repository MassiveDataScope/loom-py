"""Job callbacks a test lists in ``app.packages`` as one module, to be found by the scan."""

from __future__ import annotations

from typing import Any, ClassVar

from loom.core.job.callback import JobCallback

MODULE = __name__
"""The entry that makes the scan find the callbacks below."""


class AcmePingCallback(JobCallback):
    """Overrides both hooks, doing nothing."""

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        del job_id, result, context

    def on_failure(self, job_id: str, exc_type: str, exc_code: str | None, **context: Any) -> None:
        del job_id, exc_type, exc_code, context


class AcmeSyncCallback(JobCallback):
    """Sync hooks recording the outcome they ran for."""

    calls: ClassVar[list[str]] = []

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        self.calls.append("success")

    def on_failure(self, job_id: str, exc_type: str, exc_code: str | None, **context: Any) -> None:
        self.calls.append("failure")
