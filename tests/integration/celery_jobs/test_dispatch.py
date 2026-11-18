"""A dispatch through ``CeleryJobService`` reaches the worker and calls the right callback."""

from __future__ import annotations

from loom.celery.service import CeleryJobService
from loom.core.job.callback import DispatchableCallbacks
from tests.integration.celery_jobs.conftest import JSON, Workbench
from tests.integration.celery_jobs.doubles import (
    REJECTION,
    RecordingCallback,
    RejectedJob,
    SucceedingJob,
)


@JSON
class TestDispatchThroughTheService:
    async def test_a_succeeding_job_calls_the_success_link_with_its_result(
        self, served: Workbench
    ) -> None:
        service = CeleryJobService(served.app, callbacks=DispatchableCallbacks([RecordingCallback]))

        handle = await service.dispatch_now(
            SucceedingJob,
            on_success=RecordingCallback,
            on_failure=RecordingCallback,
            queue=served.queue,
        )
        stored, calls = served.settle(handle.job_id, notified=1)

        assert stored.state == "SUCCESS"
        assert calls == [{"job_id": handle.job_id, "hook": "on_success", "result": "done"}]

    async def test_a_rejected_job_calls_the_failure_callback_the_dispatch_named(
        self, served: Workbench
    ) -> None:
        service = CeleryJobService(served.app, callbacks=DispatchableCallbacks([RecordingCallback]))

        handle = await service.dispatch_now(
            RejectedJob,
            on_success=RecordingCallback,
            on_failure=RecordingCallback,
            queue=served.queue,
        )
        stored, calls = served.settle(handle.job_id, notified=1)

        assert stored.state == "FAILURE"
        assert calls == [
            {
                "job_id": handle.job_id,
                "hook": "on_failure",
                "exc_type": "JobRejection",
                "exc_msg": REJECTION,
            }
        ]
