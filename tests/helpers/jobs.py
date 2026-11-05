"""Jobs and sources the job service and job routing tests share."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, cast

from loom.composition import AppConfig, Application, CompositionSource, Recipe
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.job import Job
from loom.core.persistence.profile import WORKER_PROFILE
from loom.jobs.aws.sqs import SQSConfig, SQSJobService
from tests.helpers.composition import composition_source

BROKER = {"broker_url": "memory://", "result_backend": "cache+memory://"}
SQS_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/emails"
SQS_FIFO_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo"


class EmailJob(Job[str]):
    __queue__ = "emails"

    def execute(self) -> str:
        return "email"


class JobsAdapter:
    """Compose the given jobs, as a worker would, and mount nothing."""

    def __init__(self, *jobs: type[Job[Any]]) -> None:
        self._jobs = jobs

    def recipe(self, source: CompositionSource) -> Recipe:
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(
                models=(), use_cases=cast(Any, self._jobs), interfaces=(), jobs=self._jobs
            ),
            connection_only=True,
        )

    def bind(self, application: Application) -> Application:
        return application


def job_source(
    tmp_path: Path, config: dict[str, Any], job_service: str | None = None
) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict(config),
        app=AppConfig(name="jobs", job_service=job_service),
        code_path=tmp_path,
        config_root=tmp_path,
    )


class SQSClient:
    """Stands in for the boto3 ``sqs`` client.

    ``requests`` holds each message sent, one ``send_message`` or one batch
    entry; ``batches`` each ``send_message_batch`` call as its queue and ids.
    ``refuse`` counts how many more times SQS refuses a task's batch entry.
    """

    def __init__(self, error: Exception | None = None, *, overlap: float = 0) -> None:
        self.requests: list[dict[str, Any]] = []
        self.batches: list[tuple[str, list[str]]] = []
        self.refuse: dict[str, int] = {}
        self.sent = threading.Event()
        self.peak = 0
        self.entered = threading.Event()
        self.hold: threading.Event | None = None
        self._error = error
        self._overlap = overlap
        self._in_flight = 0
        self._lock = threading.Lock()
        self._other = threading.Condition(self._lock)

    def send_message(self, **request: Any) -> dict[str, str]:
        if self._error is not None:
            raise self._error
        self.requests.append(request)
        self.sent.set()
        return {"MessageId": "m-1"}

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:  # noqa: N803
        self.entered.set()
        if self.hold is not None:
            self.hold.wait(5)
        self._wait_for_another_call()
        self.batches.append((QueueUrl, [entry["Id"] for entry in Entries]))
        if self._error is not None:
            raise self._error
        failed = []
        for entry in Entries:
            if self.refuse.get(entry["Id"], 0) > 0:
                self.refuse[entry["Id"]] -= 1
                failed.append({"Id": entry["Id"], "Code": "InternalError", "Message": "hunter2"})
            else:
                self.requests.append({"QueueUrl": QueueUrl} | entry)
        self.sent.set()
        return {"Successful": [], "Failed": failed}

    def _wait_for_another_call(self) -> None:
        """Hold the call up to ``overlap`` seconds while another one starts: records the peak."""
        with self._other:
            self._in_flight += 1
            self.peak = max(self.peak, self._in_flight)
            self._other.notify_all()
            self._other.wait_for(lambda: self._in_flight > 1, timeout=self._overlap)
            self._in_flight -= 1


def sqs_service(client: SQSClient, **knobs: Any) -> SQSJobService:
    """An ``SQSJobService`` over *client* sending ``emails`` and the FIFO ``orders``."""
    config = SQSConfig(queues={"emails": SQS_URL, "orders": SQS_FIFO_URL}, **knobs)
    return SQSJobService(client, config, factory=cast(Any, None), executor=cast(Any, None))
