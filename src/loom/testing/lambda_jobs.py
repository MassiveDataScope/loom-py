"""Run a Lambda ``jobs`` function in process, over the queue it declares, in moto.

Needs the ``testing-aws`` extra (moto). Usage::

    with LambdaWorker("config.yaml", function="worker", environment="test") as worker:
        result = worker(LambdaEvent.jobs(SendEmailJob, {"to": "ada@example.com"}), None)
        assert result == {"batchItemFailures": []}
        assert worker.sqs.receive_message(QueueUrl=worker.dead_letter_url).get("Messages") is None
"""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Any

import msgspec

from loom.composition import Application, Project
from loom.core.connections.aws import AwsClientConfig, AwsClients
from loom.core.plugins.optional import import_optional
from loom.jobs.aws.queue import DeadLetterPolicy
from loom.jobs.aws.sqs import SQSConfig
from loom.serverless.aws.lambda_.functions import JobsTrigger, served_trigger
from loom.serverless.aws.lambda_.handler import LambdaAdapter, LambdaHandler
from loom.testing.overrides import Override, OverrideAdapter

_ISLAND = "loom.testing._moto"
_EXTRA = "testing-aws"
MAX_RECEIVE_COUNT = 3
"""The ``maxReceiveCount`` of the dead-letter policy of a ``redrive`` queue."""


class _MotoClients(AwsClients):
    """The application's AWS clients under moto's mock: a dummy region when none is set."""

    def __init__(self, region: str) -> None:
        super().__init__()
        self._region = region

    def client(self, service: str, config: AwsClientConfig) -> Any:
        return super().client(
            service, msgspec.structs.replace(config, region=config.region or self._region)
        )


class LambdaWorker:
    """The handler of a Lambda ``jobs`` function, composed over a moto SQS.

    The harness starts moto's AWS mock, creates the queue the function consumes
    (its ``sqs.queues`` entry, as a queue named like its URL) and, for a ``redrive``
    queue, a dead-letter queue with a real ``RedrivePolicy``, then binds the
    function. No region, credentials or patch of boto3 is needed. Closing the
    worker closes the function and stops moto.

    Args:
        config: A configuration path, or none for the default config directory.
        function: The function of ``lambda.functions`` to serve.
        overrides: Substitutions of the composed application.
        **options: Keyword arguments of ``Project.load`` (``environment``, ...).

    Attributes:
        sqs: A boto3 ``sqs`` client of the mock, to read what the function left in
            the queues.
        queue_url: The URL of the queue the function consumes, in the mock.
        dead_letter_url: The URL of its dead-letter queue; ``None`` on a ``discard`` queue.
        application: The composed application, to reach its container.

    Raises:
        MissingExtraError: When moto is not installed, naming the ``testing-aws`` extra.
    """

    def __init__(
        self,
        config: str | None = None,
        *,
        function: str,
        overrides: Sequence[Override] = (),
        **options: Any,
    ) -> None:
        island = import_optional(_ISLAND, extra=_EXTRA)
        project = Project.load(*(() if config is None else (config,)), **options)
        _, trigger = served_trigger(project.config, function, project.app.entry)
        assert isinstance(trigger, JobsTrigger) and trigger.queue is not None
        queue = SQSConfig.load(project.config).queues[trigger.queue.name]
        self._mock = island.start()
        try:
            clients = _MotoClients(island.REGION)
            self.sqs = clients.client("sqs", AwsClientConfig())
            self.queue_url, self.dead_letter_url = island.declare_queue(
                self.sqs,
                queue.url.rstrip("/").rsplit("/", 1)[-1],
                redrive=queue.dead_letter is DeadLetterPolicy.REDRIVE,
                max_receive_count=MAX_RECEIVE_COUNT,
            )
            self._adapter = LambdaAdapter(function=function)
            self.application: Application = project.compose(
                OverrideAdapter(
                    self._adapter,
                    (Override.instance(AwsClients, clients), *overrides),
                )
            )
            self._handler: LambdaHandler = self._adapter.bind(self.application)
        except BaseException:
            self._mock.stop()
            raise

    def __call__(self, event: dict[str, Any], context: Any = None) -> dict[str, Any]:
        """Answer *event*, a batch of SQS records, with the handler's ``batchItemFailures``."""
        result: dict[str, Any] = self._handler(event, context)
        return result

    def close(self) -> None:
        """Close the function and stop moto; a second call does nothing."""
        self._handler.close()
        self._mock.stop()

    def __enter__(self) -> LambdaWorker:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


__all__ = ["LambdaWorker"]
