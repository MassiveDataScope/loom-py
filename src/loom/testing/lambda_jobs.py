"""Run a Lambda ``jobs`` function in process, over the queue it declares, in moto.

Needs the ``testing-aws`` extra (moto). Usage::

    with LambdaWorker("config.yaml", function="worker", environment="test") as worker:
        result = worker.deliver(SendEmailJob, {"to": "ada@example.com"})
        assert result == {"batchItemFailures": []}
        assert worker.redeliver() == {"batchItemFailures": []}  # nothing is left to run
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from types import TracebackType
from typing import Any

import msgspec

from loom.composition import Application, Project
from loom.core.connections.aws import AwsClientConfig, AwsClients
from loom.core.identity import Identity
from loom.core.job.envelope import encode_job_message
from loom.core.job.job import Job
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
            arn = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url, AttributeNames=["QueueArn"]
            )
            self._queue_arn: str = arn["Attributes"]["QueueArn"]
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

    def deliver(
        self,
        job_type: type[Job[Any]],
        payload: dict[str, Any] | None = None,
        *,
        params: dict[str, Any] | None = None,
        identity: Identity | None = None,
    ) -> dict[str, Any]:
        """Put *job_type* on the queue, as ``SQSJobService`` writes it, and deliver it.

        Args:
            job_type: The job to run.
            payload: Its payload.
            params: Its parameters.
            identity: The caller it runs as.

        Returns:
            What :meth:`redeliver` returns for the message just sent.
        """
        body = encode_job_message(
            job_type,
            payload=payload,
            params=params,
            identity=identity,
            task_id=str(uuid.uuid4()),
        ).decode()
        request: dict[str, Any] = {"QueueUrl": self.queue_url, "MessageBody": body}
        if self.queue_url.endswith(".fifo"):
            group = (payload or {}).get(job_type.__group_by__ or "", "loom-test")
            request |= {"MessageGroupId": str(group), "MessageDeduplicationId": uuid.uuid4().hex}
        self.sqs.send_message(**request)
        return self.redeliver()

    def redeliver(self) -> dict[str, Any]:
        """Receive whatever the queue shows and invoke the function with it.

        The records carry the real receipt handle and ``ApproximateReceiveCount`` of
        the delivery, so a failed message comes back when its visibility timeout ends,
        counted. As the event source mapping does, it then deletes every message the
        result does not report.

        Returns:
            The handler's ``batchItemFailures`` for the batch received.
        """
        received = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        ).get("Messages", [])
        result = self({"Records": [self._record(message) for message in received]})
        reported = {item["itemIdentifier"] for item in result["batchItemFailures"]}
        for message in received:
            if message["MessageId"] not in reported:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
                )
        return result

    def _record(self, message: dict[str, Any]) -> dict[str, Any]:
        attributes = {
            "ApproximateReceiveCount": message["Attributes"]["ApproximateReceiveCount"],
            **{k: v for k, v in message["Attributes"].items() if k == "MessageGroupId"},
        }
        return {
            "messageId": message["MessageId"],
            "receiptHandle": message["ReceiptHandle"],
            "body": message["Body"],
            "attributes": attributes,
            "messageAttributes": {
                name: {"stringValue": value.get("StringValue"), "dataType": value["DataType"]}
                for name, value in message.get("MessageAttributes", {}).items()
            },
            "eventSource": "aws:sqs",
            "eventSourceARN": self._queue_arn,
        }

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
