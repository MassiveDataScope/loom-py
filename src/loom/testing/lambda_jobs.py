"""Run a Lambda ``jobs`` function in process, over the queue its configuration declares, in moto.

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

from loom.composition import Application, Project
from loom.core.config import ConfigError
from loom.core.connections.aws import AwsClients
from loom.core.identity import Identity
from loom.core.job.envelope import encode_job_message
from loom.core.job.job import Job
from loom.core.job.policy import Countdown
from loom.core.plugins.optional import import_optional
from loom.jobs.aws.queue import DeadLetterPolicy, OutgoingMessage, QueueConfig, SqsQueue
from loom.jobs.aws.sqs import SQSConfig
from loom.serverless.aws.lambda_.functions import JobsTrigger, served_trigger
from loom.serverless.aws.lambda_.handler import LambdaAdapter
from loom.testing.overrides import Override, OverrideAdapter

_ISLAND = "loom.testing._moto"
_EXTRA = "testing-aws"
_MAX_RECEIVE_COUNT = 3
_RECEIVE_LIMIT = 10
"""The most messages one ``ReceiveMessage`` returns."""


class LambdaWorker:
    """The handler of a Lambda ``jobs`` function, composed over a moto SQS.

    The harness enters moto's AWS mock and creates the queue the function consumes,
    and for a ``redrive`` queue its dead-letter queue with a real ``RedrivePolicy``,
    through the application's own ``sqs`` client: botocore resolves the region as it
    does for the application (``aws.region``, a profile, ``AWS_DEFAULT_REGION``) and
    moto's account is ``MOTO_ACCOUNT_ID`` or its default. The harness sets and
    restores no environment, and the queue must come out at the configured URL.
    Inside a user's own ``mock_aws()`` the worker joins it, and closing it leaves that
    one running.

    Args:
        config: A configuration path, or none for the default config directory.
        function: The ``jobs`` function of ``lambda.functions`` to serve.
        overrides: Substitutions of the composed application.
        **options: Keyword arguments of ``Project.load`` (``environment``, ...).

    Attributes:
        sqs: A boto3 ``sqs`` client of the mock, to read what the function left in
            the queues.
        queue_url: The URL of the queue the function consumes: the configured one.
        dead_letter_url: The URL of its dead-letter queue; ``None`` on a ``discard`` queue.
        application: The composed application, to reach its container.

    Raises:
        MissingExtraError: When moto is not installed, naming the ``testing-aws`` extra.
        ConfigError: When *function* is no jobs function, its queue is not mapped or its URL
            is no AWS SQS one, a request of the application's ``sqs`` client goes
            unanswered by moto (an endpoint override, FIPS, dualstack: nothing is ever sent
            to the network), or the queue moto makes is of another region or account than
            the configured URL: the message names what differs and the way out.
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
        if not isinstance(trigger, JobsTrigger) or trigger.queue is None:
            raise ConfigError(f"lambda.functions.{function} is not a jobs function")
        sqs_config = SQSConfig.load(project.config)
        entry = sqs_config.queues.get(trigger.queue.name)
        if entry is None:
            raise ConfigError(f"sqs.queues.{trigger.queue.name} is missing")
        if entry.region is None:
            raise ConfigError(
                f"queue URL {entry.url} is not an AWS SQS URL "
                "(https://sqs.<region>.amazonaws.com/<account>/<queue>): "
                "use loom.testing.TEST_QUEUE or a URL of that shape"
            )
        self._batch_size = min(trigger.queue.batch_size or _RECEIVE_LIMIT, _RECEIVE_LIMIT)
        self._island = island
        self._closed = False
        self._bound = False
        self._aws: AwsClients | None = None
        self._adapter = LambdaAdapter(function=function)
        self._mock = island.start()
        try:
            self.application: Application = project.compose(
                OverrideAdapter(self._adapter, tuple(overrides))
            )
            self._aws = self.application.kernel.container.resolve(AwsClients)
            self.sqs = self._aws.client("sqs", sqs_config)
            self.sqs.meta.events.register_last("before-send", self._unanswered)
            self.queue_url, self.dead_letter_url = self._declare(island, entry)
            self._queue = SqsQueue(self.sqs, trigger.queue.name, entry)
            arn = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url, AttributeNames=["QueueArn"]
            )
            self._queue_arn: str = arn["Attributes"]["QueueArn"]
            self._handler = self._adapter.bind(self.application)
            self._bound = True
        except BaseException:
            self.close()
            raise

    def __call__(self, event: dict[str, Any], context: Any = None) -> dict[str, Any]:
        """Answer *event*, a batch of SQS records, with the handler's ``batchItemFailures``.

        A raw record's receipt handle is not in the queue, so moto refuses the calls that
        settle it; :meth:`deliver` and :meth:`redeliver` build records that can be settled.
        """
        result: dict[str, Any] = self._handler(event, context)
        return result

    def deliver(
        self,
        job_type: type[Job[Any]],
        payload: dict[str, Any] | None = None,
        *,
        params: dict[str, Any] | None = None,
        identity: Identity | None = None,
    ) -> dict[str, Any]:
        """Put *job_type* on the queue as ``SQSJobService`` sends it, and deliver it.

        The envelope is the one ``SQSJobService`` writes, placed by the queue's own
        rule (hashed group id and the task id as deduplication id on a FIFO queue).

        Args:
            job_type: The job to run.
            payload: Its payload.
            params: Its parameters.
            identity: The caller it runs as.

        Returns:
            What :meth:`redeliver` returns for the message just sent.

        Raises:
            SQSDispatchError: As the queue's placement refuses a FIFO message.
        """
        task_id = str(uuid.uuid4())
        body = encode_job_message(
            job_type, payload=payload, params=params, identity=identity, task_id=task_id
        ).decode()
        message = OutgoingMessage(
            id=task_id,
            body=body,
            **self._queue.placement(job_type, payload or {}, task_id, Countdown(0)),
        )
        self.sqs.send_message(QueueUrl=self.queue_url, **message.request())
        return self.redeliver()

    def redeliver(self) -> dict[str, Any]:
        """Receive what the queue shows, up to ``batch_size``, and invoke the function.

        The records carry the real receipt handle and ``ApproximateReceiveCount`` of the
        delivery, so a failed message comes back when its visibility timeout ends,
        counted. As the event source mapping does, it deletes every message the result
        does not report. Nothing visible means no invocation.

        Returns:
            The handler's ``batchItemFailures`` for the batch received; empty when none.
        """
        received = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self._batch_size,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        ).get("Messages", [])
        if not received:
            return {"batchItemFailures": []}
        result = self({"Records": [self._record(message) for message in received]})
        reported = {item["itemIdentifier"] for item in result["batchItemFailures"]}
        for message in received:
            if message["MessageId"] not in reported:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
                )
        return result

    def close(self) -> None:
        """Close the function, its clients and stop the mock, once; a second call does nothing."""
        if self._closed:
            return
        self._closed = True
        try:
            self._adapter.close()
        finally:
            try:
                if not self._bound and self._aws is not None:
                    self._aws.close()
            finally:
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

    def _unanswered(self, request: Any, **_: Any) -> Any:
        """A ``before-send`` handler that runs after moto's: what moto did not answer is refused."""
        endpoint = self.sqs.meta.endpoint_url
        return self._island.unanswered(
            request,
            f"moto did not answer the request to {endpoint}, so it would have left the "
            "process: remove aws.url and sqs.url from the configuration, AWS_ENDPOINT_URL, "
            "AWS_ENDPOINT_URL_SQS, AWS_USE_FIPS_ENDPOINT and AWS_USE_DUALSTACK_ENDPOINT from "
            "the environment, and any endpoint_url of the AWS profile",
        )

    def _declare(self, island: Any, entry: QueueConfig) -> tuple[str, str | None]:
        """Create the queue and its dead-letter queue, and check they are where *entry* says.

        Raises:
            ConfigError: When a request went unanswered, or the queue made is of another
                region or account than the configured URL.
        """
        try:
            queue_url, dead_letter_url = island.declare_queue(
                self.sqs,
                entry.queue_name,
                redrive=entry.dead_letter is DeadLetterPolicy.REDRIVE,
                max_receive_count=_MAX_RECEIVE_COUNT,
            )
        except self.sqs.exceptions.ClientError as exc:
            error = exc.response["Error"]
            if error["Code"] == island.UNANSWERED:
                raise ConfigError(error["Message"]) from exc
            raise
        made = QueueConfig(url=queue_url)
        differs = [
            f"{fact} {wanted!r} against {got!r}"
            for fact, wanted, got in (
                ("region", entry.region, self.sqs.meta.region_name),
                ("account", entry.account_id, made.account_id),
            )
            if wanted != got
        ]
        if differs:
            raise ConfigError(
                f"moto made the queue {queue_url} for the configured {entry.url} "
                f"({'; '.join(differs)}): set aws.region or AWS_DEFAULT_REGION to the URL's "
                "region, MOTO_ACCOUNT_ID to its account, or use loom.testing.TEST_QUEUE"
            )
        return queue_url, dead_letter_url

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


__all__ = ["LambdaWorker"]
