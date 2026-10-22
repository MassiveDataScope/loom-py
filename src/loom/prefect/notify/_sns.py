"""AWS SNS topic notifier."""

from __future__ import annotations

from typing import Any

from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]

from loom.core.connections.aws import AwsClientConfig
from loom.core.connections.clients.aws import build_aws_client
from loom.core.logger import get_logger
from loom.prefect.notify._event import NotifyEvent
from loom.prefect.notify._format import run_details, should_notify

_ERROR_MAX_CHARS = 60_000

_log = get_logger(__name__)


class SnsNotifier:
    """Publish terminal flow events to an AWS SNS topic.

    Args:
        topic_arn: ARN of the topic to publish to; its region is the client's.
        on_failure: Publish when state is ``"Failed"`` or ``"Crashed"``.
        on_completion: Publish when state is ``"Completed"``.
        client: boto3 ``sns`` client. Defaults to one for the topic's region.
    """

    def __init__(
        self,
        *,
        topic_arn: str,
        on_failure: bool = True,
        on_completion: bool = False,
        client: Any = None,
    ) -> None:
        self._topic_arn = topic_arn
        self._on_failure = on_failure
        self._on_completion = on_completion
        self._client = (
            client
            if client is not None
            else build_aws_client("sns", AwsClientConfig(region=topic_arn.split(":")[3]))
        )

    def notify(self, event: NotifyEvent) -> None:
        """Publish *event* to the topic when filters match."""
        if not should_notify(
            event.state, on_failure=self._on_failure, on_completion=self._on_completion
        ):
            return
        try:
            self._client.publish(
                TopicArn=self._topic_arn,
                Subject=f"[{event.state}] {event.flow_name}"[:99],
                Message=_render_message(event),
            )
        except (BotoCoreError, ClientError):
            _log.warning(
                "SNS publish failed",
                flow=event.flow_name,
                run=event.flow_run_name,
                state=event.state,
                exc_info=True,
            )


def _render_message(event: NotifyEvent) -> str:
    lines = [f"{event.flow_name} -> {event.state}"]
    lines.extend(f"{label}: {value}" for label, value in run_details(event))
    if event.message:
        lines.extend(["", event.message[-_ERROR_MAX_CHARS:]])
    if event.flow_run_url:
        lines.extend(["", f"Open in Prefect: {event.flow_run_url}"])
    return "\n".join(lines)


__all__ = ["SnsNotifier"]
