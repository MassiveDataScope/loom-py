"""Tests for ``loom.prefect.notify`` notification subsystem."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from loom.prefect.notify import (
    NotifyEvent,
    SlackNotifier,
    SnsNotifier,
    build_notifiers,
)
from loom.prefect.notify import _sns as sns_module

_TOPIC = "arn:aws:sns:eu-west-1:123456789012:etl-failures"


def _make_event(
    state: str = "Failed", flow_name: str = "etl-x", message: str = "boom"
) -> NotifyEvent:
    return NotifyEvent(
        flow_name=flow_name,
        flow_run_name="run-1",
        flow_run_url="https://prefect/runs/1",
        state=state,
        correlation_id="corr-1",
        message=message,
    )


class TestSlackNotifier:
    def test_posts_to_webhook_on_matching_state(self) -> None:
        http = MagicMock()
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=True,
            http_post=http,
        )
        notifier.notify(_make_event(state="Failed"))
        assert http.call_count == 1
        url, _, kwargs = http.call_args.args[0], http.call_args.args, http.call_args.kwargs
        assert url == "https://hooks.slack.com/X"
        assert "etl-x" in kwargs["json"]["text"]
        assert "Failed" in kwargs["json"]["text"]

    def test_does_not_post_when_state_filter_excludes_event(self) -> None:
        http = MagicMock()
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=True,
            on_completion=False,
            http_post=http,
        )
        notifier.notify(_make_event(state="Completed"))
        assert http.call_count == 0

    def test_posts_on_completion_when_enabled(self) -> None:
        http = MagicMock()
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=False,
            on_completion=True,
            http_post=http,
        )
        notifier.notify(_make_event(state="Completed"))
        assert http.call_count == 1

    def test_renders_header_details_message_and_link(self) -> None:
        http = MagicMock()
        event = NotifyEvent(
            flow_name="etl-x",
            flow_run_name="run-1",
            flow_run_url="https://prefect/runs/1",
            state="Failed",
            correlation_id="corr-1",
            duration_seconds=65,
            env="production",
            message="boom",
        )
        SlackNotifier(webhook_url="https://hooks.slack.com/X", http_post=http).notify(event)
        assert http.call_args.kwargs["json"]["text"] == (
            ":x: *etl-x* → `Failed`\n"
            "run: `run-1`  env: `production`  correlation: `corr-1`  duration: `1m 05s`\n"
            "boom\n"
            "<https://prefect/runs/1|Open in Prefect>"
        )

    def test_swallows_http_failure(self) -> None:
        def _boom(*_: Any, **__: Any) -> None:
            raise RuntimeError("network down")

        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=True,
            http_post=_boom,
        )
        notifier.notify(_make_event(state="Failed"))  # must not raise


class TestBuildNotifiers:
    def test_returns_empty_when_block_missing(self) -> None:
        assert build_notifiers(None) == ()
        assert build_notifiers([]) == ()

    def test_builds_slack_notifier_from_yaml_block(self) -> None:
        block = [
            {
                "kind": "slack",
                "webhook_url": "https://hooks.slack.com/abc",
                "on_failure": True,
                "on_completion": False,
            }
        ]
        notifiers = build_notifiers(block)
        assert len(notifiers) == 1
        assert isinstance(notifiers[0], SlackNotifier)

    def test_rejects_unknown_kind(self) -> None:
        with pytest.raises(ValueError, match="unknown notifier kind"):
            build_notifiers([{"kind": "carrier-pigeon", "address": "north"}])

    def test_rejects_missing_kind(self) -> None:
        with pytest.raises(ValueError, match="kind"):
            build_notifiers([{"webhook_url": "x"}])

    def test_skips_slack_when_webhook_url_empty(self) -> None:
        block = [{"kind": "slack", "webhook_url": "", "on_failure": True}]
        assert build_notifiers(block) == ()

    def test_skips_slack_when_webhook_url_whitespace(self) -> None:
        block = [{"kind": "slack", "webhook_url": "   ", "on_failure": True}]
        assert build_notifiers(block) == ()

    def test_supports_multiple_notifiers(self) -> None:
        block = [
            {"kind": "slack", "webhook_url": "https://a", "on_failure": True},
            {"kind": "slack", "webhook_url": "https://b", "on_completion": True},
        ]
        notifiers = build_notifiers(block)
        assert len(notifiers) == 2


def _sent(client: MagicMock) -> dict[str, str]:
    return client.publish.call_args.kwargs


class TestSnsNotifier:
    def test_publishes_on_failure(self) -> None:
        client = MagicMock()
        SnsNotifier(topic_arn=_TOPIC, client=client).notify(_make_event(state="Failed"))
        client.publish.assert_called_once_with(
            TopicArn=_TOPIC,
            Subject="[Failed] etl-x",
            Message=(
                "etl-x -> Failed\n"
                "run: run-1\n"
                "correlation: corr-1\n"
                "\n"
                "boom\n"
                "\n"
                "Open in Prefect: https://prefect/runs/1"
            ),
        )

    def test_does_not_publish_on_completion_by_default(self) -> None:
        client = MagicMock()
        SnsNotifier(topic_arn=_TOPIC, client=client).notify(_make_event(state="Completed"))
        client.publish.assert_not_called()

    def test_publishes_on_completion_when_enabled(self) -> None:
        client = MagicMock()
        notifier = SnsNotifier(topic_arn=_TOPIC, on_completion=True, client=client)
        notifier.notify(_make_event(state="Completed"))
        assert _sent(client)["Subject"] == "[Completed] etl-x"

    def test_subject_is_cut_to_99_characters(self) -> None:
        client = MagicMock()
        notifier = SnsNotifier(topic_arn=_TOPIC, client=client)
        notifier.notify(_make_event(flow_name="f" * 150))
        assert _sent(client)["Subject"] == ("[Failed] " + "f" * 150)[:99]

    def test_message_keeps_the_end_of_a_huge_error(self) -> None:
        client = MagicMock()
        notifier = SnsNotifier(topic_arn=_TOPIC, client=client)
        notifier.notify(_make_event(message="Traceback\n" + "x" * 100_000 + "ValueError: end"))
        message = _sent(client)["Message"]
        assert "Traceback" not in message
        assert "x" * 59_985 + "ValueError: end\n" in message
        assert len(message) < 60_100

    def test_swallows_and_logs_a_client_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        log = MagicMock()
        monkeypatch.setattr(sns_module, "_log", log)
        client = MagicMock()
        client.publish.side_effect = ClientError(
            {"Error": {"Code": "AuthorizationError"}}, "Publish"
        )
        SnsNotifier(topic_arn=_TOPIC, client=client).notify(_make_event(state="Failed"))
        log.warning.assert_called_once_with(
            "SNS publish failed", flow="etl-x", run="run-1", state="Failed", exc_info=True
        )

    def test_default_client_uses_the_topic_region(self, monkeypatch: pytest.MonkeyPatch) -> None:
        factory = MagicMock()
        monkeypatch.setattr(sns_module.boto3, "client", factory)
        SnsNotifier(topic_arn=_TOPIC)
        factory.assert_called_once_with("sns", region_name="eu-west-1")


class TestBuildSnsNotifier:
    def test_builds_sns_notifier_from_yaml_block(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sns_module.boto3, "client", MagicMock())
        block = [{"kind": "sns", "topic_arn": _TOPIC, "on_failure": True}]
        (notifier,) = build_notifiers(block)
        assert isinstance(notifier, SnsNotifier)

    @pytest.mark.parametrize("topic_arn", ["", "   ", None])
    def test_skips_sns_when_topic_arn_empty(self, topic_arn: str | None) -> None:
        assert build_notifiers([{"kind": "sns", "topic_arn": topic_arn}]) == ()
