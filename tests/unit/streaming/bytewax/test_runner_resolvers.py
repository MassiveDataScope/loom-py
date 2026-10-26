"""Tests for ``resolvers=`` and the built-in resolver defaults in ``StreamingRunner``."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from bytewax.dataflow import Dataflow  # noqa: E402

from loom.core.config import ConfigContext, ConfigError  # noqa: E402
from loom.streaming.bytewax.runner import StreamingRunner  # noqa: E402
from loom.streaming.graph._flow import StreamFlow  # noqa: E402
from tests.helpers.extras import with_boto3, without_boto3  # noqa: E402
from tests.unit._resolver_stubs import MappingResolver  # noqa: E402
from tests.unit.streaming.bytewax.cases import Order, Result  # noqa: E402

pytestmark = pytest.mark.usefixtures("clear_builtin_resolvers")


def _write_streaming_yaml(tmp_path: Path, workers: str) -> str:
    path = tmp_path / "streaming.yaml"
    path.write_text(
        "kafka:\n"
        "  consumer:\n"
        '    brokers: ["localhost:9092"]\n'
        '    group_id: "test"\n'
        "    delivery: at_most_once\n"
        '    topics: ["orders.in"]\n'
        "  producer:\n"
        '    brokers: ["localhost:9092"]\n'
        '    client_id: "test-producer"\n'
        '    topic: "orders.out"\n'
        "streaming:\n"
        "  runtime:\n"
        f"    workers_per_process: {workers}\n",
        encoding="utf-8",
    )
    return str(path)


def _without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    without_boto3(monkeypatch)


def _stub_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_prepare_run(plan: object, **kwargs: Any) -> object:
        del plan, kwargs
        return SimpleNamespace(dataflow=Dataflow("test"), shutdown=lambda: None)

    monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)
    monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", lambda *_a, **_kw: None)


class TestFromYaml:
    def test_user_resolver_reaches_the_runtime(
        self, bytewax_stream_flow: StreamFlow[Order, Result], tmp_path: Path
    ) -> None:
        config_path = _write_streaming_yaml(tmp_path, "${stub:workers}")

        runner = StreamingRunner.from_yaml(
            bytewax_stream_flow, config_path, resolvers=[MappingResolver("stub", {"workers": 3})]
        )

        assert runner._runtime.workers_per_process == 3

    def test_secrets_placeholder_without_boto3_names_the_extra(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _without_boto3(monkeypatch)
        config_path = _write_streaming_yaml(tmp_path, "${secrets:/stream/workers}")

        with pytest.raises(ConfigError, match=r"loom-kernel\[config-ssm\]"):
            StreamingRunner.from_yaml(bytewax_stream_flow, config_path)

    def test_no_aws_client_without_placeholders(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        boto3 = MagicMock()
        with_boto3(monkeypatch, boto3)
        config_path = _write_streaming_yaml(tmp_path, "2")

        runner = StreamingRunner.from_yaml(bytewax_stream_flow, config_path)

        assert runner._runtime.workers_per_process == 2
        boto3.client.assert_not_called()

    def test_user_secrets_resolver_wins_over_the_default(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _without_boto3(monkeypatch)
        config_path = _write_streaming_yaml(tmp_path, "${secrets:/stream/workers}")
        vault = MappingResolver("secrets", {"/stream/workers": 4})

        runner = StreamingRunner.from_yaml(bytewax_stream_flow, config_path, resolvers=[vault])

        assert runner._runtime.workers_per_process == 4


class TestRunResolversRequireConfigPath:
    def test_resolvers_with_config_instead_of_config_path_is_rejected(
        self, bytewax_stream_flow: StreamFlow[Order, Result]
    ) -> None:
        config = ConfigContext.from_dict({})
        runner = StreamingRunner()
        resolvers = [MappingResolver("stub", {"workers": 3})]

        with pytest.raises(ValueError, match="resolvers apply only with config_path"):
            runner.run(flow=bytewax_stream_flow, config=config, resolvers=resolvers)

    def test_config_without_resolvers_still_runs(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _stub_execution(monkeypatch)
        config = ConfigContext.from_dict(
            {
                "kafka": {
                    "consumer": {
                        "brokers": ["localhost:9092"],
                        "group_id": "test",
                        "delivery": "at_most_once",
                        "topics": ["orders.in"],
                    },
                    "producer": {
                        "brokers": ["localhost:9092"],
                        "client_id": "test-producer",
                        "topic": "orders.out",
                    },
                },
                "streaming": {"runtime": {"workers_per_process": 2}},
            }
        )
        runner = StreamingRunner()

        runner.run(flow=bytewax_stream_flow, config=config)

        assert runner._runtime.workers_per_process == 2


class TestRunWithConfigPath:
    def test_user_resolver_reaches_the_runtime(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _stub_execution(monkeypatch)
        config_path = _write_streaming_yaml(tmp_path, "${stub:workers}")
        runner = StreamingRunner()

        runner.run(
            flow=bytewax_stream_flow,
            config_path=config_path,
            resolvers=[MappingResolver("stub", {"workers": 3})],
        )

        assert runner._runtime.workers_per_process == 3

    def test_secrets_placeholder_without_boto3_names_the_extra(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _without_boto3(monkeypatch)
        _stub_execution(monkeypatch)
        config_path = _write_streaming_yaml(tmp_path, "${secrets:/stream/workers}")
        runner = StreamingRunner()

        with pytest.raises(ConfigError, match=r"loom-kernel\[config-ssm\]"):
            runner.run(flow=bytewax_stream_flow, config_path=config_path)

    def test_no_aws_client_without_placeholders(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        boto3 = MagicMock()
        with_boto3(monkeypatch, boto3)
        _stub_execution(monkeypatch)
        config_path = _write_streaming_yaml(tmp_path, "2")
        runner = StreamingRunner()

        runner.run(flow=bytewax_stream_flow, config_path=config_path)

        assert runner._runtime.workers_per_process == 2
        boto3.client.assert_not_called()

    def test_user_secrets_resolver_wins_over_the_default(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _without_boto3(monkeypatch)
        _stub_execution(monkeypatch)
        config_path = _write_streaming_yaml(tmp_path, "${secrets:/stream/workers}")
        runner = StreamingRunner()

        runner.run(
            flow=bytewax_stream_flow,
            config_path=config_path,
            resolvers=[MappingResolver("secrets", {"/stream/workers": 4})],
        )

        assert runner._runtime.workers_per_process == 4
