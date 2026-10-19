"""``StreamingRunner`` resolves the environment through the central resolver.

The runner takes the logger defaults from the environment and runs the
deploy checks in production and staging, whichever way it gets its
configuration.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from bytewax.dataflow import Dataflow  # noqa: E402

from loom.core.checks import DeployCheckError  # noqa: E402
from loom.core.config import ConfigContext  # noqa: E402
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment  # noqa: E402
from loom.streaming.bytewax.runner import StreamingRunner  # noqa: E402
from loom.streaming.graph._flow import StreamFlow  # noqa: E402
from tests.helpers.deploy_checks import (  # noqa: E402
    FailingCheckModule,
    write_failing_check_module,
)
from tests.unit.streaming.bytewax.cases import Order, Result  # noqa: E402

_KAFKA: dict[str, Any] = {
    "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["orders.in"]},
    "producer": {"brokers": ["localhost:9092"], "client_id": "p", "topic": "orders.out"},
}


@pytest.fixture
def check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FailingCheckModule:
    module = write_failing_check_module(tmp_path)
    monkeypatch.syspath_prepend(str(module.directory))
    return module


def _config(check: FailingCheckModule) -> dict[str, Any]:
    area, key = check.trigger.split(".")
    return {"kafka": _KAFKA, "checks": check.checks_section(), area: {key: True}}


@pytest.mark.parametrize("environment", ["production", "staging"])
def test_a_deployed_runner_runs_the_deploy_checks(
    bytewax_stream_flow: StreamFlow[Order, Result],
    check: FailingCheckModule,
    environment: str,
) -> None:
    with pytest.raises(DeployCheckError, match=check.check_id):
        StreamingRunner.from_dict(bytewax_stream_flow, _config(check), environment=environment)


def test_the_selector_variable_reaches_the_runner(
    bytewax_stream_flow: StreamFlow[Order, Result],
    check: FailingCheckModule,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENVIRONMENT_VARIABLE, "production")

    with pytest.raises(DeployCheckError, match=check.check_id):
        StreamingRunner.from_dict(bytewax_stream_flow, _config(check))


def test_a_local_runner_runs_no_check(
    bytewax_stream_flow: StreamFlow[Order, Result], check: FailingCheckModule
) -> None:
    runner = StreamingRunner.from_dict(
        bytewax_stream_flow, _config(check), environment="development"
    )

    assert isinstance(runner, StreamingRunner)


def test_run_with_a_config_runs_the_deploy_checks(
    bytewax_stream_flow: StreamFlow[Order, Result],
    check: FailingCheckModule,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "loom.streaming.bytewax.runner._prepare_run",
        lambda plan, **kwargs: SimpleNamespace(dataflow=Dataflow("t"), shutdown=lambda: None),
    )
    monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", lambda *_a, **_kw: None)
    config = ConfigContext.from_dict(_config(check))

    with pytest.raises(DeployCheckError, match=check.check_id):
        StreamingRunner().run(flow=bytewax_stream_flow, config=config, environment="production")


def test_the_logger_takes_the_environment_of_the_runner(
    bytewax_stream_flow: StreamFlow[Order, Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "loom.core.observability.runtime.configure_logging_from_values",
        lambda **kwargs: captured.update(kwargs),
    )
    config = {"kafka": _KAFKA, "observability": {"log": {"config": {"level": "INFO"}}}}

    StreamingRunner.from_dict(bytewax_stream_flow, config, environment="test")

    assert captured["environment"] is Environment.TEST
