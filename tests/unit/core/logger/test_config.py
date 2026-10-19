from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import pytest
import structlog

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.environment import Environment
from loom.core.logger.config import LoggerConfig, configure_logging_from_values


def test_configure_logging_from_values_forwards_extra_processors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_configure(config: Any, *, environment: Environment) -> None:
        captured["config"] = config

    sentinel = object()
    monkeypatch.setattr("loom.core.logger.config.configure_logging", _fake_configure)

    configure_logging_from_values(extra_processors=(sentinel,))

    config = captured["config"]
    assert config.extra_processors == (sentinel,)


def _root_renderer(name: str, environment: Environment | None = None, **values: Any) -> Any:
    kwargs: dict[str, Any] = {"name": name, **values}
    if environment is not None:
        kwargs["environment"] = environment
    configure_logging_from_values(**kwargs)
    (handler,) = logging.getLogger(name).handlers
    formatter = handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    return formatter.processors[-1]


@pytest.fixture
def logger_name(request: pytest.FixtureRequest) -> Iterator[str]:
    name = f"loom.tests.renderer.{request.node.name}"
    yield name
    target = logging.getLogger(name)
    for handler in list(target.handlers):
        target.removeHandler(handler)


def test_without_an_environment_the_logger_renders_json(logger_name: str) -> None:
    renderer = _root_renderer(logger_name)

    assert isinstance(renderer, structlog.processors.JSONRenderer)


@pytest.mark.parametrize("environment", [Environment.PRODUCTION, Environment.STAGING])
def test_a_deployed_environment_renders_json(logger_name: str, environment: Environment) -> None:
    renderer = _root_renderer(logger_name, environment)

    assert isinstance(renderer, structlog.processors.JSONRenderer)


@pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.TEST])
def test_a_local_environment_renders_coloured_console_lines(
    logger_name: str, environment: Environment
) -> None:
    renderer = _root_renderer(logger_name, environment)

    assert isinstance(renderer, structlog.dev.ConsoleRenderer)
    assert renderer.colors is True


def test_an_explicit_renderer_wins_over_the_environment(logger_name: str) -> None:
    renderer = _root_renderer(logger_name, Environment.PRODUCTION, renderer="console")

    assert isinstance(renderer, structlog.dev.ConsoleRenderer)


@pytest.mark.parametrize(
    "logger_config",
    [
        {"environment": "dev"},
        {"handlers": [{"type": "stream", "colours": True}]},
    ],
)
def test_a_key_the_logger_does_not_know_is_a_config_error(logger_config: dict[str, Any]) -> None:
    config = ConfigContext.from_dict({"logger": logger_config})

    with pytest.raises(ConfigError, match="environment|colours"):
        config.section("logger", LoggerConfig)
