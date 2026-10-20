from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import pytest
import structlog

from loom.core.checks import DeployCheckRegistry, Finding, enforce_deploy_checks
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
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


@pytest.mark.parametrize(
    ("selected", "renderer_type"),
    [
        (None, structlog.processors.JSONRenderer),
        ("development", structlog.dev.ConsoleRenderer),
    ],
)
def test_without_an_environment_the_logger_resolves_it(
    logger_name: str,
    monkeypatch: pytest.MonkeyPatch,
    selected: str | None,
    renderer_type: type,
) -> None:
    if selected is None:
        monkeypatch.delenv(ENVIRONMENT_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, selected)

    renderer = _root_renderer(logger_name)

    assert isinstance(renderer, renderer_type)


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


_FLOOR_LOGGERS = ("loom", "loom.core", "loom.core.checks")


@pytest.fixture
def loom_logger_restored() -> Iterator[None]:
    # configure_logging sets process-wide structlog filtering; restore it for later tests.
    structlog_config = structlog.get_config()
    yield
    structlog.configure(**structlog_config)
    for name in _FLOOR_LOGGERS:
        target = logging.getLogger(name)
        target.setLevel(logging.NOTSET)
        for handler in list(target.handlers):
            target.removeHandler(handler)


def _warning_registry() -> DeployCheckRegistry:
    registry = DeployCheckRegistry()

    @registry.register("tests.floor.W001")
    def _always(config: ConfigContext) -> Finding | None:
        return Finding(message="tests.floor is set", hint="unset it")

    return registry


@pytest.mark.usefixtures("loom_logger_restored")
@pytest.mark.parametrize(
    ("level", "named_levels"),
    [("CRITICAL", {}), ("INFO", {"loom": "CRITICAL"}), ("INFO", {"loom.core": "CRITICAL"})],
)
def test_a_deployed_log_level_cannot_hide_the_deploy_check_warnings(
    capsys: pytest.CaptureFixture[str], level: str, named_levels: dict[str, str]
) -> None:
    configure_logging_from_values(
        name="loom", environment=Environment.PRODUCTION, level=level, named_levels=named_levels
    )
    config = ConfigContext.from_dict({"checks": {"silenced": ["tests.floor.W002"]}})

    enforce_deploy_checks(config, Environment.PRODUCTION, registry=_warning_registry())

    written = capsys.readouterr().out
    assert "tests.floor.W001" in written
    assert "silenced" in written


@pytest.mark.usefixtures("loom_logger_restored")
def test_a_local_log_level_still_hides_loom_warnings(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging_from_values(
        name="loom", environment=Environment.DEVELOPMENT, level="CRITICAL"
    )

    logging.getLogger("loom.core.checks").warning("hidden")

    assert "hidden" not in capsys.readouterr().out
