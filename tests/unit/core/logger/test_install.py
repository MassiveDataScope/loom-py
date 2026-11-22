from __future__ import annotations

import json
import logging

import pytest

from loom.core.config.errors import ConfigError
from loom.core.environment import Environment
from loom.core.logger import ContextFilter, LogField, LogRuntime
from loom.core.logger.formats import HANDLER_NAME, LambdaFormat
from loom.core.logger.install import configure_logging, resolve_format
from loom.core.logger.settings import LogSettings
from tests.helpers.loggers import CapturingHandler

_LAMBDA = "AWS_LAMBDA_FUNCTION_NAME"


def _stamping(handler: logging.Handler) -> list[ContextFilter]:
    return [f for f in handler.filters if isinstance(f, ContextFilter)]


def _installed(root: logging.Logger) -> list[logging.Handler]:
    return [h for h in root.handlers if h.get_name() == HANDLER_NAME]


class TestJsonInstallation:
    def test_json_lines_reach_stdout_stamped_with_the_service(
        self, pristine_root_logger: logging.Logger, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure_logging(
            LogSettings(format="json"),
            environment=Environment.PRODUCTION,
            service_name="billing",
            service_version="2.4.0",
        )

        logging.getLogger("acme_library").warning("Found credentials")

        entry = json.loads(capsys.readouterr().out)
        assert entry["message"] == "Found credentials"
        assert entry[LogField.SERVICE_NAME] == "billing"
        assert entry[LogField.RUNTIME] == "cli"

    def test_a_second_configuration_replaces_the_handler_of_the_first(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        for _ in range(2):
            configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)

        assert len(_installed(pristine_root_logger)) == 1

    def test_a_handler_somebody_else_added_stays(
        self, handler: CapturingHandler, pristine_root_logger: logging.Logger
    ) -> None:
        configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)

        assert handler in pristine_root_logger.handlers

    def test_the_level_and_the_named_levels_are_applied(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        settings = LogSettings(
            format="text", level="warning", named_levels={"acme_library": "ERROR"}
        )

        configure_logging(settings, environment=Environment.DEVELOPMENT)

        assert pristine_root_logger.level == logging.WARNING
        assert logging.getLogger("acme_library").level == logging.ERROR

    def test_the_runtime_is_named_after_the_composition(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        installation = configure_logging(
            LogSettings(format="json"), environment=Environment.PRODUCTION
        )

        installation.running_as(LogRuntime.CELERY)

        (handler,) = _installed(pristine_root_logger)
        record = logging.LogRecord("x", logging.INFO, __file__, 1, "x.y_z", (), None)
        _stamping(handler)[0].filter(record)
        assert record.__dict__[LogField.RUNTIME] == "celery"

    def test_an_extra_filter_goes_on_the_handler_beside_the_context_filter(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        extra = logging.Filter()

        configure_logging(
            LogSettings(format="json"), environment=Environment.PRODUCTION, filters=(extra,)
        )

        (handler,) = _installed(pristine_root_logger)
        assert extra in handler.filters
        assert len(_stamping(handler)) == 1


class TestLambdaInstallation:
    @pytest.fixture
    def runtime_handler(
        self, handler: CapturingHandler, monkeypatch: pytest.MonkeyPatch
    ) -> CapturingHandler:
        monkeypatch.setenv(_LAMBDA, "billing-handler")
        return handler

    def test_no_handler_is_installed(
        self, runtime_handler: CapturingHandler, pristine_root_logger: logging.Logger
    ) -> None:
        before = list(pristine_root_logger.handlers)

        configure_logging(LogSettings(), environment=Environment.PRODUCTION)

        assert pristine_root_logger.handlers == before

    def test_the_level_of_the_runtime_is_left_alone(
        self, runtime_handler: CapturingHandler, pristine_root_logger: logging.Logger
    ) -> None:
        pristine_root_logger.setLevel(logging.ERROR)

        configure_logging(LogSettings(level="DEBUG"), environment=Environment.PRODUCTION)

        assert pristine_root_logger.level == logging.ERROR

    def test_the_context_filter_goes_on_the_handler_of_the_runtime(
        self, runtime_handler: CapturingHandler
    ) -> None:
        configure_logging(LogSettings(), environment=Environment.PRODUCTION, service_name="billing")

        logging.getLogger("tests.lambda").error("billing.invoice_failed")

        (record,) = runtime_handler.records
        assert record.__dict__[LogField.SERVICE_NAME] == "billing"

    def test_configuring_twice_leaves_one_context_filter(
        self, runtime_handler: CapturingHandler
    ) -> None:
        for _ in range(2):
            configure_logging(LogSettings(), environment=Environment.PRODUCTION)

        assert len(_stamping(runtime_handler)) == 1


class TestResolveFormat:
    def test_an_unregistered_format_lists_the_registered_ones(self) -> None:
        with pytest.raises(ConfigError, match=r"unknown log format 'xml'.*json.*lambda.*text"):
            resolve_format("xml")

    def test_a_registered_format_is_built_by_calling_what_is_registered(self) -> None:
        assert isinstance(resolve_format("lambda")(), LambdaFormat)
