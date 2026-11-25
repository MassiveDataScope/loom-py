from __future__ import annotations

import json
import logging
from collections.abc import Iterator

import pytest

from loom.core.environment import (
    LAMBDA_FUNCTION_VARIABLE,
    LAMBDA_LOG_FORMAT_VARIABLE,
    Environment,
)
from loom.core.log_names import LogField
from loom.core.logger.formats import JsonFormatter, LambdaFormatter, TextFormatter
from loom.core.logger.install import HANDLER_NAME
from loom.core.logger.settings import LogFormatName, LogLevel, LogSettings
from tests.helpers.loggers import CapturingHandler, LambdaLoggerHandler, configured


def _installed(root: logging.Logger) -> list[logging.Handler]:
    return [h for h in root.handlers if h.get_name() == HANDLER_NAME]


class TestStdoutFormats:
    @pytest.mark.parametrize(
        ("log_format", "formatter"),
        [(LogFormatName.JSON, JsonFormatter), (LogFormatName.TEXT, TextFormatter)],
        ids=["json", "text"],
    )
    def test_loom_handler_writes_to_stdout_with_the_format_formatter(
        self,
        pristine_root_logger: logging.Logger,
        capsys: pytest.CaptureFixture[str],
        log_format: LogFormatName,
        formatter: type[logging.Formatter],
    ) -> None:
        configured(LogSettings(format=log_format), environment=Environment.PRODUCTION)

        logging.getLogger("tests.stdout").warning("billing.invoice_issued")

        (handler,) = _installed(pristine_root_logger)
        assert type(handler.formatter) is formatter
        assert "billing.invoice_issued" in capsys.readouterr().out


class TestJsonInstallation:
    def test_json_lines_reach_stdout_stamped_with_the_service(
        self, pristine_root_logger: logging.Logger, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configured(
            LogSettings(format=LogFormatName.JSON),
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
            configured(LogSettings(format=LogFormatName.JSON), environment=Environment.PRODUCTION)

        assert len(_installed(pristine_root_logger)) == 1

    def test_a_handler_somebody_else_added_stays(
        self, handler: CapturingHandler, pristine_root_logger: logging.Logger
    ) -> None:
        configured(LogSettings(format=LogFormatName.JSON), environment=Environment.PRODUCTION)

        assert handler in pristine_root_logger.handlers

    def test_the_level_and_the_named_levels_are_applied(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        settings = LogSettings(
            format=LogFormatName.TEXT,
            level=LogLevel.WARNING,
            named_levels={"acme_library": LogLevel.ERROR},
        )

        configured(settings, environment=Environment.DEVELOPMENT)

        assert pristine_root_logger.level == logging.WARNING
        assert logging.getLogger("acme_library").level == logging.ERROR
        assert logging.getLogger("botocore").level == logging.WARNING

    def test_reconfiguring_leaves_an_owned_handler_attached_once(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        export, settings = CapturingHandler(), LogSettings(format=LogFormatName.JSON)

        for _ in range(2):
            configured(settings, extra_handlers=(export,))

        assert pristine_root_logger.handlers.count(export) == 1


class TestLambdaInstallation:
    @pytest.fixture
    def runtime_handler(
        self, pristine_root_logger: logging.Logger, monkeypatch: pytest.MonkeyPatch
    ) -> Iterator[LambdaLoggerHandler]:
        monkeypatch.setenv(LAMBDA_FUNCTION_VARIABLE, "billing-handler")
        runtime = LambdaLoggerHandler()
        pristine_root_logger.addHandler(runtime)
        yield runtime
        pristine_root_logger.removeHandler(runtime)

    def test_a_handler_that_is_not_the_runtime_keeps_its_formatter(
        self, runtime_handler: LambdaLoggerHandler, handler: CapturingHandler
    ) -> None:
        configured(LogSettings(), environment=Environment.PRODUCTION)

        assert handler.formatter is None

    def test_without_a_runtime_handler_loom_writes_json_to_stdout(
        self, pristine_root_logger: logging.Logger, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(LAMBDA_FUNCTION_VARIABLE, "billing-handler")

        configured(LogSettings(), environment=Environment.PRODUCTION)

        (installed,) = [h for h in pristine_root_logger.handlers if h.get_name() == HANDLER_NAME]
        assert isinstance(installed.formatter, JsonFormatter)

    def test_no_handler_is_installed(
        self, runtime_handler: LambdaLoggerHandler, pristine_root_logger: logging.Logger
    ) -> None:
        before = list(pristine_root_logger.handlers)

        configured(LogSettings(), environment=Environment.PRODUCTION)

        assert pristine_root_logger.handlers == before

    def test_the_settings_set_the_root_level_in_lambda_too(
        self, runtime_handler: LambdaLoggerHandler, pristine_root_logger: logging.Logger
    ) -> None:
        pristine_root_logger.setLevel(logging.ERROR)

        configured(LogSettings(level=LogLevel.DEBUG), environment=Environment.PRODUCTION)

        assert pristine_root_logger.level == logging.DEBUG

    @pytest.mark.parametrize(
        ("log_format", "frame"),
        [("Text", None), ("JSON", bytes.fromhex("a55a0016"))],
        ids=["text", "json"],
    )
    def test_the_runtime_handler_gets_loom_json_for_the_function_log_format(
        self,
        runtime_handler: LambdaLoggerHandler,
        monkeypatch: pytest.MonkeyPatch,
        log_format: str,
        frame: bytes | None,
    ) -> None:
        monkeypatch.setenv(LAMBDA_LOG_FORMAT_VARIABLE, log_format)

        configured(LogSettings(), environment=Environment.PRODUCTION)

        logging.getLogger("tests.lambda").error("billing.invoice_failed")

        assert isinstance(runtime_handler.formatter, LambdaFormatter)
        assert runtime_handler.frames == [frame]

    def test_the_runtime_handler_writes_the_identity(
        self, runtime_handler: LambdaLoggerHandler
    ) -> None:
        configured(LogSettings(), environment=Environment.PRODUCTION, service_name="billing")

        logging.getLogger("tests.lambda").error("billing.invoice_failed")

        (line,) = runtime_handler.lines
        assert json.loads(line)[LogField.SERVICE_NAME] == "billing"

    def test_switching_from_json_leaves_no_loom_handler_behind(
        self, runtime_handler: LambdaLoggerHandler, pristine_root_logger: logging.Logger
    ) -> None:
        configured(LogSettings(format=LogFormatName.JSON), environment=Environment.PRODUCTION)

        configured(LogSettings(), environment=Environment.PRODUCTION)

        assert _installed(pristine_root_logger) == []
