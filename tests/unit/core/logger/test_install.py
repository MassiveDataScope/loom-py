from __future__ import annotations

import logging

import pytest

from loom.core.environment import (
    LAMBDA_FUNCTION_VARIABLE,
    LAMBDA_LOG_FORMAT_VARIABLE,
    Environment,
)
from loom.core.logger.formats import JsonFormatter, LambdaFormatter, TextFormatter
from loom.core.logger.install import HANDLER_NAME
from loom.core.logger.settings import LogFormatName, LogLevel, LogSettings
from loom.testing import make
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
        configured(
            make(LogSettings, format=log_format, level=LogLevel.INFO),
            environment=Environment.PRODUCTION,
        )

        logging.getLogger("tests.stdout").warning("billing.invoice_issued")

        (handler,) = _installed(pristine_root_logger)
        assert type(handler.formatter) is formatter
        assert "billing.invoice_issued" in capsys.readouterr().out


class TestJsonInstallation:
    def test_a_second_configuration_replaces_the_handler_of_the_first(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        settings = make(LogSettings, format=LogFormatName.JSON)

        configured(settings)
        configured(settings)

        assert len(_installed(pristine_root_logger)) == 1

    def test_a_handler_somebody_else_added_stays(
        self, handler: CapturingHandler, pristine_root_logger: logging.Logger
    ) -> None:
        configured(make(LogSettings, format=LogFormatName.JSON), environment=Environment.PRODUCTION)

        assert handler in pristine_root_logger.handlers

    def test_the_level_and_the_named_levels_are_applied(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        settings = make(
            LogSettings,
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
        export, settings = CapturingHandler(), make(LogSettings, format=LogFormatName.JSON)

        configured(settings, extra_handlers=(export,))
        configured(settings, extra_handlers=(export,))

        assert pristine_root_logger.handlers.count(export) == 1


class TestLambdaInstallation:
    def test_a_handler_that_is_not_the_runtime_keeps_its_formatter(
        self, runtime_handler: LambdaLoggerHandler, handler: CapturingHandler
    ) -> None:
        configured(make(LogSettings, format=None), environment=Environment.PRODUCTION)

        assert handler.formatter is None

    def test_without_a_runtime_handler_loom_writes_json_to_stdout(
        self, pristine_root_logger: logging.Logger, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(LAMBDA_FUNCTION_VARIABLE, "billing-handler")

        configured(make(LogSettings, format=None), environment=Environment.PRODUCTION)

        (installed,) = [h for h in pristine_root_logger.handlers if h.get_name() == HANDLER_NAME]
        assert isinstance(installed.formatter, JsonFormatter)

    def test_loom_installs_no_handler_of_its_own(
        self, runtime_handler: LambdaLoggerHandler, pristine_root_logger: logging.Logger
    ) -> None:
        before = list(pristine_root_logger.handlers)

        configured(make(LogSettings, format=None), environment=Environment.PRODUCTION)

        assert pristine_root_logger.handlers == before

    def test_the_settings_set_the_root_level_in_lambda_too(
        self, runtime_handler: LambdaLoggerHandler, pristine_root_logger: logging.Logger
    ) -> None:
        pristine_root_logger.setLevel(logging.ERROR)

        configured(make(LogSettings, level=LogLevel.DEBUG), environment=Environment.PRODUCTION)

        assert pristine_root_logger.level == logging.DEBUG

    @pytest.mark.parametrize("log_format", ["Text", "JSON"])
    def test_the_runtime_handler_gets_loom_json_for_the_function_log_format(
        self,
        runtime_handler: LambdaLoggerHandler,
        monkeypatch: pytest.MonkeyPatch,
        log_format: str,
    ) -> None:
        monkeypatch.setenv(LAMBDA_LOG_FORMAT_VARIABLE, log_format)

        configured(make(LogSettings, format=None), environment=Environment.PRODUCTION)

        assert isinstance(runtime_handler.formatter, LambdaFormatter)

    def test_switching_from_json_leaves_no_loom_handler_behind(
        self, runtime_handler: LambdaLoggerHandler, pristine_root_logger: logging.Logger
    ) -> None:
        configured(make(LogSettings, format=LogFormatName.JSON), environment=Environment.PRODUCTION)

        configured(make(LogSettings, format=None), environment=Environment.PRODUCTION)

        assert _installed(pristine_root_logger) == []
