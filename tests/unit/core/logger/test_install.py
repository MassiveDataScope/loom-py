from __future__ import annotations

import io
import json
import logging
import queue
from collections.abc import Iterator
from logging.handlers import QueueHandler, QueueListener

import pytest

from loom.core.config.errors import ConfigError
from loom.core.environment import Environment
from loom.core.errors.log_events import ErrorLogField
from loom.core.logger import LogField, LogRuntime
from loom.core.logger.filter import LogFilter
from loom.core.logger.formats import JsonFormatter, ProcessHandlers
from loom.core.logger.install import (
    HANDLER_NAME,
    UnfilteredHandlersWarning,
    configure_logging,
    resolve_format,
)
from loom.core.logger.log_events import LoggerLogEvent
from loom.core.logger.settings import THIRD_PARTY_FLOORS, LogSettings
from tests.helpers.loggers import CapturingHandler, LambdaLoggerHandler

_LAMBDA = "AWS_LAMBDA_FUNCTION_NAME"
_SECRET = "card 4111-1111-1111-1111"


def _installed(root: logging.Logger) -> list[logging.Handler]:
    return [h for h in root.handlers if h.get_name() == HANDLER_NAME]


def _filters(handler: logging.Handler) -> list[LogFilter]:
    return [f for f in handler.filters if isinstance(f, LogFilter)]


def _fail(logger: logging.Logger) -> None:
    try:
        raise ValueError(_SECRET)
    except ValueError:
        logger.exception("billing.charge_failed")


def _stream_handler(formatter: logging.Formatter) -> tuple[logging.Handler, io.StringIO]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter)
    return handler, stream


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

        logging.getLogger("acme_library").warning("billing.card_expiring")

        entry = json.loads(capsys.readouterr().out)
        assert entry["message"] == "billing.card_expiring"
        assert entry[LogField.SERVICE_NAME] == "billing"
        assert entry[LogField.RUNTIME] == "cli"

    def test_a_second_configuration_replaces_the_handler_of_the_first(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        for _ in range(2):
            configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)

        (installed,) = _installed(pristine_root_logger)
        assert len(_filters(installed)) == 1

    def test_the_level_the_floors_and_the_named_levels_are_applied(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        settings = LogSettings(
            format="text", level="warning", named_levels={"acme_library": "ERROR"}
        )

        configure_logging(settings, environment=Environment.DEVELOPMENT)

        assert pristine_root_logger.level == logging.WARNING
        assert logging.getLogger("acme_library").level == logging.ERROR
        assert {
            name: logging.getLevelName(logging.getLogger(name).level) for name in THIRD_PARTY_FLOORS
        } == dict(THIRD_PARTY_FLOORS)

    def test_the_runtime_is_named_after_the_composition(
        self, handler: CapturingHandler, pristine_root_logger: logging.Logger
    ) -> None:
        installation = configure_logging(
            LogSettings(format="json"), environment=Environment.PRODUCTION
        )
        handler.addFilter(LogFilter())

        installation.running_as(LogRuntime.CELERY)
        logging.getLogger("tests.runtime").info("billing.invoice_issued")

        (record,) = handler.records
        assert record.__dict__[LogField.RUNTIME] == "celery"


class TestEveryHandlerSeesTheSafeRecord:
    def test_a_handler_loom_does_not_own_is_left_alone_and_gets_no_exception(
        self, handler: CapturingHandler
    ) -> None:
        configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)

        _fail(logging.getLogger("acme_library"))

        assert _filters(handler) == []
        (record,) = handler.records
        assert (record.exc_info, record.stack_info) == (None, None)

    def test_a_handler_on_a_child_logger_gets_no_exception_message(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)
        child = logging.getLogger("acme_library.payments")
        child_handler, stream = _stream_handler(logging.Formatter("%(message)s"))
        child.addHandler(child_handler)
        try:
            _fail(child)
        finally:
            child.removeHandler(child_handler)

        assert stream.getvalue() == "billing.charge_failed\n"

    def test_a_handler_added_after_the_configuration_gets_no_exception_and_is_reported(
        self, pristine_root_logger: logging.Logger, caplog: pytest.LogCaptureFixture
    ) -> None:
        configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)
        late, stream = _stream_handler(logging.Formatter("%(message)s"))
        pristine_root_logger.addHandler(late)

        child = logging.getLogger("acme_library.payments")
        child_handler, _ = _stream_handler(logging.Formatter("%(message)s"))
        child.addHandler(child_handler)
        warn_once = UnfilteredHandlersWarning()
        try:
            _fail(logging.getLogger("acme_library"))
            warn_once()
            warn_once()
            unfiltered = ProcessHandlers().unfiltered()
        finally:
            child.removeHandler(child_handler)

        assert _SECRET not in stream.getvalue()
        assert {late, child_handler} <= set(unfiltered)
        assert caplog.messages.count(LoggerLogEvent.HANDLERS_UNFILTERED) == 1

    def test_a_queue_handler_carries_the_fields_and_no_message_to_its_listener(
        self, pristine_root_logger: logging.Logger
    ) -> None:
        records: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
        pristine_root_logger.addHandler(QueueHandler(records))
        configure_logging(
            LogSettings(format="json"), environment=Environment.PRODUCTION, service_name="billing"
        )
        sink, stream = _stream_handler(JsonFormatter())
        sink.addFilter(LogFilter())
        listener = QueueListener(records, sink)
        listener.start()
        try:
            _fail(logging.getLogger("acme_library"))
        finally:
            listener.stop()

        entry = json.loads(stream.getvalue())
        assert _SECRET not in stream.getvalue()
        assert (entry[LogField.SERVICE_NAME], entry[ErrorLogField.EXCEPTION_TYPE]) == (
            "billing",
            "ValueError",
        )


class TestLambdaInstallation:
    @pytest.fixture
    def runtime_handler(
        self, pristine_root_logger: logging.Logger, monkeypatch: pytest.MonkeyPatch
    ) -> Iterator[LambdaLoggerHandler]:
        monkeypatch.setenv(_LAMBDA, "billing-handler")
        runtime = LambdaLoggerHandler()
        pristine_root_logger.addHandler(runtime)
        yield runtime
        pristine_root_logger.removeHandler(runtime)

    def test_no_handler_is_installed_and_the_level_is_left_alone(
        self, runtime_handler: LambdaLoggerHandler, pristine_root_logger: logging.Logger
    ) -> None:
        pristine_root_logger.setLevel(logging.ERROR)
        before = list(pristine_root_logger.handlers)

        configure_logging(LogSettings(level="DEBUG"), environment=Environment.PRODUCTION)

        assert pristine_root_logger.handlers == before
        assert pristine_root_logger.level == logging.ERROR

    def test_the_text_format_of_the_runtime_gets_the_json_layout_so_no_field_is_lost(
        self, runtime_handler: LambdaLoggerHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AWS_LAMBDA_LOG_FORMAT", "Text")
        runtime_handler.setFormatter(logging.Formatter("%(message)s"))

        configure_logging(LogSettings(), environment=Environment.PRODUCTION, service_name="billing")
        logging.getLogger("tests.lambda").error("billing.invoice_failed")

        (record,) = runtime_handler.records
        entry = json.loads(runtime_handler.format(record))
        assert entry[LogField.SERVICE_NAME] == "billing"

    def test_the_json_format_of_the_runtime_keeps_its_own_formatter(
        self, runtime_handler: LambdaLoggerHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AWS_LAMBDA_LOG_FORMAT", "JSON")
        own = logging.Formatter("%(message)s")
        runtime_handler.setFormatter(own)

        configure_logging(LogSettings(), environment=Environment.PRODUCTION)

        assert runtime_handler.formatter is own
        assert len(_filters(runtime_handler)) == 1

    def test_only_the_runtime_handler_gets_the_json_layout(
        self,
        runtime_handler: LambdaLoggerHandler,
        handler: CapturingHandler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("AWS_LAMBDA_LOG_FORMAT", "Text")
        own = logging.Formatter("%(message)s")
        handler.setFormatter(own)

        configure_logging(LogSettings(), environment=Environment.PRODUCTION)

        assert isinstance(runtime_handler.formatter, JsonFormatter)
        assert handler.formatter is own


class TestResolveFormat:
    def test_an_unregistered_format_lists_the_registered_ones(self) -> None:
        with pytest.raises(ConfigError, match=r"unknown log format 'xml'.*json.*lambda.*text"):
            resolve_format("xml")

    def test_the_lambda_format_keeps_the_runtime_handler(self) -> None:
        assert resolve_format("lambda")(LogSettings()) is None
