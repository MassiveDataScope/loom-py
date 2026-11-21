"""Value rendering: what a field value becomes, and the filter that applies it on handlers."""

from __future__ import annotations

import logging
import math

import pytest

from loom.core.environment import Environment
from loom.core.logger import LogField, LogIdentity, current_log_fields, log_context
from loom.core.logger.filter import LogFilter
from loom.core.logger.install import configure_logging, install_record_factory
from loom.core.logger.settings import LogSettings
from loom.core.logger.values import (
    MAX_LIST_ITEMS,
    MAX_VALUE_LENGTH,
    TRUNCATION_MARKER,
    render_value,
)
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret


class _Card:
    def __repr__(self) -> str:
        return "Card(4111-1111-1111-1111)"


class TestRenderValue:
    @pytest.mark.parametrize("value", [None, True, 3, 2.5, "ok", ["a", 1, None], ("a", 2)])
    def test_a_scalar_or_a_short_list_of_scalars_is_written_as_it_is(self, value: object) -> None:
        rendered = render_value(value)

        assert rendered == (list(value) if isinstance(value, tuple) else value)

    @pytest.mark.parametrize(
        ("value", "name"),
        [(_Card(), "_Card"), ({"pan": "4111"}, "dict"), (["a"] * (MAX_LIST_ITEMS + 1), "list")],
    )
    def test_anything_else_is_its_type_name_never_its_repr(self, value: object, name: str) -> None:
        assert render_value(value) == name

    @pytest.mark.parametrize("secret", [Secret("hunter2"), RedactedMap({"token": "abc"})])
    def test_a_secret_is_the_mask(self, secret: object) -> None:
        assert render_value(secret) == MASK

    def test_a_dsn_is_its_masked_text(self) -> None:
        assert "hunter2" not in str(render_value(Dsn("postgresql://u:hunter2@db/app")))

    def test_a_long_string_is_cut_with_a_marker(self) -> None:
        rendered = render_value("x" * (MAX_VALUE_LENGTH + 10))

        assert isinstance(rendered, str)
        assert len(rendered) == MAX_VALUE_LENGTH
        assert rendered.endswith(TRUNCATION_MARKER)

    def test_a_non_finite_float_is_text_so_the_json_stays_valid(self) -> None:
        assert render_value(math.inf) == "inf"


class TestLogFilter:
    @pytest.fixture
    def record(self) -> logging.LogRecord:
        install_record_factory(LogIdentity(environment="test", service_name="billing"))
        record = logging.getLogger("tests.filter").makeRecord(
            "tests.filter",
            logging.INFO,
            __file__,
            1,
            "billing.invoice_issued",
            (),
            None,
            extra={LogField.COUNT: _Card(), LogField.REASON: None},
        )
        return record

    def test_values_are_rendered_and_a_none_value_is_not_written(
        self, record: logging.LogRecord
    ) -> None:
        LogFilter().filter(record)

        assert record.__dict__[LogField.COUNT] == "_Card"
        assert LogField.REASON not in record.__dict__
        assert record.__dict__[LogField.SERVICE_NAME] == "billing"

    def test_a_second_filter_leaves_the_record_as_the_first_wrote_it(
        self, record: logging.LogRecord
    ) -> None:
        LogFilter().filter(record)
        written = dict(record.__dict__)

        assert LogFilter().filter(record)
        assert record.__dict__ == written

    def test_a_record_no_loom_factory_created_passes_as_it_is(self) -> None:
        record = logging.LogRecord("acme", logging.INFO, "", 0, "acme.thing_done", (), None)
        record.amount = _Card()

        assert LogFilter().filter(record)
        assert isinstance(record.__dict__["amount"], _Card)

    def test_logging_never_raises_once_configured(self) -> None:
        configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)

        assert logging.raiseExceptions is False


class TestLogContextValues:
    def test_a_context_value_is_rendered_once_when_the_block_opens(self) -> None:
        with log_context({LogField.NAME: _Card(), LogField.REASON: None}):
            fields = current_log_fields()

        assert fields == {LogField.NAME.value: "_Card"}
