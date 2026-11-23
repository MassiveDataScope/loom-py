"""One record, rendered by each format: the shape a collector or a developer reads."""

from __future__ import annotations

import json
import logging

import pytest

from loom.core.logger import LogField
from loom.core.logger.formats import (
    MAX_LIST_ITEMS,
    MAX_VALUE_LENGTH,
    TRUNCATION_MARKER,
    JsonFormatter,
    TextFormatter,
    render_fields,
)
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret

_STACK = LogField.EXCEPTION_STACKTRACE.value


def _record(
    level: int = logging.INFO, msg: str = "billing.invoice_issued", **extra: object
) -> logging.LogRecord:
    record = logging.LogRecord(
        name="billing.invoices",
        level=level,
        pathname=__file__,
        lineno=7,
        msg=msg,
        args=(),
        exc_info=None,
    )
    record.__dict__.update(extra)
    render_fields(record)
    return record


class TestJsonFormatter:
    def test_a_record_is_one_json_object_with_the_keys_of_lambda_first(self) -> None:
        line = JsonFormatter().format(_record())

        entry = json.loads(line)
        assert "\n" not in line
        assert list(entry)[:4] == ["timestamp", "level", "message", "logger"]
        assert (entry["level"], entry["message"], entry["logger"]) == (
            "INFO",
            "billing.invoice_issued",
            "billing.invoices",
        )

    def test_the_timestamp_is_utc_iso_8601(self) -> None:
        entry = json.loads(JsonFormatter().format(_record()))

        assert entry["timestamp"].endswith("+00:00")

    def test_extra_keys_are_top_level_and_keep_their_dots(self) -> None:
        entry = json.loads(
            JsonFormatter().format(_record(**{LogField.JOB.value: "SendInvoice", "amount": 12.5}))
        )

        assert (entry["loom.job"], entry["amount"]) == ("SendInvoice", 12.5)

    def test_the_stack_field_is_written_whole_not_cut_like_a_value(self) -> None:
        stack = "x" * (MAX_VALUE_LENGTH * 2)

        entry = json.loads(JsonFormatter().format(_record(**{_STACK: stack})))

        assert entry[_STACK] == stack


class TestTextFormatter:
    def test_a_record_is_a_line_with_its_level_logger_and_message(self) -> None:
        line = TextFormatter().format(_record())

        assert "INFO" in line
        assert line.endswith("billing.invoices billing.invoice_issued")

    def test_extra_keys_follow_the_message_as_key_value_pairs(self) -> None:
        line = TextFormatter().format(_record(amount=12.5, **{LogField.JOB.value: "SendInvoice"}))

        assert line.endswith("billing.invoice_issued amount=12.5 loom.job=SendInvoice")

    def test_the_stack_follows_the_line_with_every_row_prefixed(self) -> None:
        stack = "Traceback (most recent call last):\nValueError: boom\n2026-01-01 ERROR fake"

        rows = TextFormatter().format(_record(logging.ERROR, **{_STACK: stack})).split("\n")

        assert [row for row in rows if not row.startswith("  | ")] == [rows[0]]
        assert _STACK not in rows[0]
        assert rows[1:] == [f"  | {row}" for row in stack.split("\n")]


def _colliding_record(extra: dict[str, str]) -> logging.LogRecord:
    record = _record()
    record.__dict__.update(extra)
    return record


class TestReservedKeys:
    COLLIDING = {
        "level": "FATAL",
        "timestamp": "never",
        "logger": "forged.logger",
    }

    def test_json_keeps_the_metadata_of_the_record_and_moves_the_colliding_keys(self) -> None:
        entry = json.loads(JsonFormatter().format(_colliding_record(self.COLLIDING)))

        assert (entry["level"], entry["logger"], entry["message"]) == (
            "INFO",
            "billing.invoices",
            "billing.invoice_issued",
        )
        assert entry["timestamp"].endswith("+00:00")
        assert {key: entry[f"loom.extra.{key}"] for key in self.COLLIDING} == self.COLLIDING

    def test_text_moves_the_colliding_keys_the_same_way(self) -> None:
        line = TextFormatter().format(_colliding_record(self.COLLIDING))

        assert "loom.extra.level=FATAL" in line
        assert "loom.extra.logger=forged.logger" in line
        assert line.count(" level=") == 0

    def test_a_key_that_does_not_collide_is_written_as_given(self) -> None:
        entry = json.loads(JsonFormatter().format(_record(levels="x")))

        assert entry["levels"] == "x"


class _Invoice:
    def __repr__(self) -> str:
        return "Invoice(card=4111111111111111)"


def _both(**extra: object) -> tuple[dict[str, object], str]:
    record = _record(**extra)
    return json.loads(JsonFormatter().format(record)), TextFormatter().format(record)


class TestValueRendering:
    @pytest.mark.parametrize("value", ["text", 3, 2.5, True, None, ["a", 1, None]])
    def test_a_scalar_or_a_short_list_of_scalars_is_written_as_it_is(self, value: object) -> None:
        entry, _ = _both(field=value)

        assert entry["field"] == value

    def test_a_long_list_of_scalars_keeps_its_first_items(self) -> None:
        entry, _ = _both(field=list(range(MAX_LIST_ITEMS + 5)))

        assert entry["field"] == list(range(MAX_LIST_ITEMS))

    @pytest.mark.parametrize(
        ("value", "type_name"),
        [(_Invoice(), "_Invoice"), ({"a": 1}, "dict"), ([[1]], "list")],
        ids=["object", "mapping", "nested-list"],
    )
    def test_anything_else_is_written_as_its_type_name_never_its_repr(
        self, value: object, type_name: str
    ) -> None:
        entry, line = _both(field=value)

        assert entry["field"] == type_name
        assert "4111" not in line

    def test_a_non_finite_float_is_written_as_text_so_the_json_stays_valid(self) -> None:
        entry, _ = _both(field=float("nan"))

        assert entry["field"] == "nan"

    @pytest.mark.parametrize(
        "secret", [Secret("hunter2"), RedactedMap({"token": "hunter2"})], ids=["secret", "map"]
    )
    def test_a_secret_is_masked_in_both_formats(self, secret: object) -> None:
        entry, line = _both(credential=secret, backups=[secret])

        assert entry["credential"] == MASK
        assert entry["backups"] == [MASK]
        assert "hunter2" not in line
        assert f"credential={MASK}" in line

    def test_a_dsn_is_written_with_its_password_masked(self) -> None:
        entry, line = _both(address=Dsn("postgres://app:hunter2@db:5432/billing"))

        assert "hunter2" not in str(entry["address"])
        assert "hunter2" not in line

    def test_a_long_value_is_cut_with_a_marker(self) -> None:
        entry, line = _both(field="x" * (MAX_VALUE_LENGTH + 500))

        assert len(entry["field"]) == MAX_VALUE_LENGTH  # type: ignore[arg-type]
        assert entry["field"].endswith(TRUNCATION_MARKER)  # type: ignore[union-attr]
        assert line.endswith(TRUNCATION_MARKER)

    def test_a_long_message_is_cut_in_both_formats(self) -> None:
        record = _record(msg="y" * (MAX_VALUE_LENGTH + 10))

        entry = json.loads(JsonFormatter().format(record))

        assert entry["message"].endswith(TRUNCATION_MARKER)
        assert TextFormatter().format(record).endswith(TRUNCATION_MARKER)


class TestLogInjection:
    FORGED = "ok\r\n2026-01-01 ERROR billing.forged forged_entry"

    def test_the_text_line_escapes_line_breaks_in_the_message(self) -> None:
        line = TextFormatter().format(_record(msg=self.FORGED))

        assert "\n" not in line
        assert "ok\\r\\n2026" in line

    def test_the_text_line_escapes_line_breaks_in_a_value(self) -> None:
        line = TextFormatter().format(_record(user=self.FORGED))

        assert "\n" not in line
        assert "user=ok\\r\\n2026" in line

    def test_json_keeps_one_line_for_a_value_with_line_breaks(self) -> None:
        line = JsonFormatter().format(_record(user=self.FORGED))

        assert "\n" not in line
        assert json.loads(line)["user"] == self.FORGED
