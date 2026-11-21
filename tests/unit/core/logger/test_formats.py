"""The formats lay a safe record out: the shape a collector or a developer reads."""

from __future__ import annotations

import json
import logging

from loom.core.logger import LogField
from loom.core.logger.formats import JsonFormatter, TextFormatter


def _record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord("billing.invoices", logging.ERROR, __file__, 7, "b.x_y", (), None)
    record.__dict__.update(extra)
    return record


_STACK = 'Traceback (most recent call last):\n  File "a.py", line 1, in run\nValueError'


class TestJsonFormatter:
    def test_fields_come_first_and_the_four_lambda_keys_are_written_last(self) -> None:
        entry = json.loads(JsonFormatter().format(_record(**{LogField.COUNT: 3})))

        assert list(entry)[-4:] == ["timestamp", "level", "message", "logger"]
        assert entry[LogField.COUNT] == 3

    def test_a_field_cannot_overwrite_the_message_or_the_level(self) -> None:
        entry = json.loads(JsonFormatter().format(_record(message="forged", level="DEBUG")))

        assert (entry["message"], entry["level"]) == ("b.x_y", "ERROR")

    def test_a_private_attribute_is_not_a_field(self) -> None:
        entry = json.loads(JsonFormatter().format(_record(_private="x")))

        assert "_private" not in entry


class TestTextFormatter:
    def test_a_record_is_one_line_with_its_fields(self) -> None:
        line = TextFormatter().format(_record(**{LogField.COUNT: 3}))

        assert line.endswith("ERROR   billing.invoices b.x_y loom.count=3")

    def test_line_breaks_in_a_value_are_escaped(self) -> None:
        line = TextFormatter().format(_record(**{LogField.REASON: "a\nFAKE b"}))

        assert "\n" not in line
        assert "a\\nFAKE b" in line

    def test_the_stacktrace_follows_the_line_with_every_row_indented(self) -> None:
        first, *rows = (
            TextFormatter().format(_record(**{LogField.EXCEPTION_STACKTRACE: _STACK})).splitlines()
        )

        assert LogField.EXCEPTION_STACKTRACE.value not in first
        assert rows[0] == "  | Traceback (most recent call last):"
        assert all(row.startswith("  | ") for row in rows)
