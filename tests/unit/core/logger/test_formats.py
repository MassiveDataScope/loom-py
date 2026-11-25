"""One record, rendered by each format: the shape a collector or a developer reads."""

from __future__ import annotations

import json
import logging
import re
import unicodedata
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum, StrEnum
from http import HTTPStatus
from pathlib import PurePosixPath

import pytest
from hypothesis import given
from hypothesis import strategies as st

from loom.core.log_names import LogField, SubKey
from loom.core.logger.formats import (
    MAX_LIST_ITEMS,
    MAX_VALUE_LENGTH,
    TRUNCATION_MARKER,
    JsonFormatter,
    Logfmt,
    RecordFields,
    TextFormatter,
)
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret
from tests.helpers.loggers import a_record, raised

_STACK = LogField.EXCEPTION_STACKTRACE.value

_ORIGIN = re.compile(
    r' code\.function\.name=\S+ code\.line\.number=\d+ code\.file\.path=("[^"]*"|\S+)'
)


def _text(record: logging.LogRecord) -> str:
    """The text line of *record* without the code origin every record carries."""
    return _ORIGIN.sub("", TextFormatter().format(record))


class TestJsonFormatter:
    def test_a_record_starts_with_the_keys_of_lambda(self) -> None:
        entry = json.loads(JsonFormatter().format(a_record()))

        assert list(entry)[:4] == ["timestamp", "level", "message", "logger"]

    def test_extra_keys_are_top_level_and_keep_their_dots(self) -> None:
        entry = json.loads(
            JsonFormatter().format(a_record(**{LogField.JOB.value: "SendInvoice", "amount": 12.5}))
        )

        assert (entry["loom.job.name"], entry["amount"]) == ("SendInvoice", 12.5)

    def test_the_stack_field_is_written_whole_not_cut_like_a_value(self) -> None:
        record = a_record(
            severity=logging.ERROR, failure=raised(ValueError("x" * (MAX_VALUE_LENGTH * 2)))
        )

        entry = json.loads(JsonFormatter().format(record))

        assert len(entry[_STACK]) > MAX_VALUE_LENGTH
        assert entry[_STACK].endswith("x" * MAX_VALUE_LENGTH)


class TestTimestamp:
    @pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()], ids=["json", "text"])
    def test_the_timestamp_is_utc_iso_8601(self, formatter: logging.Formatter) -> None:
        record = a_record()
        record.created = 0.0

        line = formatter.format(record)

        assert "1970-01-01T00:00:00.000+00:00" in line


class TestTextFormatter:
    def test_a_record_is_a_line_with_its_level_logger_and_message(self) -> None:
        line = _text(a_record())

        assert "INFO" in line
        assert line.endswith("billing.invoices billing.invoice_issued")

    def test_extra_keys_follow_the_message_as_key_value_pairs(self) -> None:
        line = _text(a_record(amount=12.5, **{LogField.JOB.value: "SendInvoice"}))

        assert line.endswith("billing.invoice_issued amount=12.5 loom.job.name=SendInvoice")

    def test_the_stack_follows_the_line_with_every_row_prefixed(self) -> None:
        rows = (
            TextFormatter()
            .format(a_record(severity=logging.ERROR, failure=raised(ValueError("boom"))))
            .split("\n")
        )

        assert [row for row in rows if not row.startswith("  | ")] == [rows[0]]
        assert _STACK not in rows[0]
        assert rows[1].startswith("  | Traceback (most recent call last):")
        assert rows[-1] == "  | ValueError: boom"


class TestReservedKeys:
    COLLIDING = {
        "level": "FATAL",
        "timestamp": "never",
        "logger": "forged.logger",
    }

    def test_json_keeps_the_metadata_of_the_record_and_moves_the_colliding_keys(self) -> None:
        entry = json.loads(JsonFormatter().format(a_record(**self.COLLIDING)))

        assert (entry["level"], entry["logger"], entry["message"]) == (
            "INFO",
            "billing.invoices",
            "billing.invoice_issued",
        )
        assert entry["timestamp"].endswith("+00:00")

    def test_json_moves_the_colliding_keys_and_the_fields_keep_them(self) -> None:
        record = a_record(**self.COLLIDING)

        entry = json.loads(JsonFormatter().format(record))

        assert {key: entry["loom.extra." + key] for key in self.COLLIDING} == self.COLLIDING
        assert RecordFields(record).fields().items() >= self.COLLIDING.items()

    def test_text_writes_the_colliding_keys_as_given_after_its_own(self) -> None:
        line = TextFormatter().format(a_record(**self.COLLIDING))

        assert " level=FATAL" in line
        assert " logger=forged.logger" in line

    def test_a_key_that_does_not_collide_is_written_as_given(self) -> None:
        entry = json.loads(JsonFormatter().format(a_record(levels="x")))

        assert entry["levels"] == "x"


class _Invoice:
    def __repr__(self) -> str:
        return "Invoice(card=4111111111111111)"


def _both(**extra: object) -> tuple[dict[str, object], str]:
    record = a_record(**extra)
    return json.loads(JsonFormatter().format(record)), TextFormatter().format(record)


class _Color(Enum):
    RED = "red"


class _Odd(Enum):
    LIST = [1, 2]
    SECRET = Secret("hunter2")


class _Level(StrEnum):
    HIGH = "high"


class TestValueRendering:
    @pytest.mark.parametrize("value", ["text", 3, 2.5, True, None, ["a", 1, None], HTTPStatus.OK])
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

    @pytest.mark.parametrize(
        ("value", "written"),
        [
            (uuid.UUID(int=7), "00000000-0000-0000-0000-000000000007"),
            (Decimal("1.50"), "1.50"),
            (PurePosixPath("/srv/a"), "/srv/a"),
            (date(2026, 1, 2), "2026-01-02"),
            (datetime(2026, 1, 2, 3, 4, tzinfo=UTC), "2026-01-02T03:04:00+00:00"),
            (time(3, 4), "03:04:00"),
            (_Color.RED, "red"),
            (_Odd.LIST, "_Odd"),
            (_Odd.SECRET, MASK),
            ([_Color.RED, uuid.UUID(int=7)], ["red", "00000000-0000-0000-0000-000000000007"]),
        ],
        ids=[
            "uuid",
            "decimal",
            "path",
            "date",
            "datetime",
            "time",
            "enum",
            "enum-list",
            "enum-secret",
            "list",
        ],
    )
    def test_a_common_safe_type_is_written_as_its_text(
        self, value: object, written: object
    ) -> None:
        entry, _ = _both(field=value)

        assert entry["field"] == written

    def test_a_str_enum_in_a_list_is_its_text_in_the_line_never_its_repr(self) -> None:
        _, line = _both(field=[_Level.HIGH, HTTPStatus.OK])

        assert line.endswith("field=\"['high', 200]\"")

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

        assert entry["address"] == "postgres://app:***@db:5432/billing"
        assert "hunter2" not in line

    @pytest.mark.parametrize("text", [str, SubKey], ids=["str", "str-subclass"])
    def test_a_long_value_is_cut_with_a_marker(self, text: type[str]) -> None:
        entry, line = _both(field=text("x" * (MAX_VALUE_LENGTH + 500)))

        assert len(entry["field"]) == MAX_VALUE_LENGTH  # type: ignore[arg-type]
        assert entry["field"].endswith("..." + TRUNCATION_MARKER)  # type: ignore[union-attr]
        assert line.endswith("..." + TRUNCATION_MARKER)

    def test_a_long_message_is_cut_in_both_formats(self) -> None:
        record = a_record(msg="y" * (MAX_VALUE_LENGTH + 10))

        entry = json.loads(JsonFormatter().format(record))

        assert entry["message"].endswith("..." + TRUNCATION_MARKER)
        assert _text(record).endswith("..." + TRUNCATION_MARKER)


class TestLogInjection:
    FORGED = "ok\r\n2026-01-01 ERROR billing.forged forged_entry"

    @pytest.mark.parametrize(
        ("fields", "written"),
        [
            ({"user": FORGED}, 'm user="ok\\r\\n2026-01-01 ERROR billing.forged forged_entry"'),
            ({"name": FORGED}, "ok\\r\\n2026-01-01 ERROR billing.forged forged_entry m"),
            ({"a\nERROR forged": "x"}, 'm "a\\nERROR forged"=x'),
            ({"msg": "bell\x07\x85next"}, "bell\\x07\\x85next"),
        ],
        ids=["value-line-breaks", "logger-name", "key-line-break", "message"],
    )
    def test_the_text_line_is_one_line_and_escapes_what_could_forge_a_record(
        self, fields: dict[str, object], written: str
    ) -> None:
        line = _text(a_record(**{"msg": "m", **fields}))

        assert len(line.splitlines()) == 1
        assert line.endswith(" " + written)

    def test_json_keeps_one_line_for_a_value_with_line_breaks(self) -> None:
        line = JsonFormatter().format(a_record(user=self.FORGED))

        assert "\n" not in line
        assert json.loads(line)["user"] == self.FORGED


_AWKWARD_CHARS = '\x00\x1f\x7f\x85\r\n\t\u2028\u2029="\\ '
_AWKWARD = st.text(alphabet=_AWKWARD_CHARS + "a")
_ONE_AWKWARD = st.builds(
    "".join,
    st.tuples(
        st.text("ab", max_size=3),
        st.sampled_from(sorted(_AWKWARD_CHARS)),
        st.text("ab", max_size=3),
    ),
)
_ANY_TEXT = st.one_of(st.text(), _AWKWARD, _ONE_AWKWARD)


class TestLogfmtProperties:
    ESCAPES = {"n": "\n", "r": "\r", "t": "\t", "\\": "\\", '"': '"'}

    @classmethod
    def read(cls, token: str) -> str:
        if not token.startswith('"'):
            return token
        return re.sub(
            r"\\(?:x([0-9a-f]{2})|u([0-9a-f]{4})|(.))",
            lambda m: chr(int(m[1] or m[2], 16)) if m[3] is None else cls.ESCAPES[m[3]],
            token[1:-1],
        )

    @staticmethod
    def has_control(text: str) -> bool:
        return any(unicodedata.category(char) in {"Cc", "Zl", "Zp"} for char in text)

    @classmethod
    def needs_quotes(cls, text: str) -> bool:
        return cls.has_control(text) or any(char.isspace() or char in '="\\' for char in text)

    @pytest.mark.parametrize("write", [Logfmt.token, Logfmt.one_line], ids=["token", "one-line"])
    @given(text=_ANY_TEXT)
    def test_written_text_holds_no_break_or_control_character(
        self, write: Callable[[str], str], text: str
    ) -> None:
        assert not self.has_control(write(text))

    @given(text=_ANY_TEXT)
    def test_a_token_is_quoted_exactly_when_the_text_needs_it(self, text: str) -> None:
        written = Logfmt.token(text)

        assert (written.startswith('"') and written.endswith('"')) == self.needs_quotes(text)

    @given(text=_ANY_TEXT)
    def test_a_token_reads_back_to_its_text(self, text: str) -> None:
        assert self.read(Logfmt.token(text)) == text
