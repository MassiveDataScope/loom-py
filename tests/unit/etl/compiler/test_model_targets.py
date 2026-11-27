"""Targets and sources declared with a model: normalisation, default keys and compile checks."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, time
from typing import Annotated, Any

import pytest

from loom.core.errors import InvalidUse
from loom.core.model import BaseModel, BusinessKey, ColumnField, Field
from loom.core.model.types import DateTime, Text
from loom.core.routing import LogicalRef
from loom.core.schema import SchemaMode
from loom.etl import Format, FromFile, FromTable, IntoHistory, IntoTable, col
from loom.etl.compiler import ETLCompilationError, ETLErrorCode
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import HistoryDateType
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.schema import ColumnSchema, LoomDtype
from tests.helpers.loggers import CapturedLog
from tests.unit.etl.compiler.probes import CompileTarget
from tests.unit.etl.models import Order, Rate


class Ledger(BaseModel):
    __tablename__ = "ledger"
    ledger_id: int = ColumnField(primary_key=True, autoincrement=True)
    memo: Annotated[str, Text]


class Booking(BaseModel):
    __tablename__ = "booking"
    __business_key__ = BusinessKey("customer_id", "slot")
    booking_id: int = ColumnField(primary_key=True, autoincrement=True)
    customer_id: Annotated[str, Text]
    slot: Annotated[str, Text]
    note: Annotated[str, Text]


class Tick(BaseModel):
    __tablename__ = "tick"
    tick_id: Annotated[str, Field(primary_key=True)]
    valid_from: Annotated[datetime, DateTime(tz=True)]
    valid_to: Annotated[datetime, DateTime(tz=True)] | None


class Clocked(BaseModel):
    __tablename__ = "clocked"
    clock_id: Annotated[str, Field(primary_key=True)]
    opens_at: time


class Nameless(BaseModel):
    nameless_id: Annotated[int, Field(primary_key=True)]


class Undated(BaseModel):
    __tablename__ = "undated"
    undated_id: Annotated[str, Field(primary_key=True)]
    valid_from: str
    valid_to: str | None


_MODEL_LOGGER = "loom.etl.compiler._validators_model"
_LAKE_REFS = {
    "reference": "sales.order",
    "logical-ref": LogicalRef("sales.order"),
    "table-ref": TableRef("sales.order"),
}
_MERGE_KEYS = {
    "primary-key": (IntoTable(Order).upsert(), (Order.c.order_id,)),
    "business-key": (IntoTable(Booking).update(), (Booking.c.customer_id, Booking.c.slot)),
    "explicit-keys": (IntoTable(Order).upsert(keys=(Order.c.customer_id,)), (Order.c.customer_id,)),
    "string-without-keys": (IntoTable("sales.order").upsert(), ()),
}
_REFUSALS = {
    "unknown-key": (
        lambda: IntoTable(Order).upsert(keys=("order_ref",)),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "order_ref",
    ),
    "unknown-include": (
        lambda: IntoTable(Order).update(include=("status",)),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
    ),
    "unknown-exclude": (
        lambda: IntoTable(Order).upsert(exclude=("status",)),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
    ),
    "unknown-partition": (
        lambda: IntoTable(Order).replace_partitions("status"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
    ),
    "unknown-track": (
        lambda: IntoHistory(Rate, keys=(Rate.c.rate_id,), track=("tier",), effective_date="d"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "tier",
    ),
    "missing-valid-to": (
        lambda: IntoHistory(Rate, keys=(Rate.c.rate_id,), effective_date="d", valid_to="ends_on"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "ends_on",
    ),
    "missing-deleted-at": (
        lambda: IntoHistory(
            Tick, keys=(Tick.c.tick_id,), effective_date="d", delete_policy="soft_delete"
        ),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "deleted_at",
    ),
    "unknown-replace-where": (
        lambda: IntoTable(Order).replace_where(col("status") == "open"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
    ),
    "time-column": (
        lambda: IntoTable(Clocked).append(),
        ETLErrorCode.MODEL_COLUMN_TYPE_REFUSED,
        Clocked.c.opens_at,
    ),
    "evolve": (
        lambda: IntoTable(Order).append(schema=SchemaMode.EVOLVE),
        ETLErrorCode.MODEL_SCHEMA_MODE_UNSUPPORTED,
        "schema",
    ),
    "autoincrement-key": (
        lambda: IntoTable(Ledger).upsert(),
        ETLErrorCode.MODEL_KEY_NOT_IN_FRAME,
        Ledger.c.ledger_id,
    ),
    "contradicting-date-type": (
        lambda: IntoHistory(
            Rate, keys=(Rate.c.rate_id,), effective_date="d", date_type="timestamp"
        ),
        ETLErrorCode.MODEL_DATE_TYPE_MISMATCH,
        "date_type",
    ),
    "non-temporal-valid-from": (
        lambda: IntoHistory(Undated, keys=(Undated.c.undated_id,), effective_date="d"),
        ETLErrorCode.MODEL_BOUNDARY_NOT_TEMPORAL,
        "valid_from",
    ),
}
_REFUSAL_MESSAGES = {
    "unknown-key": "keys",
    "unknown-include": "include",
    "unknown-exclude": "exclude",
    "unknown-partition": "partition_cols",
    "unknown-track": "track",
    "missing-valid-to": "valid_to",
    "missing-deleted-at": "deleted_at",
    "unknown-replace-where": "replace_where",
    "time-column": r"Clocked\.opens_at is a Time.*store it as a Text or a DateTime",
    "evolve": "EVOLVE",
    "autoincrement-key": "autoincrement.*not supported yet",
    "contradicting-date-type": "'timestamp'.*'valid_from'.*'date'",
    "non-temporal-valid-from": "holds no dates",
}
_SOURCE_REFUSALS = {
    "unknown-column": (
        FromTable(Order).columns(Order.c.order_id, "status"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
    ),
    "unknown-where-column": (
        FromTable(Order).where(col("status") == "open"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
    ),
    "unknown-parse-json-column": (
        FromTable(Order).parse_json("payload", list[str]),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "payload",
    ),
    "time-column": (
        FromTable(Clocked),
        ETLErrorCode.MODEL_COLUMN_TYPE_REFUSED,
        "opens_at",
    ),
    "model-without-table-name": (
        FromTable("wh.nameless").with_schema(Nameless),
        ETLErrorCode.MODEL_COLUMN_TYPE_REFUSED,
        "model",
    ),
}
_DATE_TYPES = {
    "string": ("wh.rate", None, HistoryDateType.DATE),
    "string-explicit": ("wh.rate", "timestamp", HistoryDateType.TIMESTAMP),
    "date-column": (Rate, None, HistoryDateType.DATE),
    "datetime-column": (Tick, None, HistoryDateType.TIMESTAMP),
    "matching-explicit": (Tick, "timestamp", HistoryDateType.TIMESTAMP),
}


class Pending(BaseModel):
    __tablename__ = "pending"
    pending_id: Annotated[str, Field(primary_key=True)]
    valid_from: Annotated[datetime, DateTime(tz=True)]
    valid_to: Annotated[datetime, DateTime(tz=True)] | None
    when: NotDefinedYet  # noqa: F821


def _csv_file() -> FromFile:
    return FromFile("s3://raw/p.csv", format=Format.CSV)


_BUILDERS_BEFORE_COMPILE = {
    "from-table": lambda: FromTable(Pending),
    "from-table-with-model-schema": lambda: FromTable("wh.pending").with_schema(Pending),
    "from-table-columns": lambda: FromTable(Pending).columns(Pending.c.pending_id),
    "from-table-where": lambda: FromTable(Pending).where(col(Pending.c.pending_id) == "a"),
    "from-table-parse-json": lambda: FromTable(Pending).parse_json("payload", list[str]),
    "from-file-with-model-schema": lambda: _csv_file().with_schema(Pending),
    "from-file-parse-json": lambda: _csv_file().parse_json("payload", list[str]),
    "append": lambda: IntoTable(Pending).append(),
    "replace": lambda: IntoTable(Pending).replace(),
    "replace-partitions": lambda: IntoTable(Pending).replace_partitions(Pending.c.pending_id),
    "replace-where": lambda: IntoTable(Pending).replace_where(col(Pending.c.pending_id) == "a"),
    "upsert": lambda: IntoTable(Pending).upsert(),
    "upsert-explicit-keys": lambda: IntoTable(Pending).upsert(keys=(Pending.c.pending_id,)),
    "update": lambda: IntoTable(Pending).update(include=(Pending.c.valid_to,)),
    "history": lambda: IntoHistory(Pending, keys=(Pending.c.pending_id,), effective_date="d"),
}


_CONSISTENT_TARGETS = {
    "upsert-primary-key": lambda: IntoTable(Order).upsert(),
    "update-include": lambda: IntoTable(Order).update(include=(Order.c.amount,)),
    "upsert-business-key": lambda: IntoTable(Booking).upsert(),
    "upsert-explicit-keys": lambda: IntoTable(Ledger).upsert(keys=(Ledger.c.memo,)),
    "replace-partitions": lambda: IntoTable(Order).replace_partitions(Order.c.customer_id),
    "history-date": lambda: IntoHistory(Rate, keys=(Rate.c.rate_id,), effective_date="d"),
    "history-datetime": lambda: IntoHistory(
        Tick, keys=(Tick.c.tick_id,), effective_date="d", track=()
    ),
}


class TestModelDeclaration:
    @pytest.mark.parametrize("ref", _LAKE_REFS.values(), ids=list(_LAKE_REFS))
    def test_a_source_of_a_model_is_the_reference_with_the_schema_of_the_model(
        self, ref: Any
    ) -> None:
        from_model = FromTable(Order)._to_spec("orders")

        assert from_model == FromTable(ref).with_schema(Order)._to_spec("orders")

    @pytest.mark.parametrize(("target", "keys"), _MERGE_KEYS.values(), ids=list(_MERGE_KEYS))
    def test_merge_keys_default_to_the_conflict_key_of_a_model(
        self, target: IntoTable, keys: tuple[str, ...]
    ) -> None:
        assert target._to_spec().upsert_keys == keys

    @pytest.mark.parametrize(
        ("target", "date_type", "expected"), _DATE_TYPES.values(), ids=list(_DATE_TYPES)
    )
    def test_history_date_type_follows_the_model_valid_from(
        self, target: Any, date_type: Any, expected: HistoryDateType
    ) -> None:
        history = IntoHistory(target, keys=("k",), effective_date="d", date_type=date_type)

        assert history._to_spec().date_type is expected

    @pytest.mark.parametrize(
        "build", _BUILDERS_BEFORE_COMPILE.values(), ids=list(_BUILDERS_BEFORE_COMPILE)
    )
    def test_a_builder_reads_nothing_from_a_model_whose_annotations_do_not_resolve_yet(
        self, build: Callable[[], Any]
    ) -> None:
        assert build() is not None

    @pytest.mark.parametrize("merge", [IntoTable.upsert, IntoTable.update])
    def test_a_model_target_refuses_explicitly_empty_keys(
        self, compile_target: CompileTarget, merge: Callable[..., IntoTable]
    ) -> None:
        with pytest.raises(ETLCompilationError) as refused:
            compile_target(merge(IntoTable(Order), keys=()))

        assert refused.value.code is ETLErrorCode.UPSERT_NO_KEYS

    def test_a_source_declared_with_a_model_refuses_another_schema(self) -> None:
        with pytest.raises(InvalidUse, match=r"FromTable\(Order\).*drop it"):
            FromTable(Order).with_schema((ColumnSchema("id", LoomDtype.INT64),))


class TestModelSourceCompilation:
    def test_a_source_consistent_with_its_model_compiles(
        self, compile_source: CompileTarget
    ) -> None:
        compile_source(
            FromTable(Order)
            .where(col(Order.c.amount) > 0)
            .columns(Order.c.order_id, Order.c.amount)
        )

    @pytest.mark.parametrize(
        ("source", "code", "field"), _SOURCE_REFUSALS.values(), ids=list(_SOURCE_REFUSALS)
    )
    def test_a_source_the_model_contradicts_is_refused_naming_the_field(
        self, compile_source: CompileTarget, source: FromTable, code: ETLErrorCode, field: str
    ) -> None:
        with pytest.raises(ETLCompilationError) as refused:
            compile_source(source)

        assert (refused.value.code, refused.value.field) == (code, field)


class TestModelTargetCompilation:
    @pytest.mark.parametrize("declare", _CONSISTENT_TARGETS.values(), ids=list(_CONSISTENT_TARGETS))
    def test_a_target_consistent_with_its_model_compiles(
        self, compile_target: CompileTarget, declare: Callable[[], Any]
    ) -> None:
        compile_target(declare())

    @pytest.mark.parametrize(("declare", "code", "field"), _REFUSALS.values(), ids=list(_REFUSALS))
    def test_a_target_the_model_contradicts_is_refused_naming_the_field(
        self,
        compile_target: CompileTarget,
        declare: Callable[[], Any],
        code: ETLErrorCode,
        field: str,
    ) -> None:
        with pytest.raises(ETLCompilationError) as refused:
            compile_target(declare())

        assert (refused.value.code, refused.value.field) == (code, field)

    @pytest.mark.parametrize("refusal", _REFUSAL_MESSAGES)
    def test_a_refusal_names_the_model_and_the_fix(
        self, compile_target: CompileTarget, refusal: str
    ) -> None:
        declare = _REFUSALS[refusal][0]

        with pytest.raises(ETLCompilationError, match=_REFUSAL_MESSAGES[refusal]):
            compile_target(declare())


class TestModelTargetLogs:
    def test_a_resolved_model_logs_once_with_model_and_table(
        self, compile_target: CompileTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER):
            compile_target(IntoTable(Order).upsert())

        (fields,) = CapturedLog(caplog, _MODEL_LOGGER).named(EtlEvent.MODEL_RESOLVED)
        assert (fields[EtlKey.MODEL], fields[EtlKey.TABLE]) == ("Order", "sales.order")

    def test_a_refused_model_logs_the_blamed_field_and_code_once(
        self, compile_target: CompileTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        with (
            caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER),
            pytest.raises(ETLCompilationError),
        ):
            compile_target(IntoTable(Order).upsert(keys=("order_ref",)))

        (logged,) = CapturedLog(caplog, _MODEL_LOGGER).named(EtlEvent.MODEL_REFUSED)
        assert (logged[EtlKey.FIELD], logged[EtlKey.REASON]) == (
            "order_ref",
            ETLErrorCode.MODEL_COLUMN_UNKNOWN.value,
        )

    def test_a_string_target_logs_no_model_event(
        self, compile_target: CompileTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER):
            compile_target(IntoTable("sales.order").upsert(keys=("order_id",)))

        assert CapturedLog(caplog, _MODEL_LOGGER).messages == []
