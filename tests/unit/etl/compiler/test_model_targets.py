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
from tests.unit.etl.compiler.probes import CompileDeclaration
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
_UNKNOWN_COLUMNS = {
    "unknown-key": (lambda: IntoTable(Order).upsert(keys=("order_ref",)), "order_ref", "keys"),
    "unknown-include": (lambda: IntoTable(Order).update(include=("status",)), "status", "include"),
    "unknown-exclude": (lambda: IntoTable(Order).upsert(exclude=("status",)), "status", "exclude"),
    "unknown-partition": (
        lambda: IntoTable(Order).replace_partitions("status"),
        "status",
        "partition_cols",
    ),
    "unknown-track": (
        lambda: IntoHistory(Rate, keys=(Rate.c.rate_id,), track=("tier",), effective_date="d"),
        "tier",
        "track",
    ),
    "missing-valid-to": (
        lambda: IntoHistory(Rate, keys=(Rate.c.rate_id,), effective_date="d", valid_to="ends_on"),
        "ends_on",
        "valid_to",
    ),
    "missing-deleted-at": (
        lambda: IntoHistory(
            Tick, keys=(Tick.c.tick_id,), effective_date="d", delete_policy="soft_delete"
        ),
        "deleted_at",
        "deleted_at",
    ),
    "unknown-replace-where": (
        lambda: IntoTable(Order).replace_where(col("status") == "open"),
        "status",
        "replace_where",
    ),
}
_REFUSALS = {
    "time-column": (
        lambda: IntoTable(Clocked).append(),
        ETLErrorCode.MODEL_COLUMN_TYPE_REFUSED,
        Clocked.c.opens_at,
        r"Clocked\.opens_at is a Time.*store it as a Text or a DateTime",
    ),
    "evolve": (
        lambda: IntoTable(Order).append(schema=SchemaMode.EVOLVE),
        ETLErrorCode.MODEL_SCHEMA_MODE_UNSUPPORTED,
        "schema",
        "EVOLVE.*only allows STRICT",
    ),
    "autoincrement-key": (
        lambda: IntoTable(Ledger).upsert(),
        ETLErrorCode.MODEL_KEY_NOT_IN_FRAME,
        Ledger.c.ledger_id,
        "autoincrement.*not supported yet",
    ),
    "contradicting-date-type": (
        lambda: IntoHistory(
            Rate, keys=(Rate.c.rate_id,), effective_date="d", date_type="timestamp"
        ),
        ETLErrorCode.MODEL_DATE_TYPE_MISMATCH,
        "date_type",
        "'timestamp'.*'valid_from'.*'date'",
    ),
    "non-temporal-valid-from": (
        lambda: IntoHistory(Undated, keys=(Undated.c.undated_id,), effective_date="d"),
        ETLErrorCode.MODEL_BOUNDARY_NOT_TEMPORAL,
        "valid_from",
        "holds no dates",
    ),
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


_SOURCES_BEFORE_COMPILE = {
    "from-table": FromTable(Pending),
    "from-table-where": FromTable(Pending).where(col(Pending.c.pending_id) == "a"),
    "from-file-with-model-schema": FromFile("s3://raw/p.csv", format=Format.CSV).with_schema(
        Pending
    ),
}
_TARGETS_BEFORE_COMPILE = {
    "replace": IntoTable(Pending).replace(),
    "upsert": IntoTable(Pending).upsert(),
    "update": IntoTable(Pending).update(include=(Pending.c.valid_to,)),
    "history": IntoHistory(Pending, keys=(Pending.c.pending_id,), effective_date="d"),
}
NotDefinedYet = Annotated[str, Text]


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
        assert target._to_spec().keys == keys

    @pytest.mark.parametrize(
        ("target", "date_type", "expected"), _DATE_TYPES.values(), ids=list(_DATE_TYPES)
    )
    def test_history_date_type_follows_the_model_valid_from(
        self, target: Any, date_type: Any, expected: HistoryDateType
    ) -> None:
        history = IntoHistory(target, keys=("k",), effective_date="d", date_type=date_type)

        assert history._to_spec().date_type is expected

    @pytest.mark.parametrize(
        "source", _SOURCES_BEFORE_COMPILE.values(), ids=list(_SOURCES_BEFORE_COMPILE)
    )
    def test_a_source_declared_before_its_annotations_are_defined_compiles(
        self, source: Any, compile_source: CompileDeclaration
    ) -> None:
        plan = compile_source(source)

        assert plan.source_bindings[0].spec.read_schema[-1].name == "when"

    @pytest.mark.parametrize(
        "target", _TARGETS_BEFORE_COMPILE.values(), ids=list(_TARGETS_BEFORE_COMPILE)
    )
    def test_a_target_declared_before_its_annotations_are_defined_compiles(
        self, target: Any, compile_target: CompileDeclaration
    ) -> None:
        plan = compile_target(target)

        assert plan.target_binding.spec.model_table.column_names[-1] == "when"

    @pytest.mark.parametrize("merge", [IntoTable.upsert, IntoTable.update])
    def test_a_model_target_refuses_explicitly_empty_keys(
        self, compile_target: CompileDeclaration, merge: Callable[..., IntoTable]
    ) -> None:
        with pytest.raises(ETLCompilationError) as refused:
            compile_target(merge(IntoTable(Order), keys=()))

        assert refused.value.code is ETLErrorCode.UPSERT_NO_KEYS

    def test_a_source_declared_with_a_model_refuses_another_schema(self) -> None:
        with pytest.raises(InvalidUse, match=r"FromTable\(Order\).*drop it"):
            FromTable(Order).with_schema((ColumnSchema("id", LoomDtype.INT64),))


class TestModelSourceCompilation:
    def test_a_source_consistent_with_its_model_compiles(
        self, compile_source: CompileDeclaration
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
        self, compile_source: CompileDeclaration, source: FromTable, code: ETLErrorCode, field: str
    ) -> None:
        with pytest.raises(ETLCompilationError) as refused:
            compile_source(source)

        assert (refused.value.code, refused.value.field) == (code, field)


class TestModelTargetCompilation:
    @pytest.mark.parametrize("declare", _CONSISTENT_TARGETS.values(), ids=list(_CONSISTENT_TARGETS))
    def test_a_target_consistent_with_its_model_compiles(
        self, compile_target: CompileDeclaration, declare: Callable[[], Any]
    ) -> None:
        compile_target(declare())

    @pytest.mark.parametrize(
        ("declare", "field", "role"), _UNKNOWN_COLUMNS.values(), ids=list(_UNKNOWN_COLUMNS)
    )
    def test_a_target_naming_a_column_the_model_lacks_is_refused_naming_the_role(
        self, compile_target: CompileDeclaration, declare: Callable[[], Any], field: str, role: str
    ) -> None:
        with pytest.raises(ETLCompilationError, match=rf"{role} names column {field!r}") as refused:
            compile_target(declare())

        assert (refused.value.code, refused.value.field) == (
            ETLErrorCode.MODEL_COLUMN_UNKNOWN,
            field,
        )

    @pytest.mark.parametrize(
        ("declare", "code", "field", "message"), _REFUSALS.values(), ids=list(_REFUSALS)
    )
    def test_a_target_the_model_contradicts_is_refused_naming_the_field_and_the_fix(
        self,
        compile_target: CompileDeclaration,
        declare: Callable[[], Any],
        code: ETLErrorCode,
        field: str,
        message: str,
    ) -> None:
        with pytest.raises(ETLCompilationError, match=message) as refused:
            compile_target(declare())

        assert (refused.value.code, refused.value.field) == (code, field)


class TestModelTargetLogs:
    def test_a_resolved_model_logs_once_with_model_and_table(
        self, compile_target: CompileDeclaration, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER):
            compile_target(IntoTable(Order).upsert())

        (fields,) = CapturedLog(caplog, _MODEL_LOGGER).named(EtlEvent.MODEL_RESOLVED)
        assert (fields[EtlKey.MODEL], fields[EtlKey.TABLE]) == ("Order", "sales.order")

    def test_a_refused_model_logs_the_blamed_field_and_code_once(
        self, compile_target: CompileDeclaration, caplog: pytest.LogCaptureFixture
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
        self, compile_target: CompileDeclaration, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER):
            compile_target(IntoTable("sales.order").upsert(keys=("order_id",)))

        assert CapturedLog(caplog, _MODEL_LOGGER).messages == []
