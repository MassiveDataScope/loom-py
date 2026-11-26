"""Targets and sources declared with a model: normalisation, default keys and compile checks."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any

import pytest

from loom.core.errors import InvalidType
from loom.core.model import BaseModel, BusinessKey, ColumnField, Field
from loom.core.model.types import DateTime, Numeric, Text
from loom.core.routing import LogicalRef
from loom.core.schema import SchemaMode
from loom.etl import FromTable, IntoHistory, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLErrorCode
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import HistoryDateType
from loom.etl.event import EtlEvent, EtlKey
from tests.helpers.loggers import CapturedLog
from tests.unit.etl.compiler.conftest import CompileTarget
from tests.unit.etl.engines.frame_types import Order


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


class Rate(BaseModel):
    __tablename__ = "rate"
    rate_id: Annotated[str, Field(primary_key=True)]
    price: Annotated[Decimal, Numeric(precision=12, scale=2)]
    valid_from: date
    valid_to: date | None
    deleted_at: datetime | None


class Tick(BaseModel):
    __tablename__ = "tick"
    tick_id: Annotated[str, Field(primary_key=True)]
    valid_from: Annotated[datetime, DateTime(tz=True)]
    valid_to: Annotated[datetime, DateTime(tz=True)] | None


class Undated(BaseModel):
    __tablename__ = "undated"
    undated_id: Annotated[str, Field(primary_key=True)]
    valid_from: str
    valid_to: str | None


class Nameless(BaseModel):
    order_id: Annotated[str, Field(primary_key=True)]


_MODEL_LOGGER = "loom.etl.compiler._validators_model"
_LAKE_REFS = {
    "reference": "sales.order",
    "logical-ref": LogicalRef("sales.order"),
    "table-ref": TableRef("sales.order"),
}
_MERGE_KEYS = {
    "primary-key": (IntoTable(Order).upsert(), ("order_id",)),
    "business-key": (IntoTable(Booking).update(), ("customer_id", "slot")),
    "explicit-keys": (IntoTable(Order).upsert(keys=(Order.c.customer_id,)), ("customer_id",)),
    "string-without-keys": (IntoTable("sales.order").upsert(), ()),
}
_REFUSALS = {
    "unknown-key": (
        lambda: IntoTable(Order).upsert(keys=("order_ref",)),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "order_ref",
        "keys",
    ),
    "unknown-include": (
        lambda: IntoTable(Order).update(include=("status",)),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
        "include",
    ),
    "unknown-exclude": (
        lambda: IntoTable(Order).upsert(exclude=("status",)),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
        "exclude",
    ),
    "unknown-partition": (
        lambda: IntoTable(Order).replace_partitions("status"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "status",
        "partition_cols",
    ),
    "unknown-track": (
        lambda: IntoHistory(Rate, keys=(Rate.c.rate_id,), track=("tier",), effective_date="d"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "tier",
        "track",
    ),
    "missing-valid-to": (
        lambda: IntoHistory(Rate, keys=("rate_id",), effective_date="d", valid_to="ends_on"),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "ends_on",
        "valid_to",
    ),
    "missing-deleted-at": (
        lambda: IntoHistory(
            Tick, keys=("tick_id",), effective_date="d", delete_policy="soft_delete"
        ),
        ETLErrorCode.MODEL_COLUMN_UNKNOWN,
        "deleted_at",
        "deleted_at",
    ),
    "evolve": (
        lambda: IntoTable(Order).append(schema=SchemaMode.EVOLVE),
        ETLErrorCode.MODEL_SCHEMA_MODE_UNSUPPORTED,
        "schema",
        "EVOLVE",
    ),
    "overwrite-history": (
        lambda: IntoHistory(
            Rate, keys=("rate_id",), effective_date="d", schema=SchemaMode.OVERWRITE
        ),
        ETLErrorCode.MODEL_SCHEMA_MODE_UNSUPPORTED,
        "schema",
        "OVERWRITE",
    ),
    "autoincrement-key": (
        lambda: IntoTable(Ledger).upsert(),
        ETLErrorCode.MODEL_KEY_NOT_IN_FRAME,
        "ledger_id",
        "autoincrement.*spec 024",
    ),
    "contradicting-date-type": (
        lambda: IntoHistory(Rate, keys=("rate_id",), effective_date="d", date_type="timestamp"),
        ETLErrorCode.MODEL_DATE_TYPE_MISMATCH,
        "date_type",
        "'timestamp'.*'valid_from'.*'date'",
    ),
    "non-temporal-valid-from": (
        lambda: IntoHistory(Undated, keys=("undated_id",), effective_date="d"),
        ETLErrorCode.MODEL_BOUNDARY_NOT_TEMPORAL,
        "valid_from",
        "holds no dates",
    ),
}
_DATE_TYPES = {
    "string": ("wh.rate", None, HistoryDateType.DATE),
    "string-explicit": ("wh.rate", "timestamp", HistoryDateType.TIMESTAMP),
    "date-column": (Rate, None, HistoryDateType.DATE),
    "datetime-column": (Tick, None, HistoryDateType.TIMESTAMP),
    "matching-explicit": (Tick, "timestamp", HistoryDateType.TIMESTAMP),
}


class TestModelDeclaration:
    @pytest.mark.parametrize("ref", _LAKE_REFS.values(), ids=list(_LAKE_REFS))
    def test_a_source_of_a_model_is_the_reference_with_the_schema_of_the_model(
        self, ref: Any
    ) -> None:
        from_model = FromTable(Order)._to_spec("orders")

        assert from_model == FromTable(ref).with_schema(Order)._to_spec("orders")

    def test_a_model_names_its_table_with_its_tablename(self) -> None:
        assert IntoTable(Order).append()._to_spec().table_ref == TableRef("sales.order")
        assert IntoHistory(Rate, keys=("rate_id",), effective_date="d")._to_spec().table_ref == (
            TableRef("rate")
        )

    def test_a_class_without_a_tablename_is_refused_naming_it(self) -> None:
        with pytest.raises(InvalidType, match="__tablename__"):
            IntoTable(Nameless)

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


class TestModelTargetCompilation:
    @pytest.mark.parametrize(
        "declare",
        [
            lambda: IntoTable(Order).upsert(),
            lambda: IntoTable(Order).update(include=(Order.c.amount,)),
            lambda: IntoTable(Booking).upsert(),
            lambda: IntoTable(Ledger).upsert(keys=(Ledger.c.memo,)),
            lambda: IntoTable(Order).replace_partitions(Order.c.customer_id),
            lambda: IntoHistory(Rate, keys=(Rate.c.rate_id,), effective_date="d"),
            lambda: IntoHistory(Tick, keys=(Tick.c.tick_id,), effective_date="d", track=()),
        ],
    )
    def test_a_target_consistent_with_its_model_compiles(
        self, compile_target: CompileTarget, declare: Callable[[], Any]
    ) -> None:
        compile_target(declare())

    @pytest.mark.parametrize(
        ("declare", "code", "field", "message"), _REFUSALS.values(), ids=list(_REFUSALS)
    )
    def test_a_target_the_model_contradicts_is_refused_naming_model_field_and_fix(
        self,
        compile_target: CompileTarget,
        declare: Callable[[], Any],
        code: ETLErrorCode,
        field: str,
        message: str,
    ) -> None:
        with pytest.raises(ETLCompilationError, match=message) as refused:
            compile_target(declare())

        assert (refused.value.code, refused.value.field) == (code, field)

    def test_a_string_target_still_requires_keys(self, compile_target: CompileTarget) -> None:
        with pytest.raises(ETLCompilationError) as refused:
            compile_target(IntoTable("sales.order").upsert())

        assert refused.value.code is ETLErrorCode.UPSERT_NO_KEYS


class TestModelTargetLogs:
    def test_a_resolved_model_logs_once_with_model_and_table(
        self, compile_target: CompileTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER):
            compile_target(IntoTable(Order).upsert())

        (fields,) = CapturedLog(caplog, _MODEL_LOGGER).named(EtlEvent.MODEL_RESOLVED)
        assert (fields[EtlKey.MODEL], fields[EtlKey.TABLE]) == ("Order", "sales.order")

    def test_a_refusal_logs_once_with_model_table_field_and_reason(
        self, compile_target: CompileTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        with (
            caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER),
            pytest.raises(ETLCompilationError),
        ):
            compile_target(IntoTable(Ledger).upsert())

        log = CapturedLog(caplog, _MODEL_LOGGER)
        (fields,) = log.named(EtlEvent.MODEL_REFUSED)
        assert fields[EtlKey.MODEL] == "Ledger"
        assert fields[EtlKey.TABLE] == "ledger"
        assert fields[EtlKey.FIELD] == "ledger_id"
        assert fields[EtlKey.REASON] == ETLErrorCode.MODEL_KEY_NOT_IN_FRAME.value
        assert EtlEvent.MODEL_RESOLVED not in log.messages

    def test_a_string_target_logs_no_model_event(
        self, compile_target: CompileTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_MODEL_LOGGER):
            compile_target(IntoTable("sales.order").upsert(keys=("order_id",)))

        assert CapturedLog(caplog, _MODEL_LOGGER).messages == []
