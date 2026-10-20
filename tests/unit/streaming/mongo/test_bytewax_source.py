"""Unit tests for the MongoDB CDC Bytewax source runtime."""

from __future__ import annotations

import builtins
from typing import Any, Literal

import pytest
from pymongo.errors import OperationFailure

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.streaming.compiler import CompiledMongoCDCSource  # noqa: E402
from loom.streaming.mongo import MongoSourceConfig  # noqa: E402
from loom.streaming.mongo._bytewax_source import (  # noqa: E402
    MongoCDCPartition,
    _build_mongo_client,
    _build_pipeline,
    _resolve_watch_options,
)


def test_build_pipeline_filters_multi_collection_without_mutating_options() -> None:
    watch_options = {
        "pipeline": [{"$match": {"operationType": "insert"}}],
        "full_document": "updateLookup",
    }

    pipeline = _build_pipeline(("orders", "invoices"), watch_options)

    assert pipeline == [
        {"$match": {"ns.coll": {"$in": ["orders", "invoices"]}}},
        {"$match": {"operationType": "insert"}},
    ]
    assert watch_options["pipeline"] == [{"$match": {"operationType": "insert"}}]


def test_resolve_watch_options_adds_resume_token_without_pipeline() -> None:
    resolved = _resolve_watch_options(
        {"pipeline": [{"$match": {"operationType": "update"}}], "max_await_time_ms": 1000},
        {"token": "resume-1"},
    )

    assert resolved == {
        "max_await_time_ms": 1000,
        "resume_after": {"token": "resume-1"},
    }


def test_partition_snapshots_latest_resume_token(monkeypatch: Any) -> None:
    partition = _build_partition(
        monkeypatch,
        [
            {
                "_id": {"_data": "resume-2"},
                "operationType": "insert",
                "ns": {"db": "app", "coll": "orders"},
                "documentKey": {"_id": "order-1"},
            }
        ],
    )

    batch = partition.next_batch()

    assert len(batch) == 1
    assert batch[0].payload.event_id == "resume-2"
    assert partition.snapshot() == {"_data": "resume-2"}
    partition.close()


def test_partition_restarts_from_now_when_oplog_has_expired(monkeypatch: Any) -> None:
    streams = [
        _FailingStream(
            OperationFailure(
                "ChangeStreamHistoryLost: resume point may no longer be in the oplog",
                code=286,
                details={"codeName": "ChangeStreamHistoryLost"},
            )
        ),
        _FakeStream(
            [
                {
                    "_id": {"_data": "resume-3"},
                    "operationType": "insert",
                    "ns": {"db": "app", "coll": "orders"},
                    "documentKey": {"_id": "order-2"},
                }
            ]
        ),
    ]
    partition = _build_partition(
        monkeypatch,
        [],
        on_oplog_expired="restart_from_now",
        streams=streams,
    )

    batch = partition.next_batch()

    assert len(batch) == 1
    assert batch[0].payload.event_id == "resume-3"
    assert partition.snapshot() == {"_data": "resume-3"}
    partition.close()


def test_partition_raises_when_oplog_has_expired_and_policy_is_fail(monkeypatch: Any) -> None:
    partition = _build_partition(
        monkeypatch,
        [],
        on_oplog_expired="fail",
        streams=[
            _FailingStream(
                OperationFailure(
                    "ChangeStreamHistoryLost: resume point may no longer be in the oplog",
                    code=286,
                    details={"codeName": "ChangeStreamHistoryLost"},
                )
            )
        ],
    )

    with pytest.raises(OperationFailure, match="ChangeStreamHistoryLost"):
        partition.next_batch()

    partition.close()


def test_build_mongo_client_raises_helpful_import_error(monkeypatch: Any) -> None:
    real_import = builtins.__import__

    def _fake_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "pymongo" or name.startswith("pymongo."):
            raise ImportError("missing pymongo")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    source = _compiled_source()
    with pytest.raises(ImportError, match=r"loom-kernel\[mongo\]"):
        _build_mongo_client(source)


def _build_partition(
    monkeypatch: Any,
    changes: list[dict[str, object]],
    *,
    on_oplog_expired: Literal["fail", "restart_from_now"] = "fail",
    streams: list[object] | None = None,
) -> MongoCDCPartition:
    open_streams = list(streams) if streams is not None else [_FakeStream(changes)]
    monkeypatch.setattr(
        "loom.streaming.mongo._bytewax_source._build_mongo_client",
        lambda source: _FakeClient(),
    )
    monkeypatch.setattr(
        "loom.streaming.mongo._bytewax_source._open_change_stream",
        lambda client, source, resume_state: open_streams.pop(0),
    )
    return MongoCDCPartition(
        _compiled_source(on_oplog_expired=on_oplog_expired),
        resume_state={"token": "resume-1"},
    )


def _compiled_source(
    *,
    on_oplog_expired: Literal["fail", "restart_from_now"] = "fail",
) -> CompiledMongoCDCSource:
    return CompiledMongoCDCSource(
        settings=MongoSourceConfig(
            uri="mongodb://localhost:27017",
            database="app",
            on_oplog_expired=on_oplog_expired,
        ),
        collections=("orders",),
        watch_options={},
    )


class _FakeStream:
    def __init__(self, changes: list[dict[str, object]]) -> None:
        self._changes = list(changes)
        self.closed = False

    def try_next(self) -> dict[str, object] | None:
        if not self._changes:
            return None
        return self._changes.pop(0)

    def close(self) -> None:
        self.closed = True


class _FailingStream:
    def __init__(self, error: Exception) -> None:
        self._error = error
        self.closed = False

    def try_next(self) -> dict[str, object] | None:
        raise self._error

    def close(self) -> None:
        self.closed = True


class _FakeClient:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_partition_skips_batch_when_try_next_raises_unexpected_error(monkeypatch: Any) -> None:
    partition = _build_partition(
        monkeypatch,
        [],
        streams=[_FailingStream(ValueError("unexpected"))],
    )

    batch = partition.next_batch()

    assert batch == []
    partition.close()


def test_partition_skips_event_when_message_build_fails(monkeypatch: Any) -> None:
    docs = [
        {
            "_id": {"_data": "resume-bad"},
            "operationType": "insert",
            "ns": {"db": "app", "coll": "orders"},
            "documentKey": {"_id": "order-bad"},
        },
        {
            "_id": {"_data": "resume-ok"},
            "operationType": "insert",
            "ns": {"db": "app", "coll": "orders"},
            "documentKey": {"_id": "order-ok"},
        },
    ]
    call_count = 0
    original_build = __import__(
        "loom.streaming.mongo._normalize",
        fromlist=["build_mongo_cdc_message"],
    ).build_mongo_cdc_message

    def _build_once_failing(change: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise TypeError("bad document")
        return original_build(change)

    monkeypatch.setattr(
        "loom.streaming.mongo._bytewax_source.build_mongo_cdc_message",
        _build_once_failing,
    )
    partition = _build_partition(monkeypatch, docs)

    batch = partition.next_batch()

    assert len(batch) == 1
    assert batch[0].payload.event_id == "resume-ok"
    partition.close()


def test_partition_does_not_swallow_operation_failure_unexpired(monkeypatch: Any) -> None:
    from pymongo.errors import OperationFailure

    non_expiry_error = OperationFailure("some other mongo error", code=999)
    partition = _build_partition(
        monkeypatch,
        [],
        on_oplog_expired="restart_from_now",
        streams=[_FailingStream(non_expiry_error)],
    )

    with pytest.raises(OperationFailure, match="some other mongo error"):
        partition.next_batch()

    partition.close()
