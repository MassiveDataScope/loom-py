"""MongoDB CDC contract tests for streaming."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from bson import ObjectId
from bson.binary import Binary
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.int64 import Int64
from bson.timestamp import Timestamp

from loom.core.model import LoomFrozenStruct
from loom.core.routing import LogicalRef
from loom.streaming import (
    FromMongoCDC,
    MongoBsonTimestamp,
    MongoCDCEvent,
    MongoCDCNamespace,
    MongoDBRef,
    MongoObjectId,
    build_mongo_cdc_event,
    build_mongo_cdc_message,
    normalize_bson_value,
)
from loom.streaming.nodes._shape import StreamShape


class TestMongoBoundaryContracts:
    def test_public_streaming_imports_do_not_require_mongo_runtime_dependency(self) -> None:
        streaming = importlib.import_module("loom.streaming")
        nodes = importlib.import_module("loom.streaming.nodes")

        assert streaming.FromMongoCDC.__name__ == "FromMongoCDC"
        assert streaming.MongoCDCEvent.__name__ == "MongoCDCEvent"
        assert nodes.FromMongoCDC.__name__ == "FromMongoCDC"

    def test_from_mongo_cdc_holds_declared_values(self) -> None:
        source = FromMongoCDC(
            "domain_events",
            collections=("orders", "payments"),
            watch_options={"full_document": "updateLookup"},
            shape=StreamShape.BATCH,
        )

        assert source.name == "domain_events"
        assert source.collections == ("orders", "payments")
        assert source.watch_options == {"full_document": "updateLookup"}
        assert source.shape is StreamShape.BATCH

    def test_from_mongo_cdc_uses_logical_ref(self) -> None:
        source = FromMongoCDC("domain_events")

        assert source.logical_ref == LogicalRef("domain_events")

    def test_from_mongo_cdc_defaults_to_database_scope(self) -> None:
        source = FromMongoCDC("domain_events")

        assert source.collections == ()
        assert source.watch_options == {}
        assert source.shape is StreamShape.RECORD

    def test_from_mongo_cdc_unparametrized_is_instantiable(self) -> None:
        source = FromMongoCDC("events")

        assert isinstance(source, FromMongoCDC)
        assert source.name == "events"

    def test_from_mongo_cdc_parametrized_with_default_type_is_instantiable(self) -> None:
        source = FromMongoCDC[MongoCDCEvent]("events")

        assert isinstance(source, FromMongoCDC)
        assert source.name == "events"

    def test_from_mongo_cdc_generic_parametrization_does_not_raise(self) -> None:
        parametrized = FromMongoCDC[MongoCDCEvent]

        assert parametrized is not None


class TestMongoPayloadContracts:
    def test_mongo_cdc_payload_contracts_are_loom_structs(self) -> None:
        event = MongoCDCEvent(
            event_id="token-1",
            operation_type="insert",
            namespace=MongoCDCNamespace(db="app", coll="orders"),
            resume_token={"_data": "825E"},
            document_id="507f1f77bcf86cd799439011",
            cluster_time=MongoBsonTimestamp(seconds=1716400000, increment=7),
            wall_time_ms=1716400000123,
            lag_ms=321,
            full_document={"_id": "507f1f77bcf86cd799439011", "status": "open"},
            update_description=None,
            raw_json='{"operationType":"insert"}',
        )

        assert isinstance(event, LoomFrozenStruct)
        assert isinstance(event.namespace, LoomFrozenStruct)
        assert isinstance(event.cluster_time, LoomFrozenStruct)
        assert event.namespace.db == "app"
        assert event.cluster_time is not None
        assert event.cluster_time.increment == 7
        assert event.resume_token == {"_data": "825E"}
        assert event.lag_ms == 321

    def test_mongo_cdc_value_contracts_are_immutable(self) -> None:
        event = MongoCDCEvent(
            event_id="token-1",
            operation_type="insert",
            namespace=MongoCDCNamespace(db="app"),
            resume_token={"_data": "825E"},
            raw_json="{}",
        )

        with pytest.raises(AttributeError):
            cast(Any, event).event_id = "token-2"


class TestMongoNormalizationContracts:
    def test_normalize_bson_value_converts_core_scalar_types(self) -> None:
        object_id = normalize_bson_value(ObjectId("507f1f77bcf86cd799439011"))
        timestamp = normalize_bson_value(Timestamp(1716400000, 7))
        decimal = normalize_bson_value(Decimal128("428.79"))
        when = normalize_bson_value(datetime(2024, 5, 22, 12, 0, tzinfo=UTC))

        assert isinstance(object_id, MongoObjectId)
        assert object_id.id == "507f1f77bcf86cd799439011"
        assert object_id.created_at_ms is None or isinstance(object_id.created_at_ms, int)
        assert timestamp == {"seconds": 1716400000, "increment": 7}
        assert decimal == "428.79"
        assert when == 1716379200000

    def test_normalize_bson_value_converts_binary_and_dbref(self) -> None:
        binary = normalize_bson_value(Binary(b"mongo"))
        dbref = normalize_bson_value(
            DBRef("orders", ObjectId("507f1f77bcf86cd799439011"), database="app")
        )

        assert binary == "bW9uZ28="
        assert isinstance(dbref, MongoDBRef)
        assert dbref.id == "507f1f77bcf86cd799439011"
        assert dbref.collection == "orders"
        assert dbref.database == "app"

    def test_build_mongo_cdc_event_preserves_core_metadata(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from loom.streaming.mongo import _normalize as mongo_normalize

        monkeypatch.setattr(mongo_normalize, "_current_time_ms", lambda: 1716379200123)
        change = {
            "_id": {"_data": "825E"},
            "operationType": "update",
            "ns": {"db": "app", "coll": "orders"},
            "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")},
            "clusterTime": Timestamp(1716400000, 7),
            "wallTime": datetime(2024, 5, 22, 12, 0, tzinfo=UTC),
            "fullDocument": {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "amount": Decimal128("428.79"),
            },
            "updateDescription": {"updatedFields": {"status": "paid"}},
        }

        event = build_mongo_cdc_event(change)

        assert event.event_id == "825E"
        assert event.operation_type == "update"
        assert event.namespace == MongoCDCNamespace(db="app", coll="orders")
        assert isinstance(event.document_id, MongoObjectId)
        assert event.document_id.id == "507f1f77bcf86cd799439011"
        assert event.document_id.created_at_ms is None or isinstance(
            event.document_id.created_at_ms, int
        )
        assert event.cluster_time == MongoBsonTimestamp(seconds=1716400000, increment=7)
        assert event.wall_time_ms == 1716379200000
        assert event.lag_ms == 123
        assert event.resume_token == {"_data": "825E"}
        assert event.full_document is not None
        full_doc_id = event.full_document["_id"]
        assert isinstance(full_doc_id, MongoObjectId)
        assert full_doc_id.id == "507f1f77bcf86cd799439011"
        assert event.full_document["amount"] == "428.79"
        assert event.raw_json

    def test_normalize_bson_value_unwraps_int64_to_a_plain_int(self) -> None:
        value = normalize_bson_value(Int64(9007199254740993))

        assert type(value) is int
        assert value == 9007199254740993

    def test_build_mongo_cdc_event_encodes_multi_document_transaction_events(self) -> None:
        """Change events written inside a multi-document transaction carry ``lsid`` and an
        Int64 ``txnNumber``; Int64 subclasses int, which msgspec refuses to encode."""
        change = {
            "_id": {"_data": "826ABD45A4"},
            "operationType": "update",
            "ns": {"db": "app", "coll": "transactions"},
            "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")},
            "clusterTime": Timestamp(1716400000, 7),
            "wallTime": datetime(2024, 5, 22, 12, 0, tzinfo=UTC),
            "lsid": {"id": Binary(b"session-id", 4), "uid": Binary(b"user-hash")},
            "txnNumber": Int64(23),
            "updateDescription": {"updatedFields": {"counter": Int64(5)}},
        }

        event = build_mongo_cdc_event(change)

        assert event.update_description is not None
        updated = cast(dict[str, object], event.update_description["updatedFields"])
        assert type(updated["counter"]) is int
        assert '"txnNumber":23' in event.raw_json

    def test_normalize_dbref_without_database_field_sets_database_to_none(self) -> None:
        dbref = normalize_bson_value(DBRef("products", ObjectId("507f1f77bcf86cd799439011")))

        assert isinstance(dbref, MongoDBRef)
        assert dbref.collection == "products"
        assert dbref.id == "507f1f77bcf86cd799439011"
        assert dbref.database is None

    def test_normalize_dbref_with_non_objectid_identifier_converts_to_string(self) -> None:
        dbref = normalize_bson_value(DBRef("products", 42))

        assert isinstance(dbref, MongoDBRef)
        assert dbref.id == "42"

    def test_normalize_dbref_with_none_identifier_raises_type_error(self) -> None:
        # Class must be named "DBRef" so the type-name dispatch in normalize_bson_value routes it.
        class DBRef:  # noqa: N801
            collection = "products"
            database = None
            id = None

        db_ref = DBRef()
        with pytest.raises(TypeError, match="non-None 'id'"):
            normalize_bson_value(db_ref)

    def test_normalize_objectid_without_generation_time_sets_created_at_ms_to_none(self) -> None:
        # Class must be named "ObjectId" so the type-name dispatch routes it to _normalize_objectid.
        class ObjectId:  # noqa: N801
            def __str__(self) -> str:
                return "507f1f77bcf86cd799439011"

        result = normalize_bson_value(ObjectId())

        assert isinstance(result, MongoObjectId)
        assert result.id == "507f1f77bcf86cd799439011"
        assert result.created_at_ms is None

    def test_build_mongo_cdc_message_maps_neutral_message_meta_fields(self) -> None:
        message = build_mongo_cdc_message(
            {
                "_id": {"_data": "825E"},
                "operationType": "insert",
                "ns": {"db": "app", "coll": "orders"},
                "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")},
                "clusterTime": Timestamp(1716400000, 7),
            }
        )

        assert message.meta.message_id == "825E"
        assert message.meta.message_type == "loom.mongo.cdc"
        assert message.meta.key == "507f1f77bcf86cd799439011"
        assert message.meta.topic is None
        assert message.meta.partition is None
        assert message.meta.offset is None
