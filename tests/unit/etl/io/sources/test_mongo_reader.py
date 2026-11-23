"""Tests for MongoSourceReader using hand-rolled fake pymongo objects.

No live MongoDB required. Filter correctness is tested in test_mongo_predicate.py.
Here we verify that the reader passes the right args to pymongo and converts
documents correctly to Polars DataFrames.
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from typing import Any

import polars as pl
import pytest
from bson import ObjectId
from bson.decimal128 import Decimal128

from loom.etl.declarative.expr import col
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.engines.polars._dtype import loom_type_to_polars
from loom.etl.io.sources._mongo import MongoSourceReader
from loom.etl.io.sources._mongo_batch import _build_schema_plan, _canonicalize_batch
from loom.etl.io.sources._mongo_bson import normalize_bson_doc
from loom.etl.schema._contract import resolve_schema
from loom.etl.schema._schema import ColumnSchema, ListType, LoomDtype, StructField, StructType

# ---------------------------------------------------------------------------
# Fake pymongo client stack
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, docs: list[dict]) -> None:
        self._docs = list(docs)
        self.filter_used: dict = {}
        self.projection_used: dict | None = None
        self.batch_size_used: int | None = None
        self._limit: int | None = None

    def limit(self, n: int) -> _FakeCursor:
        self._limit = n
        return self

    def close(self) -> None:
        # no real connection to close in the fake cursor
        pass

    def __iter__(self):
        docs = self._docs if self._limit is None else self._docs[: self._limit]
        return iter(docs)


class _FakeCollection:
    def __init__(
        self, docs: list[dict], name: str = "fake", database: object | None = None
    ) -> None:
        self._docs = docs
        self._cursor: _FakeCursor | None = None
        self.name = name
        self.database = database

    def find(
        self,
        filter: dict | None = None,
        projection: dict | None = None,
        batch_size: int = 100,
    ) -> _FakeCursor:
        cursor = _FakeCursor(self._docs)
        cursor.filter_used = filter or {}
        cursor.projection_used = projection
        cursor.batch_size_used = batch_size
        self._cursor = cursor
        return cursor


class _FakeDatabase:
    def __init__(self, collections: dict[str, list[dict]], name: str = "fakedb") -> None:
        self._cols = collections
        self.collections: dict[str, _FakeCollection] = {}
        self.name = name

    def __getitem__(self, name: str) -> _FakeCollection:
        if name not in self.collections:
            self.collections[name] = _FakeCollection(
                self._cols.get(name, []), name=name, database=self
            )
        return self.collections[name]


class _FakeClient:
    def __init__(self, dbs: dict[str, dict[str, list[dict]]]) -> None:
        self._dbs = dbs
        self.databases: dict[str, _FakeDatabase] = {}
        self.closed = False

    def __getitem__(self, db_name: str) -> _FakeDatabase:
        if db_name not in self.databases:
            self.databases[db_name] = _FakeDatabase(self._dbs.get(db_name, {}))
        return self.databases[db_name]

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Schema classes — plain Python, no msgspec
# ---------------------------------------------------------------------------


class OrderIdStatusDoc:
    order_id: str
    status: str


class ProductDoc:
    label: str


class DeepLeaf:
    refs: list[str] | None = None


class DeepNode:
    leaves: list[DeepLeaf] | None = None
    tags: list[str] | None = None


class DeepEnvelope:
    payload: DeepNode | None = None


class MetaDoc:
    name: str
    score: float


class ArchiveAsset:
    url: str
    name: str
    s3_url: str
    _id: str
    createdAt: datetime
    updatedAt: datetime


class ArchiveBundle:
    assets: list[ArchiveAsset] | None = None


class ArchiveEnvelope:
    bundle: ArchiveBundle | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ORDERS = [
    {"order_id": "o1", "status": "active"},
    {"order_id": "o2", "status": "pending"},
]

_ORDER_SCHEMA = (
    ColumnSchema("order_id", LoomDtype.UTF8),
    ColumnSchema("status", LoomDtype.UTF8),
)


def _reader(
    docs: list[dict], *, db: str = "app", collection: str = "orders"
) -> tuple[MongoSourceReader, _FakeClient]:
    client = _FakeClient({db: {collection: docs}})
    return MongoSourceReader(client, db), client


def _spec(*, collection: str = "orders", **kwargs: Any) -> MongoSourceSpec:
    return MongoSourceSpec(alias=collection, collection=collection, **kwargs)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_lazy_frame(self) -> None:
        reader, _ = _reader(_ORDERS)
        result = reader.read(_spec(), None)
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df.shape == (2, 2)
        assert set(df.columns) == {"order_id", "status"}

    def test_values_preserved(self) -> None:
        reader, _ = _reader(_ORDERS)
        df = reader.read(_spec(), None).collect()
        assert df["order_id"].to_list() == ["o1", "o2"]
        assert df["status"].to_list() == ["active", "pending"]

    def test_lazyframe_can_be_collected_twice(self) -> None:
        # Regression: cursor_iter captured in closure was exhausted on 2nd collect,
        # returning only the first inference batch instead of all documents.
        # batch_size=2 forces inference_size=2, leaving 3 docs unconsumed in cursor.
        docs = [{"id": i, "val": f"v{i}"} for i in range(5)]
        reader, _ = _reader(docs)
        lf = reader.read(_spec(batch_size=2), None)

        df1 = lf.collect()
        df2 = lf.collect()

        assert df1.shape == df2.shape, (
            f"Second collect returned {df2.shape[0]} rows, expected {df1.shape[0]}"
        )
        assert df1.sort("id").equals(df2.sort("id"))


# ---------------------------------------------------------------------------
# pymongo call args
# ---------------------------------------------------------------------------


class TestCallArgs:
    def test_no_filter_passes_empty_dict(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(filter=None), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.filter_used == {}

    def test_filter_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(filter=col("status") == "active"), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.filter_used == {"status": "active"}

    def test_projection_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(projection=("order_id", "status")), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.projection_used == {"order_id": 1, "status": 1}

    def test_no_projection_passes_none(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(projection=None), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.projection_used is None

    def test_batch_size_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(batch_size=50), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.batch_size_used == 50


# ---------------------------------------------------------------------------
# limit
# ---------------------------------------------------------------------------


class TestLimit:
    @pytest.mark.parametrize(
        ("available", "limit", "expected"),
        [
            pytest.param(10, 3, 3, id="limit_applied"),
            pytest.param(5, None, 5, id="no_limit_returns_all"),
        ],
    )
    def test_limit(self, available: int, limit: int | None, expected: int) -> None:
        docs = [{"n": i} for i in range(available)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(limit=limit), None).collect()
        assert df.shape[0] == expected


# ---------------------------------------------------------------------------
# Empty collection
# ---------------------------------------------------------------------------


class TestEmptyCollection:
    def test_empty_returns_empty_dataframe(self) -> None:
        reader, _ = _reader([])
        result = reader.read(_spec(), None)
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df.shape[0] == 0

    def test_empty_with_schema_returns_correct_columns(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(schema=_ORDER_SCHEMA), None).collect()
        assert set(df.columns) == {"order_id", "status"}
        assert df.shape[0] == 0

    def test_empty_with_recursive_schema_keeps_nested_types(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(schema=resolve_schema(DeepEnvelope)), None).collect()
        assert df.shape == (0, 1)
        assert df["payload"].dtype == pl.Struct(
            {
                "leaves": pl.List(pl.Struct({"refs": pl.List(pl.String)})),
                "tags": pl.List(pl.String),
            }
        )


# ---------------------------------------------------------------------------
# BSON type normalization
# ---------------------------------------------------------------------------


class TestBsonTypes:
    def test_objectid_normalized_to_str(self) -> None:
        oid = ObjectId("507f1f77bcf86cd799439011")
        reader, _ = _reader([{"_id": oid, "name": "foo"}])
        df = reader.read(_spec(), None).collect()
        assert df["_id"].dtype == pl.String
        assert df["_id"][0] == "507f1f77bcf86cd799439011"

    def test_decimal128_normalized_to_float(self) -> None:
        reader, _ = _reader([{"amount": Decimal128("3.14"), "n": 1}])
        df = reader.read(_spec(), None).collect()
        assert df["amount"].dtype == pl.Float64
        assert abs(df["amount"][0] - 3.14) < 1e-10

    def test_nested_objectid_in_subdoc(self) -> None:
        oid = ObjectId("507f1f77bcf86cd799439011")
        reader, _ = _reader([{"meta": {"ref_id": oid}, "n": 1}])
        df = reader.read(_spec(), None).collect()
        assert isinstance(df["meta"][0], dict)
        assert df["meta"][0]["ref_id"] == "507f1f77bcf86cd799439011"

    def test_mixed_types_no_crash(self) -> None:
        from datetime import datetime

        reader, _ = _reader(
            [
                {
                    "_id": ObjectId("507f1f77bcf86cd799439011"),
                    "amount": Decimal128("100.00"),
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "tags": ["a", "b"],
                }
            ]
        )
        df = reader.read(_spec(), None).collect()
        assert df.shape[0] == 1
        assert df["_id"].dtype == pl.String
        assert df["amount"].dtype == pl.Float64


# ---------------------------------------------------------------------------
# extra_fields_mode
# ---------------------------------------------------------------------------


class TestExtraFieldsMode:
    def test_error_mode_raises_on_extra_field(self) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        frame = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="error"), None)
        with pytest.raises((ValueError, pl.exceptions.ComputeError), match="not declared in"):
            frame.collect()

    def test_ignore_mode_drops_extra_field(self) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="ignore"), None).collect()
        assert "extra_col" not in df.columns
        assert set(df.columns) == {"order_id", "status"}

    def test_warn_mode_drops_extra_field_and_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="warn"), None).collect()
        assert "extra_col" not in df.columns
        assert any("extra_col" in msg for msg in caplog.messages)

    def test_no_extra_fields_no_error(self) -> None:
        docs = [{"order_id": "o1", "status": "active"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="error"), None).collect()
        assert df.shape == (1, 2)

    def test_capture_mode_stores_extra_in_column(self) -> None:
        import json

        docs = [{"order_id": "o1", "status": "active", "extra_col": "val"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="capture"), None).collect()
        assert "_extra" in df.columns
        assert "extra_col" not in df.columns
        extra = json.loads(df["_extra"][0])
        assert extra["extra_col"] == "val"


# ---------------------------------------------------------------------------
# Heterogeneous types within a single batch
# ---------------------------------------------------------------------------


class TestHeterogeneousTypesWithinBatch:
    def test_dict_vs_string_same_field(self) -> None:
        import json

        docs = [
            {"id": 1, "attributes": {"weight": 12, "color": "red"}},
            {"id": 2, "attributes": "see label"},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["attributes"].dtype == pl.String
        first = df.filter(pl.col("id") == 1)["attributes"][0]
        parsed = json.loads(first)
        assert parsed == {"weight": 12, "color": "red"}

    def test_int_vs_string_same_field(self, caplog: pytest.LogCaptureFixture) -> None:
        import json

        docs = [
            {"id": 1, "year": 2020},
            {"id": 2, "year": "unknown"},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(), None).collect()
        assert df["year"].dtype == pl.String
        int_row = df.filter(pl.col("id") == 1)["year"][0]
        assert json.loads(int_row) == 2020
        assert any("year" in msg for msg in caplog.messages)

    def test_list_of_strings_vs_list_of_dicts(self) -> None:
        import json

        docs = [
            {"id": 1, "tags": ["sport"]},
            {"id": 2, "tags": [{"name": "sport"}]},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["tags"].dtype == pl.String
        first = df.filter(pl.col("id") == 1)["tags"][0]
        parsed = json.loads(first)
        assert parsed == ["sport"]

    def test_null_does_not_create_conflict(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"id": 1, "attributes": {"weight": 12}},
            {"id": 2, "attributes": None},
            {"id": 3, "attributes": {"weight": 30}},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(), None).collect()
        assert df["attributes"].dtype == pl.Struct
        assert df["attributes"][1] is None
        assert not any("attributes" in msg for msg in caplog.messages)

    def test_uniform_field_not_serialized(self) -> None:
        docs = [{"id": i, "price": 5000} for i in range(4)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["price"].dtype == pl.Int64
        assert df["price"][0] == 5000

    def test_warning_emitted_on_conflict(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"id": 1, "attributes": {"weight": 12}},
            {"id": 2, "attributes": "see label"},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            reader.read(_spec(), None)
        assert any("attributes" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Heterogeneous types across batches
# ---------------------------------------------------------------------------


class TestHeterogeneousTypesAcrossBatches:
    def test_struct_in_batch1_string_in_batch2(self) -> None:
        # Cross-batch type conflicts in a single batch are detected and JSON-serialised.
        # With schemaless inference reading only the first batch, callers that need
        # cross-batch resolution must declare the field as UTF8.
        import json

        docs = [
            {"id": 1, "attributes": {"weight": 10}},
            {"id": 2, "attributes": "n/a"},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(batch_size=2), None).collect()
        assert df.shape[0] == 2
        assert df["attributes"].dtype == pl.String
        first = df.filter(pl.col("id") == 1)["attributes"][0]
        parsed = json.loads(first)
        assert parsed == {"weight": 10}

    def test_warning_emitted_on_cross_batch_conflict(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        docs = [
            {"id": 1, "attributes": {"weight": 10}},
            {"id": 2, "attributes": "n/a"},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            reader.read(_spec(batch_size=2), None).collect()
        assert any("attributes" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Schema-guided coercion of fields declared as UTF8
# ---------------------------------------------------------------------------


class TestSchemaGuidedCoercion:
    def test_struct_coerced_to_str_when_schema_declares_utf8(self) -> None:
        import json

        docs = [
            {"label": {"short": "A1", "long": "Alpha One"}},
            {"label": {"short": "B2", "long": "Beta Two"}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("label", LoomDtype.UTF8),)
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["label"].dtype == pl.String
        parsed = json.loads(df["label"][0])
        assert parsed == {"short": "A1", "long": "Alpha One"}

    def test_debug_emitted_on_schema_coercion(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"label": {"short": "A1", "long": "Alpha One"}},
            {"label": {"short": "B2", "long": "Beta Two"}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("label", LoomDtype.UTF8),)
        with caplog.at_level(logging.DEBUG):
            reader.read(_spec(schema=schema), None).collect()
        assert any("label" in msg for msg in caplog.messages)

    def test_recursive_schema_materialises_nested_empty_lists(self) -> None:
        docs = [
            {"payload": {"leaves": [{"refs": []}], "tags": []}},
            {"payload": {"leaves": [{"refs": []}], "tags": []}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=resolve_schema(DeepEnvelope)), None).collect()
        assert df["payload"].dtype == pl.Struct(
            {
                "leaves": pl.List(pl.Struct({"refs": pl.List(pl.String)})),
                "tags": pl.List(pl.String),
            }
        )
        assert "Null" not in str(df["payload"].dtype)

    def test_recursive_schema_canonicalizes_nested_struct_key_order(self) -> None:
        docs = [
            {
                "bundle": {
                    "assets": [
                        {
                            "_id": "asset-1",
                            "name": "scan",
                            "url": "https://example.invalid/a",
                            "s3_url": "s3://bucket/a",
                            "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                            "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                        }
                    ]
                }
            },
            {
                "bundle": {
                    "assets": [
                        {
                            "url": "https://example.invalid/b",
                            "name": "scan-b",
                            "s3_url": "s3://bucket/b",
                            "_id": "asset-2",
                            "createdAt": datetime(2024, 5, 2, tzinfo=UTC),
                            "updatedAt": datetime(2024, 5, 2, tzinfo=UTC),
                        }
                    ]
                }
            },
        ]
        reader, _ = _reader(docs, collection="archives")
        df = reader.read(
            _spec(collection="archives", schema=resolve_schema(ArchiveEnvelope)), None
        ).collect()
        assets = df["bundle"][0]["assets"]
        assert isinstance(assets, list)
        first = assets[0]
        assert list(first.keys()) == [
            "url",
            "name",
            "s3_url",
            "_id",
            "createdAt",
            "updatedAt",
        ]
        assert first["url"] == "https://example.invalid/a"

    def test_canonicalize_batch_reorders_nested_structs_without_ordered_dict(self) -> None:
        schema = resolve_schema(ArchiveEnvelope)
        declared = {"bundle": loom_type_to_polars(schema[0].dtype)}
        batch = [
            {
                "bundle": {
                    "assets": [
                        {
                            "_id": "asset-1",
                            "name": "scan",
                            "url": "https://example.invalid/a",
                            "s3_url": "s3://bucket/a",
                            "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                            "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                        }
                    ]
                }
            }
        ]

        _canonicalize_batch(batch, _build_schema_plan(declared))

        assets = batch[0]["bundle"]["assets"]
        assert isinstance(assets, list)
        assert list(assets[0].keys()) == [
            "url",
            "name",
            "s3_url",
            "_id",
            "createdAt",
            "updatedAt",
        ]

    def test_canonicalize_batch_sorts_extra_nested_dict_fields(self) -> None:
        docs = [
            {
                "bundle": {
                    "assets": [
                        {
                            "_id": "asset-1",
                            "name": "scan",
                            "url": "https://example.invalid/a",
                            "s3_url": "s3://bucket/a",
                            "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                            "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                        }
                    ],
                    "metadata": {"beta": 2, "alpha": 1},
                }
            },
            {
                "bundle": {
                    "assets": [
                        {
                            "url": "https://example.invalid/b",
                            "name": "scan-b",
                            "s3_url": "s3://bucket/b",
                            "_id": "asset-2",
                            "createdAt": datetime(2024, 5, 2, tzinfo=UTC),
                            "updatedAt": datetime(2024, 5, 2, tzinfo=UTC),
                        }
                    ],
                    "metadata": {"alpha": 3, "beta": 4},
                }
            },
        ]
        reader, _ = _reader(docs, collection="archives")
        df = reader.read(
            _spec(collection="archives", schema=resolve_schema(ArchiveEnvelope)), None
        ).collect()
        assert "metadata" not in df["bundle"][0]

    def test_extra_complex_fields_are_serialized_before_inference(self) -> None:
        docs = [
            {
                "bundle": {"assets": []},
                "attachments": [
                    {
                        "_id": "asset-1",
                        "name": "scan",
                        "url": "https://example.invalid/a",
                        "s3_url": "s3://bucket/a",
                        "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                        "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                    }
                ],
            },
            {
                "bundle": {"assets": []},
                "attachments": [
                    {
                        "url": "https://example.invalid/b",
                        "name": "scan-b",
                        "s3_url": "s3://bucket/b",
                        "_id": "asset-2",
                        "createdAt": datetime(2024, 5, 2, tzinfo=UTC),
                        "updatedAt": datetime(2024, 5, 2, tzinfo=UTC),
                    }
                ],
            },
        ]
        reader, _ = _reader(docs, collection="archives")
        df = reader.read(
            _spec(collection="archives", schema=resolve_schema(ArchiveEnvelope)), None
        ).collect()
        assert "attachments" not in df.columns


# ---------------------------------------------------------------------------
# Nested / deep heterogeneous types
# ---------------------------------------------------------------------------


class TestNestedHeterogeneousTypes:
    """Conflicts inside nested dicts/lists that are invisible at root level."""

    def test_list_of_dicts_with_mixed_nested_field(self) -> None:
        import json

        docs = [
            {"id": 1, "ad": {"leads": [{"origin": {"channel": "search", "paid": True}}]}},
            {"id": 2, "ad": {"leads": [{"origin": "direct"}]}},
            {"id": 3, "ad": {"leads": [{"origin": {"channel": "email", "paid": False}}]}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df.shape[0] == 3
        assert df["ad"].dtype == pl.String
        parsed = json.loads(df["ad"][0])
        assert parsed["leads"][0]["origin"]["channel"] == "search"

    def test_nested_dict_field_with_scalar_type_conflict(self) -> None:
        import json

        docs = [
            {"id": 1, "pricing": {"amount": 1500, "currency": "EUR"}},
            {"id": 2, "pricing": {"amount": 1499.99, "currency": "EUR"}},
            {"id": 3, "pricing": {"amount": "consult", "currency": "EUR"}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df.shape[0] == 3
        assert df["pricing"].dtype == pl.String
        row = json.loads(df["pricing"][2])
        assert row["amount"] == "consult"

    def test_list_of_scalars_not_serialised_when_uniform(self) -> None:
        docs = [
            {"id": 1, "tags": ["news", "featured"], "ad": {"leads": [{"origin": {"ch": "a"}}]}},
            {"id": 2, "tags": ["sale"], "ad": {"leads": [{"origin": "direct"}]}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df.shape[0] == 2
        assert df["tags"].dtype == pl.List(pl.String)
        assert df["ad"].dtype == pl.String

    def test_warning_emitted_for_nested_conflict(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"id": 1, "ad": {"leads": [{"origin": {"channel": "search"}}]}},
            {"id": 2, "ad": {"leads": [{"origin": "direct"}]}},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            reader.read(_spec(), None)
        assert any("ad" in msg for msg in caplog.messages)
        assert any("nested" in msg.lower() for msg in caplog.messages)

    def test_non_conflicting_nested_dict_stays_as_struct(self) -> None:
        docs = [
            {"id": 1, "location": {"city": "Madrid", "zip": "28001"}},
            {"id": 2, "location": {"city": "BCN", "zip": "08001"}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["location"].dtype == pl.Struct({"city": pl.String, "zip": pl.String})

    def test_int_and_float_mix_not_treated_as_conflict(self) -> None:
        """int+float in the same field is NOT a conflict — both are numeric and
        Polars resolves them as Float64 without serialisation to JSON string."""
        docs = [
            {"id": 1, "km": 50000, "price": 14999.99},
            {"id": 2, "km": 49500.5, "price": 16000},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["km"].dtype == pl.Float64
        assert df["price"].dtype == pl.Float64
        assert df["km"].to_list() == [50000.0, 49500.5]

    def test_int_float_with_declared_float64_schema_not_serialised(self) -> None:
        """int+float field declared as Float64 — no conflict serialisation, stays numeric."""
        docs = [
            {"id": 1, "km": 50000},
            {"id": 2, "km": 49500.5},
        ]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema("km", LoomDtype.FLOAT64),
        )
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["km"].dtype == pl.Float64
        assert df["km"][0] == pytest.approx(50000.0)

    def test_int_string_mix_still_serialised(self) -> None:
        """int+string IS a real conflict and must still be serialised."""
        import json

        docs = [
            {"id": 1, "status": 0},
            {"id": 2, "status": "active"},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["status"].dtype == pl.String
        # The int value ends up serialised as a JSON number string
        assert json.loads(df["status"][0]) == 0

    def test_declared_struct_with_nested_scalar_conflict_stays_struct(self) -> None:
        """A schema-declared Struct field is not serialised even when a sub-field
        has mixed types across documents.  The conflicted sub-value is coerced or
        nulled by the canonicalization step instead of flattening the whole field."""
        docs = [
            {"id": 1, "pricing": {"amount": 1500, "currency": "EUR"}},
            {"id": 2, "pricing": {"amount": 1499.99, "currency": "EUR"}},
            {"id": 3, "pricing": {"amount": "consult", "currency": "EUR"}},
        ]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema(
                "pricing",
                StructType(
                    fields=(
                        StructField("amount", LoomDtype.FLOAT64),
                        StructField("currency", LoomDtype.UTF8),
                    )
                ),
            ),
        )
        df = reader.read(_spec(schema=schema), None).collect()

        assert df["pricing"].dtype == pl.Struct({"amount": pl.Float64, "currency": pl.String})
        assert df["pricing"][0]["amount"] == pytest.approx(1500.0)
        assert df["pricing"][1]["amount"] == pytest.approx(1499.99)
        assert df["pricing"][2]["amount"] is None  # "consult" cannot parse as Float64

    def test_undeclared_struct_with_nested_conflict_still_serialises(self) -> None:
        """Without a declared schema the existing behaviour is preserved:
        a nested type conflict → JSON string."""
        docs = [
            {"id": 1, "pricing": {"amount": 1500, "currency": "EUR"}},
            {"id": 2, "pricing": {"amount": "consult", "currency": "EUR"}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["pricing"].dtype == pl.String

    def test_declared_struct_drops_extra_sub_fields_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Extra sub-fields not in the declared Struct schema are dropped and a warning
        is emitted — strict projection: only declared fields survive."""
        docs = [
            {"id": 1, "pricing": {"amount": 100.0, "currency": "EUR", "extra_fee": 5.0}},
            {"id": 2, "pricing": {"amount": 200.0, "currency": "USD"}},
        ]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema(
                "pricing",
                StructType(
                    fields=(
                        StructField("amount", LoomDtype.FLOAT64),
                        StructField("currency", LoomDtype.UTF8),
                    )
                ),
            ),
        )
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(schema=schema), None).collect()

        assert df["pricing"].dtype == pl.Struct({"amount": pl.Float64, "currency": pl.String})
        row0 = df["pricing"][0]
        assert row0["amount"] == pytest.approx(100.0)
        assert row0["currency"] == "EUR"
        assert "extra_fee" not in row0
        assert any("extra_fee" in msg for msg in caplog.messages)

    def test_declared_list_of_struct_with_nested_conflict_stays_list(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Same protection extends to List[Struct] fields."""
        docs = [
            {"id": 1, "tags": [{"name": "sale", "weight": 1}]},
            {"id": 2, "tags": [{"name": "new", "weight": "high"}]},
        ]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema(
                "tags",
                ListType(
                    inner=StructType(
                        fields=(
                            StructField("name", LoomDtype.UTF8),
                            StructField("weight", LoomDtype.INT64),
                        )
                    )
                ),
            ),
        )
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(schema=schema), None).collect()

        assert df["tags"].dtype == pl.List(pl.Struct({"name": pl.String, "weight": pl.Int64}))
        assert any("deferring" in msg.lower() for msg in caplog.messages)


# ---------------------------------------------------------------------------
# UTF8 schema fields — pre-serialization to JSON string
# ---------------------------------------------------------------------------


class TestSchemaStrFields:
    """Fields declared LoomDtype.UTF8 are pre-serialized when the value is complex."""

    def test_utf8_field_dict_becomes_json_string(self) -> None:
        import json

        docs = [
            {"id": 1, "meta": {"key": "val", "count": 3}},
            {"id": 2, "meta": {"key": "other", "count": 7}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("meta", LoomDtype.UTF8))
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["meta"].dtype == pl.String
        parsed = json.loads(df["meta"][0])
        assert parsed == {"key": "val", "count": 3}

    def test_utf8_field_preserves_plain_string(self) -> None:
        docs = [{"id": 1, "meta": "already a string"}]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("meta", LoomDtype.UTF8))
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["meta"].dtype == pl.String
        assert df["meta"][0] == "already a string"

    def test_utf8_field_null_stays_null(self) -> None:
        docs = [
            {"id": 1, "meta": None},
            {"id": 2, "meta": {"k": "v"}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("meta", LoomDtype.UTF8))
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["meta"].dtype == pl.String
        assert df["meta"][0] is None

    def test_non_utf8_field_unaffected(self) -> None:
        docs = [{"id": 1, "price": 42, "meta": {"k": "v"}}]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema("price", LoomDtype.INT64),
            ColumnSchema("meta", LoomDtype.UTF8),
        )
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["price"].dtype == pl.Int64
        assert df["price"][0] == 42

    def test_class_annotation_str_auto_pre_serialized(self) -> None:
        import json

        docs = [{"name": {"first": "Alpha", "last": "One"}, "score": 9.5}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=resolve_schema(MetaDoc)), None).collect()
        assert df["name"].dtype == pl.String
        parsed = json.loads(df["name"][0])
        assert parsed == {"first": "Alpha", "last": "One"}


# ---------------------------------------------------------------------------
# _json_default fallback for non-serializable types
# ---------------------------------------------------------------------------


class TestJsonDefaultFallback:
    """_json_default must serialise BSON types that bypass normalize_bson_doc."""

    def test_normalize_bson_doc_promotes_naive_and_aware_datetimes_to_utc(self) -> None:
        naive = datetime(2024, 1, 1, 12, 0)
        aware = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        normalized = normalize_bson_doc({"naive": naive, "aware": aware})

        assert normalized["naive"].tzinfo == UTC
        assert normalized["naive"].isoformat() == "2024-01-01T12:00:00+00:00"
        assert normalized["aware"].tzinfo == UTC
        assert normalized["aware"].isoformat() == "2024-01-01T12:00:00+00:00"

    def test_objectid_in_conflicted_field_serializes_as_hex(self) -> None:
        """An ObjectId inside a conflicted field (dict vs string) must appear as
        its 24-char hex string in the JSON output, not as '<ObjectId>'."""
        import json

        from loom.etl.io.sources._mongo_batch import MongoBatchProcessor

        oid = ObjectId("507f1f77bcf86cd799439011")
        batch = [
            {"id": 1, "ref": {"_id": oid, "name": "foo"}},  # dict — conflict with row below
            {"id": 2, "ref": "plain-string"},
        ]
        processor = MongoBatchProcessor(schema_str_fields=frozenset())
        df = processor.build_frame(batch)

        assert df["ref"].dtype == pl.String
        parsed = json.loads(df["ref"][0])
        assert parsed["_id"] == "507f1f77bcf86cd799439011"
        assert parsed["name"] == "foo"

    def test_unknown_bson_type_in_safe_dumps_uses_str(self) -> None:
        """_safe_dumps must not produce '<ClassName>' placeholders for BSON types."""
        import json

        from loom.etl.io.sources._mongo_batch import _safe_dumps

        oid = ObjectId("507f1f77bcf86cd799439011")
        result = _safe_dumps({"_id": oid})
        assert result is not None
        parsed = json.loads(result)
        assert parsed["_id"] == "507f1f77bcf86cd799439011"


# ---------------------------------------------------------------------------
# Extended JSON literal flattening
# ---------------------------------------------------------------------------


class TestExtendedJsonLiterals:
    def test_off_by_default_keeps_wrapper_dict(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc({"d": {"$date": "2026-06-19T02:00:00Z"}})
        assert doc["d"] == {"$date": "2026-06-19T02:00:00Z"}

    def test_date_string_wrapper_becomes_utc_datetime(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc({"d": {"$date": "2026-06-19T02:00:00Z"}}, extended_json=True)
        assert doc["d"] == datetime(2026, 6, 19, 2, tzinfo=UTC)

    def test_date_numberlong_wrapper(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {"d": {"$date": {"$numberLong": "1750291200000"}}}, extended_json=True
        )
        assert doc["d"] == datetime.fromtimestamp(1750291200, tz=UTC)

    def test_oid_and_number_wrappers(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {
                "o": {"$oid": "507f1f77bcf86cd799439011"},
                "p": {"$numberDecimal": "3599.5"},
                "n": {"$numberInt": "42"},
            },
            extended_json=True,
        )
        assert doc["o"] == "507f1f77bcf86cd799439011"
        assert doc["p"] == pytest.approx(3599.5)
        assert doc["n"] == 42

    def test_wrapper_nested_in_subdoc_and_list(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {"delete_data": {"txs": [{"at": {"$date": "2026-04-10T10:07:47.010Z"}}]}},
            extended_json=True,
        )
        assert doc["delete_data"]["txs"][0]["at"] == datetime(
            2026, 4, 10, 10, 7, 47, 10000, tzinfo=UTC
        )

    def test_malformed_wrapper_left_as_dict(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {"d": {"$date": "not-a-date"}, "n": {"$numberInt": "abc"}}, extended_json=True
        )
        assert doc["d"] == {"$date": "not-a-date"}
        assert doc["n"] == {"$numberInt": "abc"}

    def test_multikey_and_unknown_dollar_dicts_untouched(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {"a": {"$set": 1, "$inc": 2}, "b": {"$unknown": 1}}, extended_json=True
        )
        assert doc["a"] == {"$set": 1, "$inc": 2}
        assert doc["b"] == {"$unknown": 1}

    def test_out_of_range_epochs_degrade_per_value(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {
                "a": {"$date": 9223372036854775807},
                "b": {"$date": float("nan")},
                "c": {"$date": float("inf")},
                "d": {"$date": -1e18},
                "e": {"$date": {"$numberLong": "99999999999999999999"}},
            },
            extended_json=True,
        )
        assert doc["a"] == {"$date": 9223372036854775807}
        assert math.isnan(doc["b"]["$date"])
        assert math.isinf(doc["c"]["$date"])
        assert doc["d"]["$date"] == pytest.approx(-1e18)
        assert doc["e"] == {"$date": 99999999999999999999}

    def test_nonfinite_and_oversized_values_degrade(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {
                "f": {"$numberDouble": "1e309"},
                "g": {"$numberLong": float("inf")},
                "h": {"$date": "x" * 10_000},
            },
            extended_json=True,
        )
        assert doc["f"] == {"$numberDouble": "1e309"}
        assert math.isinf(doc["g"]["$numberLong"])
        assert doc["h"] == {"$date": "x" * 10_000}

    def test_oid_requires_canonical_hex(self) -> None:
        from loom.etl.io.sources._mongo_bson import normalize_bson_doc

        doc = normalize_bson_doc(
            {"ok": {"$oid": "507f1f77bcf86cd799439011"}, "bad": {"$oid": "'; DROP TABLE--"}},
            extended_json=True,
        )
        assert doc["ok"] == "507f1f77bcf86cd799439011"
        assert doc["bad"] == {"$oid": "'; DROP TABLE--"}

    def test_reader_flag_flattens_through_scan(self) -> None:
        docs = [
            {"order_id": "o1", "deleted": {"$date": "2026-04-10T10:07:47.010Z"}},
            {"order_id": "o2", "deleted": None},
        ]
        reader, _ = _reader(docs)
        spec = _spec()
        object.__setattr__(spec, "normalize_extended_json", True)
        df = reader.read(spec, None).collect()
        assert df["deleted"][0] == datetime(2026, 4, 10, 10, 7, 47, 10000, tzinfo=UTC)

    def test_from_mongo_builder_sets_spec_flag(self) -> None:
        from loom.etl.declarative.source import FromMongo

        assert FromMongo("t").normalize_extended_json()._to_spec("t").normalize_extended_json
        assert not FromMongo("t")._to_spec("t").normalize_extended_json


# ---------------------------------------------------------------------------
# Serialization failure degradation
# ---------------------------------------------------------------------------


_DEEPER_THAN_ANY_RECURSION_LIMIT = 200_000


class TestSafeDumpsDegradation:
    def test_circular_reference_returns_null_and_counts(self) -> None:
        from loom.etl.io.sources._mongo_batch import _safe_dumps
        from loom.etl.io.sources._serialization import _serialization_stats

        mark = _serialization_stats.failures
        circular: dict = {}
        circular["self"] = circular
        assert _safe_dumps(circular) is None
        assert _serialization_stats.failures == mark + 1
        assert "ValueError" in _serialization_stats.last_error

    def test_non_str_key_returns_null_instead_of_raising(self) -> None:
        from loom.etl.io.sources._mongo_batch import _safe_dumps
        from loom.etl.io.sources._serialization import _serialization_stats

        mark = _serialization_stats.failures
        assert _safe_dumps({("a", "b"): 1}) is None
        assert _serialization_stats.failures == mark + 1
        assert "TypeError" in _serialization_stats.last_error

    def test_raising_str_returns_null_instead_of_raising(self) -> None:
        from loom.etl.io.sources._mongo_batch import _safe_dumps
        from loom.etl.io.sources._serialization import _serialization_stats

        class Evil:
            def __str__(self) -> str:
                raise ZeroDivisionError

        mark = _serialization_stats.failures
        assert _safe_dumps({"field": Evil()}) is None
        assert _serialization_stats.failures == mark + 1

    def test_recursion_error_returns_null(self) -> None:
        from loom.etl.io.sources._mongo_batch import _safe_dumps
        from loom.etl.io.sources._serialization import _serialization_stats

        # sys.setrecursionlimit cannot force this: from Python 3.12 the C json
        # encoder is guarded by a C-level limit that ignores it, and 3.14 measures
        # the machine stack instead. 20 000 levels raised up to 3.13 but not on
        # 3.14; this depth is past the effective limit on every supported version.
        deep: list = []
        cursor = deep
        for _ in range(_DEEPER_THAN_ANY_RECURSION_LIMIT):
            cursor.append([])
            cursor = cursor[0]
        assert _safe_dumps(deep) is None
        assert "RecursionError" in _serialization_stats.last_error

    def test_serialize_conflicted_degrades_per_doc(self) -> None:
        from loom.etl.io.sources._mongo_batch import _serialize_conflicted

        good = {"payload": {"a": 1}}
        circular: dict = {}
        circular["self"] = circular
        bad = {"payload": circular}
        batch = [good, bad]
        _serialize_conflicted(batch, {"payload"})
        assert batch[0]["payload"] == '{"a": 1}'
        assert batch[1]["payload"] is None

    def test_pre_serialize_value_degrades_to_null_and_counts_once(self) -> None:
        from loom.etl.io.sources._mongo_batch import _pre_serialize_value
        from loom.etl.io.sources._serialization import _serialization_stats

        circular: dict = {}
        circular["self"] = circular
        mark = _serialization_stats.failures
        assert _pre_serialize_value([circular]) is None
        assert _serialization_stats.failures == mark + 1

    def test_scan_counts_and_warns_on_serialization_failures(self, caplog) -> None:
        import logging

        from loom.etl.io.sources._serialization import _serialization_stats

        class Evil:
            def __str__(self) -> str:
                raise ZeroDivisionError

        docs = [{"order_id": "o1", "ref": {"at": Evil()}}, {"order_id": "o2", "ref": "plain"}]
        reader, _ = _reader(docs)
        mark = _serialization_stats.failures
        with caplog.at_level(logging.WARNING, logger="loom.etl.io.sources._serialization"):
            df = reader.read(_spec(), None).collect()
        assert df.height == 2
        assert df["ref"][0] == '{"at": null}'
        assert _serialization_stats.failures > mark
        assert any("could not serialise" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Schemaless null/empty-list safeguard
# ---------------------------------------------------------------------------


class TestNullTypeSafeguard:
    """Without schema, Null and List(Null) dtypes are promoted to String / List(String)."""

    def test_all_null_field_promoted_to_string(self) -> None:
        docs = [{"id": i, "notes": None} for i in range(5)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["notes"].dtype == pl.String

    def test_all_empty_list_promoted_to_list_string(self) -> None:
        docs = [{"id": i, "tags": []} for i in range(5)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["tags"].dtype == pl.List(pl.String)

    def test_fallback_preserves_declared_struct_field_types(self) -> None:
        """When fallback fires, schema_overrides are forwarded so nested Null→declared type.

        Reproduces: declared task.user:str|None, all docs have user=None,
        _resolve_conflicts serialises 'ad' to string → pl.from_dicts fails with
        schema mismatch → fallback used → without fix, task.user ends up Null.
        """
        from loom.etl.io.sources._mongo_batch import _build_frame_fallback

        task_dtype = pl.List(
            pl.Struct({"name": pl.String, "status": pl.Boolean, "user": pl.String})
        )
        batch = [
            {"id": 1, "task": [{"name": "A", "status": False, "user": None}]},
            {"id": 2, "task": []},
            {"id": 3, "task": [{"name": "B", "status": True, "user": None}]},
        ]
        schema_overrides = {"id": pl.Int64, "task": task_dtype}

        df = _build_frame_fallback(batch, schema_overrides)

        assert df["task"].dtype == task_dtype
        assert "Null" not in str(df["task"].dtype)


# ---------------------------------------------------------------------------
# Schema drift detection (between batches)
# ---------------------------------------------------------------------------


class TestSchemaDrift:
    """MongoBatchProcessor._check_schema_drift warns when fields appear or disappear."""

    def test_new_field_in_second_batch_emits_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from loom.etl.io.sources._mongo_batch import MongoBatchProcessor

        processor = MongoBatchProcessor(schema_str_fields=frozenset())
        batch1 = [{"id": 1, "name": "a"}]
        batch2 = [{"id": 2, "name": "b", "extra": "new"}]

        processor.build_frame(batch1)
        with caplog.at_level(logging.WARNING):
            processor.build_frame(batch2)

        assert any("schema drift" in m and "extra" in m for m in caplog.messages)

    def test_dropped_field_in_second_batch_emits_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from loom.etl.io.sources._mongo_batch import MongoBatchProcessor

        processor = MongoBatchProcessor(schema_str_fields=frozenset())
        batch1 = [{"id": 1, "name": "a", "score": 10}]
        batch2 = [{"id": 2, "name": "b"}]

        processor.build_frame(batch1)
        with caplog.at_level(logging.WARNING):
            processor.build_frame(batch2)

        assert any("schema drift" in m and "score" in m for m in caplog.messages)

    def test_dropped_field_warning_persists_in_subsequent_batches(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from loom.etl.io.sources._mongo_batch import MongoBatchProcessor

        processor = MongoBatchProcessor(schema_str_fields=frozenset())
        processor.build_frame([{"id": 1, "score": 10}])
        processor.build_frame([{"id": 2}])  # score disappears

        with caplog.at_level(logging.WARNING):
            processor.build_frame([{"id": 3}])  # still no score

        assert any("schema drift" in m and "score" in m for m in caplog.messages)

    def test_stable_schema_emits_no_drift_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        from loom.etl.io.sources._mongo_batch import MongoBatchProcessor

        processor = MongoBatchProcessor(schema_str_fields=frozenset())
        with caplog.at_level(logging.WARNING):
            processor.build_frame([{"id": 1, "name": "a"}])
            processor.build_frame([{"id": 2, "name": "b"}])
            processor.build_frame([{"id": 3, "name": "c"}])

        assert not any("schema drift" in m for m in caplog.messages)


# ---------------------------------------------------------------------------
# _value_risk_notes — scalar check regression
# ---------------------------------------------------------------------------


class TestValueRiskNotes:
    """_value_risk_notes must not produce false positives for native Python types."""

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(42, id="int_for_int64_no_false_positive"),
            pytest.param("123", id="convertible_str_for_int64_no_note"),
        ],
    )
    def test_convertible_value_for_int64_has_no_note(self, value: object) -> None:
        from loom.etl.io.sources._mongo_batch import (
            _CanonicalValuePlan,
            _value_risk_notes,
        )

        plan = _CanonicalValuePlan(kind="scalar", dtype=pl.Int64)
        assert _value_risk_notes(value, plan, "$.id") == []

    def test_float_for_float64_no_false_positive(self) -> None:
        from loom.etl.io.sources._mongo_batch import (
            _CanonicalValuePlan,
            _value_risk_notes,
        )

        plan = _CanonicalValuePlan(kind="scalar", dtype=pl.Float64)
        assert _value_risk_notes(3.14, plan, "$.price") == []

    def test_unconvertible_str_for_int64_emits_note(self) -> None:
        from loom.etl.io.sources._mongo_batch import (
            _CanonicalValuePlan,
            _value_risk_notes,
        )

        plan = _CanonicalValuePlan(kind="scalar", dtype=pl.Int64)
        notes = _value_risk_notes("not-a-number", plan, "$.id")
        assert len(notes) == 1
        assert "unconvertible str" in notes[0]


class TestPhase2Improvements:
    """Regression and unit tests for Phase 2 refactoring (T10–T14)."""

    # ------------------------------------------------------------------
    # T10 — _log_risky_rows builds schema plan once per batch
    # ------------------------------------------------------------------

    def test_log_risky_rows_plan_built_once(self) -> None:
        """_build_schema_plan must be called exactly once regardless of batch size."""
        from unittest.mock import patch

        from loom.etl.io.sources._mongo_batch import _log_risky_rows

        schema: dict[str, pl.DataType] = {"x": pl.Int64}
        batch = [{"x": "not-a-number"}] * 50

        with patch(
            "loom.etl.io.sources._mongo_batch._build_schema_plan",
            wraps=__import__(
                "loom.etl.io.sources._mongo_batch", fromlist=["_build_schema_plan"]
            )._build_schema_plan,
        ) as mock_plan:
            _log_risky_rows(batch, schema)

        mock_plan.assert_called_once_with(schema)

    # ------------------------------------------------------------------
    # T11 — _classify_batch_keys detects both conflict types in one pass
    # ------------------------------------------------------------------

    def test_classify_batch_keys_returns_conflicted_and_complex_root(self) -> None:
        from loom.etl.io.sources._mongo_batch import _classify_batch_keys

        batch = [
            {"a": 1, "b": {"x": 1}},
            {"a": "string", "b": {"x": "string"}},
        ]
        conflicted, complex_root = _classify_batch_keys(batch)

        assert "a" in conflicted, "int/str mix must be conflicted"
        assert "b" in complex_root, "nested type conflict must be in complex_root"
        assert "b" not in conflicted, "root type (dict) is uniform — not conflicted at root"

    def test_classify_batch_keys_stable_schema_returns_empty_sets(self) -> None:
        from loom.etl.io.sources._mongo_batch import _classify_batch_keys

        batch = [{"x": 1, "y": "hello"}, {"x": 2, "y": "world"}]
        conflicted, complex_root = _classify_batch_keys(batch)

        assert not conflicted
        assert not complex_root

    # ------------------------------------------------------------------
    # T12 — _build_frame_fallback logs cast failures at DEBUG
    # ------------------------------------------------------------------

    def test_build_frame_fallback_logs_debug_on_cast_failure(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from unittest.mock import patch

        from loom.etl.io.sources._mongo_batch import _build_frame_fallback

        original_cast = pl.Series.cast

        def _failing_cast(self: pl.Series, dtype: pl.DataType, **kwargs: Any) -> pl.Series:
            if dtype == pl.Int64:
                raise pl.exceptions.ComputeError("forced test failure")
            return original_cast(self, dtype, **kwargs)

        batch = [{"score": 3.14}]
        with (
            patch.object(pl.Series, "cast", _failing_cast),
            caplog.at_level(logging.DEBUG, logger="loom.etl.io.sources._mongo_batch"),
        ):
            _build_frame_fallback(batch, {"score": pl.Int64})

        debug_messages = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("could not cast" in m for m in debug_messages)
        assert any("score" in m for m in debug_messages)

    # ------------------------------------------------------------------
    # T14 — BatchProcessorProtocol structural compatibility
    # ------------------------------------------------------------------

    def test_batch_processor_satisfies_protocol(self) -> None:
        from loom.etl.io.sources._mongo_batch import BatchProcessorProtocol, MongoBatchProcessor

        processor = MongoBatchProcessor(schema_str_fields=frozenset())
        assert isinstance(processor, BatchProcessorProtocol)

    def test_custom_object_satisfies_protocol_structurally(self) -> None:
        from loom.etl.io.sources._mongo_batch import BatchProcessorProtocol

        class _FakeProcessor:
            def build_frame(
                self,
                batch: list[dict[str, Any]],
                schema_overrides: dict[str, pl.DataType] | None = None,
            ) -> pl.DataFrame:
                return pl.DataFrame()

        assert isinstance(_FakeProcessor(), BatchProcessorProtocol)

    # ------------------------------------------------------------------
    # T15 — _canonicalize_batch takes plan directly (no schema_overrides)
    # ------------------------------------------------------------------

    def test_canonicalize_batch_uses_plan_directly(self) -> None:
        from loom.etl.io.sources._mongo_batch import _canonicalize_batch, _CanonicalValuePlan

        plan = {"x": _CanonicalValuePlan(kind="scalar", dtype=pl.Int64)}
        batch = [{"x": "42", "y": "untouched"}]
        _canonicalize_batch(batch, plan)

        assert batch[0]["x"] == 42
        assert batch[0]["y"] == "untouched"

    def test_canonicalize_batch_empty_plan_returns_batch_unchanged(self) -> None:
        from loom.etl.io.sources._mongo_batch import _canonicalize_batch

        batch = [{"x": "hello"}]
        original = [dict(d) for d in batch]
        _canonicalize_batch(batch, {})
        assert batch == original


# ---------------------------------------------------------------------------
# Stable batch schema — column order invariants (TDD Red phase)
# ---------------------------------------------------------------------------


class TestStableBatchSchema:
    """Column order must be stable across batches with different field presence.

    These tests are written against the *fixed* contract:
      - apply_declared_schema always returns columns in declared key order.
      - apply_declared_schema in capture mode always includes _extra (null when absent).
      - align_to_schema always returns columns in schema key order, raising if a key
        is absent from the DataFrame rather than silently dropping it.

    All five tests FAIL against the current (buggy) implementations and PASS after
    the fix is applied.
    """

    # ------------------------------------------------------------------
    # T1 — apply_declared_schema: different field presence → stable order
    # ------------------------------------------------------------------

    def test_apply_declared_schema_different_field_presence_stable_column_order(self) -> None:
        """Two sparse batches with the same declared schema must produce identical
        column order so pl.concat (vstack) succeeds."""
        from loom.etl.io.sources._mongo_batch import apply_declared_schema

        declared: dict[str, pl.DataType] = {
            "brand": pl.String(),
            "name": pl.String(),
            "info": pl.String(),
        }

        batch1 = pl.DataFrame([{"brand": "X", "name": "Y"}])
        batch2 = pl.DataFrame([{"info": "Z", "name": "B"}])

        f1 = apply_declared_schema(batch1, declared, "ignore", "test_schema")
        f2 = apply_declared_schema(batch2, declared, "ignore", "test_schema")

        expected_columns = ["brand", "name", "info"]
        assert f1.columns == expected_columns, (
            f"batch1 columns {f1.columns!r} != {expected_columns!r}"
        )
        assert f2.columns == expected_columns, (
            f"batch2 columns {f2.columns!r} != {expected_columns!r}"
        )

        # Must not raise ShapeError
        pl.concat([f1, f2], how="vertical")

    # ------------------------------------------------------------------
    # T2 — apply_declared_schema: partial declared-columns in wrong order
    #       → missing declared cols appended at end → early return with wrong order
    # ------------------------------------------------------------------

    def test_apply_declared_schema_partial_declared_wrong_order_is_reordered(self) -> None:
        """When a batch contains some declared columns in a different order than the
        declared schema, and other declared columns are absent, apply_declared_schema
        must still return columns in declared key order.

        Buggy path: missing cols are appended with with_columns(), then extra_cols is
        empty so the function returns early *without* a final select reorder.
        Result is ['name', 'info', 'brand'] instead of ['brand', 'name', 'info'].
        """
        from loom.etl.io.sources._mongo_batch import apply_declared_schema

        # Declared order: brand, name, info
        declared: dict[str, pl.DataType] = {
            "brand": pl.String(),
            "name": pl.String(),
            "info": pl.String(),
        }

        # Batch has 'name' and 'info' (both declared) but NOT 'brand'
        # After with_columns([brand_null]), df.columns = ['name', 'info', 'brand']
        # No extra_cols → early return → WRONG ORDER (bug)
        df = pl.DataFrame([{"name": "Y", "info": "Z"}])

        result = apply_declared_schema(df, declared, "ignore", "test_schema")

        expected_columns = ["brand", "name", "info"]
        assert result.columns == expected_columns, (
            f"Expected {expected_columns!r}, got {result.columns!r}"
        )
        assert result["brand"][0] is None

    # ------------------------------------------------------------------
    # T3 — apply_declared_schema capture mode: _extra always present
    # ------------------------------------------------------------------

    def test_apply_declared_schema_capture_mode_always_has_extra_column(self) -> None:
        """In capture mode both batches (one with extras, one without) must have
        an _extra column so pl.concat does not raise a ShapeError."""
        from loom.etl.io.sources._mongo_batch import apply_declared_schema

        declared: dict[str, pl.DataType] = {"name": pl.String()}

        # batch1 has an undeclared field
        batch1 = pl.DataFrame([{"name": "A", "undeclared_field": "x"}])
        # batch2 has no undeclared field
        batch2 = pl.DataFrame([{"name": "B"}])

        f1 = apply_declared_schema(batch1, declared, "capture", "test_schema")
        f2 = apply_declared_schema(batch2, declared, "capture", "test_schema")

        assert "_extra" in f1.columns, "f1 (with extra fields) must contain _extra"
        assert "_extra" in f2.columns, "f2 (no extra fields) must also contain _extra"

        # Must not raise ShapeError
        pl.concat([f1, f2], how="vertical")

    # ------------------------------------------------------------------
    # T4 — apply_declared_schema warn mode: columns must be in declared order
    # ------------------------------------------------------------------

    def test_apply_declared_schema_warn_mode_stable_column_order(self) -> None:
        """In warn mode, after dropping extra columns, the returned frame must have
        columns in declared key order — not in the order they appear in the batch.

        Buggy path: df.drop(extra_cols) does not reorder; columns remain in the
        original batch order rather than the declared schema order.
        """
        from loom.etl.io.sources._mongo_batch import apply_declared_schema

        # Declared order: brand, name, info
        declared: dict[str, pl.DataType] = {
            "brand": pl.String(),
            "name": pl.String(),
            "info": pl.String(),
        }

        # Batch has columns in wrong declared order + an extra undeclared column
        # After drop('extra'), order would be ['info', 'name', 'brand'] — wrong
        batch = pl.DataFrame([{"info": "Z", "name": "B", "brand": "X", "extra": "drop_me"}])

        result = apply_declared_schema(batch, declared, "warn", "test_schema")

        expected_columns = ["brand", "name", "info"]
        assert result.columns == expected_columns, (
            f"Expected {expected_columns!r}, got {result.columns!r}"
        )

        # Verify data integrity
        assert result["brand"][0] == "X"
        assert result["name"][0] == "B"
        assert result["info"][0] == "Z"

    # ------------------------------------------------------------------
    # T5 — apply_declared_schema: capture mode column order is stable
    #       even when batch has declared cols in wrong order + extras
    # ------------------------------------------------------------------

    def test_apply_declared_schema_capture_mode_declared_cols_in_declared_order(self) -> None:
        """In capture mode, the non-extra declared columns must appear in declared
        key order in the returned frame, even when the batch columns are in a
        different order and some declared cols are missing.

        Buggy path: df.drop(extra_cols).with_columns(extra_series) preserves the
        batch's original column order for declared cols rather than reordering to
        the declared schema key order.
        """
        from loom.etl.io.sources._mongo_batch import apply_declared_schema

        # Declared order: brand, name, info
        declared: dict[str, pl.DataType] = {
            "brand": pl.String(),
            "name": pl.String(),
            "info": pl.String(),
        }

        # Batch has 'info' and 'name' in wrong order + an extra column; 'brand' missing
        # Buggy result: missing brand appended → ['info', 'name', 'extra', 'brand']
        # after drop('extra').with_columns(extra_series):
        # declared cols are ['info', 'name', 'brand'] — wrong order
        batch = pl.DataFrame([{"info": "Z", "name": "B", "extra": "x"}])

        result = apply_declared_schema(batch, declared, "capture", "test_schema")

        # _extra must be present
        assert "_extra" in result.columns, f"_extra missing from {result.columns!r}"

        declared_cols_in_result = [c for c in result.columns if c != "_extra"]
        expected_declared_order = ["brand", "name", "info"]
        assert declared_cols_in_result == expected_declared_order, (
            f"Declared cols in wrong order: {declared_cols_in_result!r} "
            f"!= {expected_declared_order!r}"
        )


# ---------------------------------------------------------------------------
# BSON unknown-type normalization fallback (TDD Red phase)
# ---------------------------------------------------------------------------


class TestBsonNormalizationFallback:
    """_normalize() and _json_default() must handle unknown bson-origin types safely.

    Current bug: ``str(value)`` is called on BSON C/Rust extension types whose
    ``__str__`` produces ``"Object({...})"`` — a non-JSON-compatible string that
    breaks downstream ``json_decode()`` calls.

    Fixed contract:
    - bson-origin type with ``.items()``  → recursive dict (WARNING logged)
    - bson-origin type without ``.items()`` → ``None`` (WARNING logged)
    - non-bson unknown type                → ``str(value)`` preserved (unchanged)
    - ``_json_default`` for bson-origin    → delegates to ``deep_normalize_for_json``

    All five tests FAIL against the current (buggy) code and PASS after the fix.
    """

    # ------------------------------------------------------------------
    # Helper: manufacture fake bson-module types without importing pymongo
    # ------------------------------------------------------------------

    @staticmethod
    def _make_bson_type(
        class_name: str,
        items_dict: dict | None = None,
        str_repr: str = "Object({})",
    ) -> object:
        """Create a fake object whose ``__module__`` starts with ``"bson"``."""
        attrs: dict[str, Any] = {
            "__repr__": lambda self: str_repr,
            "__str__": lambda self: str_repr,
        }
        if items_dict is not None:
            # Capture by default-arg to avoid late-binding closure issues
            attrs["items"] = lambda self, _d=items_dict: _d.items()
        cls = type(class_name, (), attrs)
        cls.__module__ = "bson.fake"  # type: ignore[attr-defined]
        return cls()

    # ------------------------------------------------------------------
    # T1 — bson dict-like type returns a plain Python dict, not a string
    # ------------------------------------------------------------------

    def test_normalize_bson_dict_like_type_returns_python_dict(self) -> None:
        """A bson-origin type that exposes ``.items()`` must be recursively
        normalized to a plain Python dict — not coerced with ``str()``.

        Current bug: ``_normalize`` falls through to ``return str(value)`` which
        produces ``"Object({...})"`` — a string, not a dict.
        """
        from loom.etl.io.sources._mongo_bson import _normalize

        fake_bson = self._make_bson_type(
            "FakeBsonDoc",
            items_dict={"source": "web", "medium": "favorito"},
        )

        result = _normalize(fake_bson, 0)

        # Bug: result is "Object({})" — a string
        assert not isinstance(result, str), (
            f"_normalize returned a string {result!r} instead of a dict for a "
            "bson dict-like type — str() fallback must not fire for bson-origin types"
        )
        assert result == {"source": "web", "medium": "favorito"}

    # ------------------------------------------------------------------
    # T2 — bson scalar type (no .items()) returns None, not a string
    # ------------------------------------------------------------------

    def test_normalize_bson_scalar_type_returns_none(self) -> None:
        """A bson-origin type without ``.items()`` must be normalized to ``None``
        rather than serialised with ``str()``.

        Current bug: ``_normalize`` returns ``"MaxKey()"`` (the string).
        """
        from loom.etl.io.sources._mongo_bson import _normalize

        fake_bson_scalar = self._make_bson_type(
            "MaxKey",
            items_dict=None,
            str_repr="MaxKey()",
        )

        result = _normalize(fake_bson_scalar, 0)

        assert result is None, (
            f"_normalize returned {result!r} instead of None for a bson scalar "
            "type without .items() — str() fallback must not fire for bson-origin types"
        )

    # ------------------------------------------------------------------
    # T3 — nested bson dict-like types are fully recursed
    # ------------------------------------------------------------------

    def test_normalize_nested_bson_dict_is_fully_recursed(self) -> None:
        """When a bson dict-like type contains another bson dict-like type as a
        value, both levels must be normalized to plain Python dicts.

        Current bug: the outer ``str()`` fallback prevents any recursion — the
        result is a flat string rather than a nested dict.
        """
        from loom.etl.io.sources._mongo_bson import _normalize

        inner_bson = self._make_bson_type(
            "FakeBsonInner",
            items_dict={"nested": "value"},
        )
        outer_bson = self._make_bson_type(
            "FakeBsonOuter",
            items_dict={"field": inner_bson},
        )

        result = _normalize(outer_bson, 0)

        assert result == {"field": {"nested": "value"}}, (
            f"_normalize returned {result!r}; expected fully recursed dict "
            '{"field": {"nested": "value"}}'
        )

    # ------------------------------------------------------------------
    # T4 — result of normalizing a bson dict-like type is JSON-serializable
    # ------------------------------------------------------------------

    def test_normalize_bson_dict_value_is_json_serializable(self) -> None:
        """The normalized result of a bson dict-like type must be a plain Python
        dict that survives a ``json.dumps`` / ``json.loads`` round-trip unchanged.

        Current bug: ``_normalize`` returns the string ``"Object({...})"``; while
        technically valid JSON, it is *semantically* wrong — the assertion checks
        ``isinstance(result, dict)`` to catch this case.
        """
        import json

        from loom.etl.io.sources._mongo_bson import _normalize

        fake_bson = self._make_bson_type(
            "FakeBsonDoc",
            items_dict={"city": "Madrid", "code": "28001"},
        )

        result = _normalize(fake_bson, 0)

        # Primary assertion: must be a dict, not a string
        assert isinstance(result, dict), (
            f"_normalize returned {result!r} (type {type(result).__name__}) "
            "instead of a plain dict for a bson dict-like type"
        )

        # Secondary: round-trip through JSON must be lossless
        serialized = json.dumps(result)
        assert json.loads(serialized) == result

    # ------------------------------------------------------------------
    # T5 — _json_default for bson-origin types delegates to deep_normalize_for_json
    # ------------------------------------------------------------------

    def test_json_default_bson_origin_delegates_to_normalize(self) -> None:
        """When a bson-origin dict-like type reaches ``_json_default``, the result
        must be the normalized dict (via ``deep_normalize_for_json``), not the raw
        ``str()`` of the object.

        Current bug: ``_json_default`` calls ``str(obj)`` unconditionally for
        unrecognized types, producing ``'"Object({...})"'`` — ``json.loads`` returns
        a string, not a dict.
        """
        import json

        from loom.etl.io.sources._mongo_batch import _json_dumps

        fake_bson = self._make_bson_type(
            "FakeBsonDoc",
            items_dict={"key": "value"},
        )

        raw = _json_dumps(fake_bson)
        decoded = json.loads(raw)

        assert isinstance(decoded, dict), (
            f"_json_dumps of a bson dict-like type yielded {decoded!r} "
            "(type {type(decoded).__name__}); expected a dict — "
            "_json_default must delegate to deep_normalize_for_json for bson-origin types"
        )
        assert decoded == {"key": "value"}


# ---------------------------------------------------------------------------
# Sparse fields beyond pl.from_dicts' default sampling window (100 rows)
# ---------------------------------------------------------------------------


class TestSparseFieldBeyondDefaultSampleWindow:
    """A field present only past row 100 of a batch must not be dropped.

    Regression test for a production data-loss bug: ``pl.from_dicts`` without
    an explicit ``infer_schema_length`` samples only its default (100) rows to
    decide which columns exist. A sparse field absent from that sample was
    silently dropped for the *entire* batch, even for rows where it was
    present — with no exception and no serialization-failure counter
    incremented. Confirmed in production: 403/3833 transactions with a real
    ``delete_data`` subdocument lost it this way in a single 2000-row batch.
    """

    def _docs(self, *, total: int, sparse_field_at: int) -> list[dict[str, object]]:
        docs: list[dict[str, object]] = [{"id": i, "status": "active"} for i in range(total)]
        docs[sparse_field_at] = {
            "id": sparse_field_at,
            "status": "active",
            "delete_data": {"reason": "", "status": "CONFIRMED"},
        }
        return docs

    def test_declared_utf8_field_past_row_100_is_preserved(self) -> None:
        docs = self._docs(total=200, sparse_field_at=150)
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema("status", LoomDtype.UTF8),
            ColumnSchema("delete_data", LoomDtype.UTF8),
        )
        df = reader.read(_spec(schema=schema, batch_size=200), None).collect()
        assert "delete_data" in df.columns
        row = df.filter(pl.col("id") == 150)
        assert row["delete_data"][0] is not None
        assert "CONFIRMED" in row["delete_data"][0]

    def test_undeclared_field_past_row_100_is_preserved(self) -> None:
        docs = self._docs(total=200, sparse_field_at=150)
        reader, _ = _reader(docs)
        df = reader.read(_spec(batch_size=200), None).collect()
        assert "delete_data" in df.columns
        row = df.filter(pl.col("id") == 150)
        assert row["delete_data"][0] is not None
