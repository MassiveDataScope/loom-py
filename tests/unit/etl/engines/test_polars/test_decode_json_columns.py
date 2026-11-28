"""Public contract of loom.etl.engines.polars.decode_json_columns."""

from __future__ import annotations

import json
from pathlib import Path

import msgspec
import polars as pl
from deltalake import write_deltalake
from polars.testing import assert_frame_equal

from loom.etl import Format, FromFile, FromTable
from loom.etl.engines.polars import PolarsSourceReader, decode_json_columns

from .conftest import table_path


class AcmePayload(msgspec.Struct):
    event_type: str
    user_id: int


class Customer(msgspec.Struct, rename="camel"):
    given_name: str
    order_count: int


def _raw() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "payload": [
                json.dumps({"event_type": "click", "user_id": 7}),
                None,
                "null",
            ],
            "tags": [json.dumps(["a", "b"]), json.dumps([]), None],
        }
    )


_EVENTS = FromTable("raw.events").parse_json("payload", AcmePayload).parse_json("tags", list[str])


class TestDecodeJsonColumns:
    def test_decodes_the_columns_a_from_table_declares(self) -> None:
        decoded = decode_json_columns(_raw(), _EVENTS.json_columns)

        assert decoded.schema["payload"] == pl.Struct(
            {"event_type": pl.String, "user_id": pl.Int64}
        )
        assert decoded.schema["tags"] == pl.List(pl.String)
        assert decoded["payload"].to_list() == [{"event_type": "click", "user_id": 7}, None, None]
        assert decoded["tags"].to_list() == [["a", "b"], [], None]
        assert decoded["id"].to_list() == [1, 2, 3]

    def test_keeps_the_frame_kind(self) -> None:
        eager = decode_json_columns(_raw(), _EVENTS.json_columns)
        lazy = decode_json_columns(_raw().lazy(), _EVENTS.json_columns)

        assert isinstance(eager, pl.DataFrame)
        assert isinstance(lazy, pl.LazyFrame)
        assert_frame_equal(lazy.collect(), eager)

    def test_returns_the_frame_itself_without_json_columns(self) -> None:
        frame = _raw()
        assert decode_json_columns(frame, FromTable("raw.events").json_columns) is frame
        assert decode_json_columns(frame, iter(())) is frame

    def test_decodes_the_columns_a_from_file_declares(self) -> None:
        source = FromFile("s3://bucket/events.json", format=Format.JSON).parse_json(
            "payload", AcmePayload
        )
        decoded = decode_json_columns(_raw().select("payload"), source.json_columns)

        assert decoded["payload"].to_list()[0] == {"event_type": "click", "user_id": 7}

    def test_fills_a_camel_case_sub_entity_by_its_encode_names(self) -> None:
        frame = pl.DataFrame({"customer": [json.dumps({"givenName": "Ada", "orderCount": 3})]})
        source = FromTable("raw.orders").parse_json("customer", Customer)

        decoded = decode_json_columns(frame, source.json_columns)

        assert decoded["customer"].to_list() == [{"givenName": "Ada", "orderCount": 3}]

    def test_the_reader_decodes_the_columns_its_source_declares(self, tmp_path: Path) -> None:
        write_deltalake(str(table_path(tmp_path, _EVENTS.table_ref)), _raw(), mode="overwrite")

        read = PolarsSourceReader(tmp_path).read(_EVENTS._to_spec("events"), None).collect()

        assert_frame_equal(read, decode_json_columns(_raw(), _EVENTS.json_columns))
