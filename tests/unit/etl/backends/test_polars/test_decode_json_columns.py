"""Public contract of loom.etl.backends.polars.decode_json_columns."""

from __future__ import annotations

import json

import msgspec
import polars as pl
from polars.testing import assert_frame_equal

from loom.etl import Format, FromFile, FromTable
from loom.etl.backends.polars import PolarsSourceReader, decode_json_columns
from loom.etl.backends.polars._reader import _apply_json_decode


class Payload(msgspec.Struct):
    event_type: str
    user_id: int


def _raw() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "payload": [
                json.dumps({"event_type": "click", "user_id": 7}),
                None,
                "not json",
            ],
            "tags": [json.dumps(["a", "b"]), json.dumps([]), None],
        }
    )


_EVENTS = FromTable("raw.events").parse_json("payload", Payload).parse_json("tags", list[str])


def test_decodes_the_columns_a_from_table_declares() -> None:
    decoded = decode_json_columns(_raw(), _EVENTS.json_columns)

    assert decoded.schema["payload"] == pl.Struct({"event_type": pl.String, "user_id": pl.Int64})
    assert decoded.schema["tags"] == pl.List(pl.String)
    assert decoded["payload"].to_list() == [{"event_type": "click", "user_id": 7}, None, None]
    assert decoded["tags"].to_list() == [["a", "b"], [], None]
    assert decoded["id"].to_list() == [1, 2, 3]


def test_keeps_the_frame_kind() -> None:
    eager = decode_json_columns(_raw(), _EVENTS.json_columns)
    lazy = decode_json_columns(_raw().lazy(), _EVENTS.json_columns)

    assert isinstance(eager, pl.DataFrame)
    assert isinstance(lazy, pl.LazyFrame)
    assert_frame_equal(lazy.collect(), eager)


def test_returns_the_frame_itself_without_json_columns() -> None:
    frame = _raw()
    assert decode_json_columns(frame, FromTable("raw.events").json_columns) is frame
    assert decode_json_columns(frame, iter(())) is frame


def test_decodes_the_columns_a_from_file_declares() -> None:
    source = FromFile("s3://bucket/events.json", format=Format.JSON).parse_json("payload", Payload)
    decoded = decode_json_columns(_raw().select("payload"), source.json_columns)

    assert decoded["payload"].to_list()[0] == {"event_type": "click", "user_id": 7}


def test_normalises_a_field_that_changes_type_between_rows() -> None:
    frame = pl.DataFrame(
        {
            "payload": [
                json.dumps({"event_type": "click", "user_id": 7}),
                json.dumps({"event_type": {"name": "view"}, "user_id": 8}),
            ]
        }
    )
    decoded = decode_json_columns(frame, _EVENTS.json_columns[:1])

    assert decoded["payload"].to_list()[1] == {"event_type": '{"name": "view"}', "user_id": 8}


def test_the_reader_and_its_compatibility_helper_decode_through_it() -> None:
    lazy = _raw().lazy()
    expected = decode_json_columns(lazy, _EVENTS.json_columns).collect()

    assert_frame_equal(_apply_json_decode(lazy, _EVENTS.json_columns).collect(), expected)
    assert_frame_equal(
        PolarsSourceReader._apply_json_decode(lazy, _EVENTS.json_columns).collect(), expected
    )
