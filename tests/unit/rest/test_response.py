"""Unit tests for MsgspecJSONResponse."""

from __future__ import annotations

import json

import msgspec
import pydantic
import pytest

from loom.rest.fastapi.response import MsgspecJSONResponse

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _Item(msgspec.Struct):
    id: int
    name: str


class _ItemModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    name: str


class _Unsupported:
    pass


# ---------------------------------------------------------------------------
# media_type
# ---------------------------------------------------------------------------


def test_media_type_is_json() -> None:
    r = MsgspecJSONResponse(content={"key": "value"})
    assert r.media_type == "application/json"


# ---------------------------------------------------------------------------
# render — plain Python types
# ---------------------------------------------------------------------------


def test_render_dict() -> None:
    r = MsgspecJSONResponse(content={"a": 1, "b": "hello"})
    assert json.loads(bytes(r.body)) == {"a": 1, "b": "hello"}


def test_render_list() -> None:
    r = MsgspecJSONResponse(content=[1, 2, 3])
    assert json.loads(bytes(r.body)) == [1, 2, 3]


@pytest.mark.parametrize(
    "content",
    [
        pytest.param("hello", id="render_string"),
        pytest.param(42, id="render_int"),
    ],
)
def test_render_value(content: object) -> None:
    r = MsgspecJSONResponse(content=content)
    assert json.loads(bytes(r.body)) == content


def test_render_none() -> None:
    r = MsgspecJSONResponse(content=None)
    assert json.loads(bytes(r.body)) is None


# ---------------------------------------------------------------------------
# render — msgspec.Struct
# ---------------------------------------------------------------------------


def test_render_msgspec_struct() -> None:
    item = _Item(id=1, name="Widget")
    r = MsgspecJSONResponse(content=item)
    data = json.loads(bytes(r.body))
    assert data == {"id": 1, "name": "Widget"}


def test_render_list_of_structs() -> None:
    items = [_Item(id=1, name="A"), _Item(id=2, name="B")]
    r = MsgspecJSONResponse(content=items)
    data = json.loads(bytes(r.body))
    assert data == [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]


# ---------------------------------------------------------------------------
# render — pydantic.BaseModel, through the loom type's enc_hook
# ---------------------------------------------------------------------------


def test_render_basemodel() -> None:
    model = _ItemModel(id=1, name="Widget")
    r = MsgspecJSONResponse(content=model)
    data = json.loads(bytes(r.body))
    assert data == {"id": 1, "name": "Widget"}


def test_render_list_of_basemodels() -> None:
    models = [_ItemModel(id=1, name="A"), _ItemModel(id=2, name="B")]
    r = MsgspecJSONResponse(content=models)
    data = json.loads(bytes(r.body))
    assert data == [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]


def test_render_envelope_holding_a_basemodel() -> None:
    class _Envelope(msgspec.Struct):
        item: _ItemModel

    r = MsgspecJSONResponse(content=_Envelope(item=_ItemModel(id=1, name="Widget")))
    data = json.loads(bytes(r.body))
    assert data == {"item": {"id": 1, "name": "Widget"}}


def test_render_an_unsupported_type_raises_type_error() -> None:
    r = MsgspecJSONResponse(content={})
    unsupported = _Unsupported()
    with pytest.raises(TypeError):
        r.render(unsupported)


# ---------------------------------------------------------------------------
# status_code passthrough
# ---------------------------------------------------------------------------


def test_status_code_default_200() -> None:
    r = MsgspecJSONResponse(content={})
    assert r.status_code == 200


def test_status_code_201() -> None:
    r = MsgspecJSONResponse(content={}, status_code=201)
    assert r.status_code == 201


def test_status_code_404() -> None:
    r = MsgspecJSONResponse(content={"detail": "not found"}, status_code=404)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# body is bytes
# ---------------------------------------------------------------------------


def test_body_is_bytes() -> None:
    r = MsgspecJSONResponse(content={"x": 1})
    assert isinstance(r.body, bytes)
