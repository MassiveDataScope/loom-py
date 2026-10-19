"""Parity of the two ``LoomType`` implementations and the factory's strictness.

Every boundary carries a ``LoomType`` compiled once from an authored symbol, so
the msgspec and pydantic implementations must agree on what strict JSON means:
no ``int``/``float``/``bool`` coercion from strings, ``int`` accepted for
``float``, ISO datetime, UUID and Decimal accepted as strings, unknown keys and
malformed JSON rejected with one loom exception carrying a one-line message.
"""

from __future__ import annotations

import enum
import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

import msgspec
import pytest

from loom.core.model import (
    BoundaryValidationError,
    LoomType,
    UnsupportedBoundaryType,
    loom_type,
    loom_type_of,
    msgspec_type,
    pydantic_type,
)
from loom.core.model._loom_type import _msgspec_violation

pydantic = pytest.importorskip("pydantic")
BaseModel = pydantic.BaseModel
ConfigDict = pydantic.ConfigDict

_LAX_REASON = (
    "the type must reject unknown fields: forbid_unknown_fields=True or "
    "model_config['extra'] == 'forbid' (invariant 5)"
)


class StructInvoice(msgspec.Struct, forbid_unknown_fields=True):
    total: int
    price: float
    paid: bool
    note: str
    when: datetime
    ident: UUID
    amount: Decimal


class ModelInvoice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    price: float
    paid: bool
    note: str
    when: datetime
    ident: UUID
    amount: Decimal


class LaxStruct(msgspec.Struct):
    total: int


class LaxModel(BaseModel):
    total: int


class KwargsModel(BaseModel, extra="forbid"):
    total: int


class ReopenedModel(ModelInvoice, extra="allow"):
    pass


_PAYLOAD: dict[str, Any] = {
    "total": 3,
    "price": 1.5,
    "paid": True,
    "note": "ok",
    "when": "2026-09-15T10:00:00Z",
    "ident": "12345678-1234-5678-1234-567812345678",
    "amount": "1.50",
}


def _with(**overrides: Any) -> str:
    return json.dumps({**_PAYLOAD, **overrides})


def _root_object(schema: Any) -> dict[str, Any]:
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        root: dict[str, Any] = schema["$defs"][name]
        return root
    return dict(schema)


@pytest.fixture(params=[StructInvoice, ModelInvoice], ids=["msgspec", "pydantic"])
def invoice(request: pytest.FixtureRequest) -> LoomType:
    compiled: LoomType = loom_type(request.param)
    return compiled


class TestDecodeJson:
    """Strict JSON semantics are the same behind both libraries."""

    def test_accepts_the_payload(self, invoice: LoomType) -> None:
        value = invoice.decode_json(_with())

        assert isinstance(value, invoice.type)
        assert value.total == 3
        assert value.price == 1.5
        assert value.paid is True
        assert value.note == "ok"
        assert value.when == datetime(2026, 9, 15, 10, tzinfo=UTC)
        assert value.ident == UUID("12345678-1234-5678-1234-567812345678")
        assert value.amount == Decimal("1.50")

    def test_accepts_bytes(self, invoice: LoomType) -> None:
        assert invoice.decode_json(_with().encode()).total == 3

    @pytest.mark.parametrize(
        "field, wrong",
        [("total", "5"), ("total", 5.5), ("price", "1.5"), ("paid", "true"), ("paid", 1)],
        ids=["int-from-str", "int-from-float", "float-from-str", "bool-from-str", "bool-from-int"],
    )
    def test_rejects_scalar_coercion(self, invoice: LoomType, field: str, wrong: Any) -> None:
        body = _with(**{field: wrong})
        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(body)

        assert field in str(excinfo.value)

    def test_rejects_an_unknown_key(self, invoice: LoomType) -> None:
        body = _with(zz=1)
        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(body)

        assert "zz" in str(excinfo.value)

    def test_rejects_a_missing_field(self, invoice: LoomType) -> None:
        payload = dict(_PAYLOAD)
        del payload["note"]
        body = json.dumps(payload)

        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(body)

        assert "note" in str(excinfo.value)

    def test_rejects_malformed_json(self, invoice: LoomType) -> None:
        with pytest.raises(BoundaryValidationError):
            invoice.decode_json('{"total": ')

    def test_error_carries_the_library_error_and_a_one_line_message(
        self, invoice: LoomType
    ) -> None:
        body = _with(total="5", zz=1)
        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(body)

        error = excinfo.value
        assert error.code == "boundary_validation"
        assert error.__cause__ is not None
        assert "\n" not in error.message
        assert "input_value" not in error.message
        assert "https://" not in error.message


class TestSchema:
    """The root object forbids additional properties and carries no root title."""

    def test_root_object_forbids_unknown_fields(self, invoice: LoomType) -> None:
        root = _root_object(invoice.schema())

        assert root["type"] == "object"
        assert root["additionalProperties"] is False
        assert set(root["required"]) == set(_PAYLOAD)

    def test_pydantic_root_has_no_title(self) -> None:
        schema = loom_type(ModelInvoice).schema()

        assert "title" not in schema
        assert schema["properties"]["total"] == {"title": "Total", "type": "integer"}


class TestToBuiltins:
    """A decoded value goes back to JSON-mode builtins and decodes again."""

    def test_round_trip(self, invoice: LoomType) -> None:
        value = invoice.decode_json(_with())

        builtins = invoice.to_builtins(value)

        assert builtins["total"] == 3
        assert builtins["price"] == 1.5
        assert builtins["paid"] is True
        assert builtins["ident"] == _PAYLOAD["ident"]
        assert builtins["amount"] == "1.50"
        assert builtins["when"] == "2026-09-15T10:00:00Z"
        assert invoice.decode_json(json.dumps(builtins)) == value


class TestIdentity:
    """``type`` and ``library`` tell consumers what they carry."""

    def test_msgspec_identity(self) -> None:
        compiled = loom_type(StructInvoice)

        assert compiled.type is StructInvoice
        assert compiled.library == "msgspec"

    def test_pydantic_identity(self) -> None:
        compiled = loom_type(ModelInvoice)

        assert compiled.type is ModelInvoice
        assert compiled.library == "pydantic"


class TestStrictness:
    """The factory checks strictness once, for both libraries, with one reason."""

    def test_class_kwargs_are_strict(self) -> None:
        assert loom_type(KwargsModel).library == "pydantic"

    @pytest.mark.parametrize("symbol", [LaxStruct, LaxModel, ReopenedModel])
    def test_lax_types_are_rejected(self, symbol: type) -> None:
        with pytest.raises(UnsupportedBoundaryType) as excinfo:
            loom_type(symbol)

        assert excinfo.value.code == "boundary_type_unsupported"
        assert str(excinfo.value) == _LAX_REASON

    @pytest.mark.parametrize("symbol", [dict, object(), dict[str, Any]])
    def test_other_symbols_are_rejected(self, symbol: Any) -> None:
        with pytest.raises(UnsupportedBoundaryType) as excinfo:
            loom_type(symbol)

        assert excinfo.value.code == "boundary_type_unsupported"
        assert "msgspec.Struct or pydantic.BaseModel" in str(excinfo.value)


class StructBytes(msgspec.Struct, forbid_unknown_fields=True):
    raw: bytes


class ModelBytesBase64(BaseModel):
    model_config = ConfigDict(extra="forbid", ser_json_bytes="base64", val_json_bytes="base64")

    raw: bytes


class ModelBytesDefault(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw: bytes


class InnerStruct(msgspec.Struct, frozen=True):
    x: int


class RootStructWithInner(msgspec.Struct, forbid_unknown_fields=True):
    inner: InnerStruct


class InnerModel(BaseModel):
    x: int


class RootModelWithInner(BaseModel):
    model_config = ConfigDict(extra="forbid")

    inner: InnerModel


class TestBytesEncoding:
    """Bytes round trip through base64 when both libraries opt into it."""

    @pytest.fixture(params=[StructBytes, ModelBytesBase64], ids=["msgspec", "pydantic"])
    def bytes_type(self, request: pytest.FixtureRequest) -> LoomType:
        compiled: LoomType = loom_type(request.param)
        return compiled

    def test_to_builtins_renders_base64(self, bytes_type: LoomType) -> None:
        value = bytes_type.decode_json(json.dumps({"raw": "AP8="}))

        builtins = bytes_type.to_builtins(value)

        assert builtins["raw"] == "AP8="

    def test_decode_json_accepts_base64(self, bytes_type: LoomType) -> None:
        value = bytes_type.decode_json(json.dumps({"raw": "AP8="}))

        assert value.raw == b"\x00\xff"

    def test_pydantic_default_config_decodes_bytes_as_utf8(self) -> None:
        compiled = loom_type(ModelBytesDefault)

        value = compiled.decode_json(json.dumps({"raw": "AP8="}))

        assert value.raw == b"AP8="


class TestNestedLaxType:
    """A lax nested type inside a strict root is accepted; strictness is root-only."""

    def test_msgspec_nested_struct_without_forbid_unknown_fields_is_accepted(self) -> None:
        compiled = loom_type(RootStructWithInner)

        value = compiled.decode_json(json.dumps({"inner": {"x": 1, "zz": 2}}))

        assert value.inner.x == 1

    def test_pydantic_nested_model_without_extra_is_accepted(self) -> None:
        compiled = loom_type(RootModelWithInner)

        value = compiled.decode_json(json.dumps({"inner": {"x": 1, "zz": 2}}))

        assert value.inner.x == 1


class TestSchemaShape:
    """Both libraries expose an object schema whose root forbids extra keys."""

    def test_pydantic_root_has_the_expected_keys(self) -> None:
        schema = loom_type(RootModelWithInner).schema()

        assert "additionalProperties" in schema
        assert "properties" in schema
        assert schema["type"] == "object"
        assert "$defs" in schema


class TestMsgspecType:
    """Compiler-generated annotations are wrapped without a strictness check."""

    def test_open_dict(self) -> None:
        compiled = msgspec_type(dict[str, Any])

        assert compiled.library == "msgspec"
        assert compiled.decode_json('{"a": [1, "b"]}') == {"a": [1, "b"]}
        assert compiled.schema()["type"] == "object"

    def test_list_of_int(self) -> None:
        compiled = msgspec_type(list[int])

        assert compiled.decode_json("[1, 2]") == [1, 2]
        with pytest.raises(BoundaryValidationError):
            compiled.decode_json('["1"]')

    def test_defstruct(self) -> None:
        generated = msgspec.defstruct("Answer", [("total", int)], forbid_unknown_fields=True)
        compiled = msgspec_type(generated)

        value = compiled.decode_json('{"total": 1}')

        assert compiled.type is generated
        assert compiled.to_builtins(value) == {"total": 1}
        assert _root_object(compiled.schema())["additionalProperties"] is False

    def test_an_annotation_msgspec_rejects_raises_unsupported_boundary_type(self) -> None:
        with pytest.raises(UnsupportedBoundaryType):
            msgspec_type(3)


class TestFromBuiltins:
    """``from_builtins`` is the cache/MCP entry point: builtins in, the typed value out."""

    def test_accepts_the_same_payload_shapes_on_both_libraries(self, invoice: LoomType) -> None:
        payload = dict(_PAYLOAD)

        value = invoice.from_builtins(payload)

        assert isinstance(value, invoice.type)
        assert value.total == 3
        assert value.when.year == 2026
        assert value.ident.hex == "12345678123456781234567812345678"
        assert value.amount == Decimal("1.50")

    def test_rejects_an_unknown_key_on_both_libraries(self, invoice: LoomType) -> None:
        payload = {**_PAYLOAD, "zz": 1}

        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.from_builtins(payload)

        assert isinstance(excinfo.value, ValueError)

    def test_rejects_a_malformed_payload_on_both_libraries(self, invoice: LoomType) -> None:
        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.from_builtins({"total": "not-an-int"})

        assert isinstance(excinfo.value, ValueError)


class TestLoomTypeOfDispatch:
    """The single dispatcher, without any strictness check."""

    def test_a_struct_class_resolves_to_msgspec(self) -> None:
        compiled = loom_type_of(StructInvoice)

        assert compiled.library == "msgspec"

    def test_a_basemodel_class_resolves_to_pydantic(self) -> None:
        compiled = loom_type_of(ModelInvoice)

        assert compiled.library == "pydantic"

    def test_a_list_of_basemodel_resolves_to_pydantic(self) -> None:
        compiled = loom_type_of(list[ModelInvoice])

        assert compiled.library == "pydantic"

    def test_an_optional_basemodel_resolves_to_pydantic(self) -> None:
        compiled = loom_type_of(ModelInvoice | None)

        assert compiled.library == "pydantic"

    def test_a_list_of_int_resolves_to_msgspec(self) -> None:
        compiled = loom_type_of(list[int])

        assert compiled.library == "msgspec"

    def test_mixing_both_libraries_is_rejected(self) -> None:
        with pytest.raises(UnsupportedBoundaryType) as excinfo:
            loom_type_of(StructInvoice | ModelInvoice)

        assert excinfo.value.code == "boundary_type_unsupported"
        assert "mixes msgspec and pydantic" in str(excinfo.value)

    def test_a_non_strict_struct_is_accepted_by_loom_type_of(self) -> None:
        compiled = loom_type_of(LaxStruct)

        assert compiled.library == "msgspec"

    def test_a_non_strict_struct_is_rejected_by_loom_type(self) -> None:
        with pytest.raises(UnsupportedBoundaryType):
            loom_type(LaxStruct)

    def test_a_non_strict_model_is_accepted_by_loom_type_of(self) -> None:
        compiled = loom_type_of(LaxModel)

        assert compiled.library == "pydantic"

    def test_a_non_strict_model_is_rejected_by_loom_type(self) -> None:
        with pytest.raises(UnsupportedBoundaryType):
            loom_type(LaxModel)


class TestLoomTypeOfMemoisation:
    """The dispatcher is a pure factory, memoised by the annotation's identity."""

    def test_the_same_annotation_returns_the_same_compiled_type(self) -> None:
        assert loom_type_of(StructInvoice) is loom_type_of(StructInvoice)

    @pytest.mark.parametrize("symbol", [StructInvoice, ModelInvoice], ids=["msgspec", "pydantic"])
    def test_loom_type_of_the_same_authored_symbol_is_memoised(self, symbol: type) -> None:
        assert loom_type(symbol) is loom_type(symbol)

    def test_an_unhashable_annotated_annotation_still_resolves(self) -> None:
        annotation = Annotated[int, {"k": 1}]

        compiled = loom_type_of(annotation)

        assert compiled.library == "msgspec"


class SubModelInvoice(ModelInvoice):
    extra_field: str = "unused"


class TestPydanticToBuiltinsSubclass:
    """A subclass instance is serialised through the declared type's own fields."""

    def test_only_the_declared_fields_are_serialised(self) -> None:
        compiled = loom_type(ModelInvoice)
        instance = SubModelInvoice(**_PAYLOAD)

        builtins = compiled.to_builtins(instance)

        assert "extra_field" not in builtins
        assert set(builtins) == set(_PAYLOAD)


class TestMsgspecViolationParser:
    """D6: one parser over the msgspec message text, pinned by its five literal shapes."""

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("Expected `int`, got `str` - at `$.km`", ("km", "Expected `int`, got `str`")),
            (
                "Object missing required field `email`",
                ("email", "Object missing required field `email`"),
            ),
            (
                "Object missing required field `x` - at `$.a`",
                ("a.x", "Object missing required field `x`"),
            ),
            ("Expected `int`, got `str` - at `$[0]`", ("0", "Expected `int`, got `str`")),
            (
                "Expected `str`, got `int` - at `key` in `$.tags`",
                ("tags", "Expected `str`, got `int`"),
            ),
            (
                "Invalid enum value 'a - at b' - at `$.kind`",
                ("kind", "Invalid enum value 'a - at b'"),
            ),
            (
                "Expected `int`, got `str` - at `$.meta[...]`",
                ("meta", "Expected `int`, got `str`"),
            ),
            (
                "Object contains unknown field `zz`",
                ("zz", "Object contains unknown field `zz`"),
            ),
        ],
    )
    def test_parses_the_literal_shapes(self, message: str, expected: tuple[str, str]) -> None:
        assert _msgspec_violation(message) == expected

    def test_a_real_msgspec_failure_carries_one_violation(self) -> None:
        compiled = msgspec_type(StructInvoice)

        with pytest.raises(BoundaryValidationError) as excinfo:
            compiled.from_builtins({**_PAYLOAD, "total": "not-an-int"})

        assert excinfo.value.violations == (("total", "Expected `int`, got `str`"),)
        assert excinfo.value.violations[0][0] == "total"


class NestedModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int


class ParentModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nested: NestedModel
    other: int


class TestPydanticViolations:
    """D6: pydantic builds one violation per error, field is the dotted ``loc``."""

    def test_a_real_pydantic_failure_with_two_errors_carries_two_dotted_violations(self) -> None:
        compiled = pydantic_type(ParentModel)

        with pytest.raises(BoundaryValidationError) as excinfo:
            compiled.from_builtins({"nested": {}, "other": "not-an-int"})

        fields = {field for field, _ in excinfo.value.violations}
        assert fields == {"nested.id", "other"}
        assert len(excinfo.value.violations) == 2


_SECRET = "secret-input-value"
_SECRET_NUMBER = 424242


class Colour(enum.Enum):
    RED = "red"


class Level(enum.IntEnum):
    LOW = 1


class Circle(msgspec.Struct, forbid_unknown_fields=True, tag="circle"):
    pass


class Square(msgspec.Struct, forbid_unknown_fields=True, tag="square"):
    pass


class Draft(msgspec.Struct, forbid_unknown_fields=True, tag=1):
    pass


class Final(msgspec.Struct, forbid_unknown_fields=True, tag=2):
    pass


class Paint(msgspec.Struct, forbid_unknown_fields=True):
    colour: Colour = Colour.RED
    level: Level = Level.LOW
    finish: Literal["matte"] = "matte"
    code: Annotated[str, msgspec.Meta(pattern="^[A-Z]{3}$")] = "ABC"
    stock: dict[Colour, int] = msgspec.field(default_factory=dict)
    shape: Circle | Square = msgspec.field(default_factory=Circle)
    stage: Draft | Final = msgspec.field(default_factory=Draft)


class CircleModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["circle"] = "circle"


class SquareModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["square"] = "square"


class PaintModel(BaseModel):
    model_config = ConfigDict(extra="forbid", val_json_bytes="base64")

    colour: Colour = Colour.RED
    finish: Literal["matte"] = "matte"
    code: Annotated[str, pydantic.Field(pattern="^[A-Z]{3}$")] = "ABC"
    shape: Annotated[CircleModel | SquareModel, pydantic.Field(discriminator="type")] = (
        CircleModel()
    )
    ident: UUID = UUID(int=0)
    raw: bytes = b""


class TestErrorsDoNotEchoTheInput:
    """A violation names the field and the reason, never the rejected value."""

    @pytest.mark.parametrize(
        ("payload", "violation"),
        [
            ({"colour": _SECRET}, ("colour", "Invalid enum value")),
            ({"level": _SECRET_NUMBER}, ("level", "Invalid enum value")),
            ({"finish": _SECRET}, ("finish", "Invalid enum value")),
            ({"code": _SECRET}, ("code", "Expected `str` matching regex '^[A-Z]{3}$'")),
            ({"stock": {_SECRET: 1}}, ("stock", "Invalid enum value")),
            ({"shape": {"type": _SECRET}}, ("shape.type", "Invalid value")),
            ({"stage": {"type": _SECRET_NUMBER}}, ("stage.type", "Invalid value")),
        ],
        ids=["enum", "int-enum", "literal", "pattern", "enum-key", "str-tag", "int-tag"],
    )
    def test_msgspec_decode_json(self, payload: dict[str, Any], violation: tuple[str, str]) -> None:
        compiled = loom_type(Paint)

        raw = json.dumps(payload)

        with pytest.raises(BoundaryValidationError) as excinfo:
            compiled.decode_json(raw)

        assert excinfo.value.violations == (violation,)
        assert str(_SECRET_NUMBER) not in excinfo.value.message
        assert _SECRET not in excinfo.value.message

    def test_msgspec_from_builtins(self) -> None:
        compiled = loom_type(Paint)

        with pytest.raises(BoundaryValidationError) as excinfo:
            compiled.from_builtins({"colour": _SECRET})

        assert excinfo.value.violations == (("colour", "Invalid enum value"),)
        assert excinfo.value.message == "Invalid enum value - at `$.colour`"

    @pytest.mark.parametrize(
        ("payload", "violation"),
        [
            ({"colour": _SECRET}, ("colour", "Input should be 'red'")),
            ({"finish": _SECRET}, ("finish", "Input should be 'matte'")),
            ({"code": _SECRET}, ("code", "String should match pattern '^[A-Z]{3}$'")),
            (
                {"shape": {"type": _SECRET}},
                ("shape", "Input tag does not match any of the expected tags"),
            ),
            ({"ident": f"z{_SECRET}"}, ("ident", "Input should be a valid UUID")),
            ({"raw": f"!{_SECRET}"}, ("raw", "Data should be valid in its declared encoding")),
        ],
        ids=["enum", "literal", "pattern", "tag", "uuid", "base64"],
    )
    def test_pydantic_decode_json(
        self, payload: dict[str, Any], violation: tuple[str, str]
    ) -> None:
        compiled = loom_type(PaintModel)

        raw = json.dumps(payload)

        with pytest.raises(BoundaryValidationError) as excinfo:
            compiled.decode_json(raw)

        assert excinfo.value.violations == (violation,)
        assert excinfo.value.message == f"{violation[0]}: {violation[1]}"


class Tint(enum.StrEnum):
    RED = "red"


class Grade(enum.IntEnum):
    LOW = 1
    HIGH = 2


class StructQuery(msgspec.Struct, forbid_unknown_fields=True):
    limit: int = 50
    ratio: float = 1.0
    flag: bool = False
    level: Grade | None = None
    colour: Tint | None = None
    ident: UUID | None = None
    since: Annotated[datetime, msgspec.Meta(tz=True)] | None = None
    day: date | None = None
    at: time | None = None


class ModelQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: int = 50
    ratio: float = 1.0
    flag: bool = False
    level: Grade | None = None
    colour: Tint | None = None
    ident: UUID | None = None
    since: pydantic.AwareDatetime | None = None
    day: date | None = None
    at: time | None = None


_QUERY = {
    "limit": "50",
    "ratio": "0.5",
    "flag": "true",
    "level": "2",
    "colour": "red",
    "ident": "12345678-1234-5678-1234-567812345678",
    "since": "2026-09-15T10:00:00+02:00",
}


class TestFromQuery:
    """Query strings become typed values only when they are exactly what the field declares."""

    @pytest.fixture(params=[StructQuery, ModelQuery], ids=["msgspec", "pydantic"])
    def query(self, request: pytest.FixtureRequest) -> LoomType:
        compiled: LoomType = loom_type(request.param)
        return compiled

    def test_converts_every_string(self, query: LoomType) -> None:
        value = query.from_query(_QUERY)

        assert (
            value.limit,
            value.ratio,
            value.flag,
            value.level,
            value.colour,
            value.ident,
            value.since.utcoffset(),
        ) == (
            50,
            0.5,
            True,
            Grade.HIGH,
            Tint.RED,
            UUID("12345678-1234-5678-1234-567812345678"),
            timedelta(hours=2),
        )

    @pytest.mark.parametrize(
        "since", ["2026-09-15T10:00:00Z", "2026-09-15T10:00:00-03:00", "2026-09-15T10:00:00+05:30"]
    )
    def test_accepts_a_date_time_with_any_offset(self, query: LoomType, since: str) -> None:
        assert query.from_query({"since": since}).since.tzinfo is not None

    @pytest.mark.parametrize(
        ("field", "wrong"),
        [
            ("limit", "5.0"),
            ("limit", " 5"),
            ("limit", "5_0"),
            ("limit", "+5"),
            ("limit", "\u0663"),
            ("ratio", "\u0663.5"),
            ("limit", "007"),
            ("limit", str(2**64)),
            ("limit", str(-(2**63) - 1)),
            ("limit", "9" * 5000),
            ("ratio", ".5"),
            ("ratio", "5."),
            ("ratio", "+1.5"),
            ("ratio", "01.5"),
            ("ratio", "nan"),
            ("ratio", "1e999"),
            ("flag", "1"),
            ("flag", "True"),
            ("level", "3"),
            ("colour", "blue"),
            ("ident", "not-a-uuid"),
            ("ident", "null"),
            ("since", "2026-09-15T10:00:00"),
            ("since", "1700000000"),
            ("day", "20260915"),
            ("at", "1000"),
            ("since", "+1700000000"),
            ("since", "0017"),
            ("since", ".5"),
            ("since", "-5"),
            ("since", "null"),
        ],
    )
    def test_a_bad_value_names_its_field(self, query: LoomType, field: str, wrong: str) -> None:
        params = {**_QUERY, field: wrong}

        with pytest.raises(BoundaryValidationError) as excinfo:
            query.from_query(params)
        assert [name for name, _ in excinfo.value.violations] == [field]
        assert excinfo.value.code == "boundary_validation"
        assert "sys." not in excinfo.value.message

    def test_accepts_a_date_and_a_time(self, query: LoomType) -> None:
        value = query.from_query({"day": "2026-09-15", "at": "10:30:00"})

        assert (value.day, value.at) == (date(2026, 9, 15), time(10, 30))

    @pytest.mark.parametrize("limit", [2**64 - 1, -(2**63), 0, -7])
    def test_accepts_an_integer_in_the_64_bit_range(self, query: LoomType, limit: int) -> None:
        assert query.from_query({"limit": str(limit)}).limit == limit

    @pytest.mark.parametrize("ratio", ["0", "-0.5", "1e2", "1.5E-3"])
    def test_accepts_the_json_number_grammar(self, query: LoomType, ratio: str) -> None:
        assert query.from_query({"ratio": ratio}).ratio == float(ratio)
