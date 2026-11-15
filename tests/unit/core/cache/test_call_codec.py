"""Return-type grammar of a cached call.

``build_call_codec`` derives the codec of a ``@cache_call`` coroutine from its
declared return annotation and applies it on both paths, so a hit and a miss
answer the same value *and* the same type. The msgspec grammar
``build_result_codec`` already implements is tried first; a pydantic model,
root model, parameterised generic model or ``pydantic.dataclasses`` type — and
``list``/``tuple``/optional of those — goes through a single
``pydantic.TypeAdapter``, which is what closes consumer finding L13: a pydantic
value used to raise on write and read back as a miss.

An annotation the grammar cannot describe answers ``None``, and the caller
runs the coroutine uncached rather than storing something a hit and a miss
would disagree about. A mapping is refused explicitly, by decision.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Generic, TypedDict, TypeVar

import msgspec
import pytest
from pydantic import BaseModel, ConfigDict, RootModel
from pydantic.dataclasses import dataclass as pydantic_dataclass

from loom.core.cache.result_codec import build_call_codec

T = TypeVar("T")


class AcmeCodecStructDoc(msgspec.Struct):
    """Return type inside the msgspec grammar."""

    name: str


class AcmeCodecPydanticDoc(BaseModel):
    """Plain pydantic model, outside the msgspec grammar."""

    name: str
    score: int


def _to_camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(word.capitalize() for word in rest)


class AliasedDoc(BaseModel):
    """Model whose serialisation aliases differ from its field names.

    ``populate_by_name`` is deliberately left off: the model then validates
    only from its aliases, so a payload dumped without ``by_alias=True`` cannot
    be read back.
    """

    model_config = ConfigDict(alias_generator=_to_camel)

    document_name: str


class RootDoc(RootModel[list[str]]):
    """Root model, which a ``TypeAdapter`` handles like any other."""


class GenericDoc(BaseModel, Generic[T]):
    """Generic model, parameterised at the annotation."""

    item: T


@pydantic_dataclass
class DataclassDoc:
    """``pydantic.dataclasses`` type, also covered by one adapter."""

    name: str


class AcmeCodecLooseDoc(BaseModel):
    """Model with an imprecise field type, pinning the documented limit."""

    when: Any


async def struct_list() -> list[AcmeCodecStructDoc]: ...


async def pydantic_doc() -> AcmeCodecPydanticDoc: ...


async def aliased_doc() -> AliasedDoc: ...


async def root_doc() -> RootDoc: ...


async def generic_doc() -> GenericDoc[int]: ...


async def dataclass_doc() -> DataclassDoc: ...


async def optional_pydantic_doc() -> AcmeCodecPydanticDoc | None: ...


async def optional_pydantic_list() -> list[AcmeCodecPydanticDoc] | None: ...


async def loose_doc() -> AcmeCodecLooseDoc: ...


class MappingDoc(TypedDict):
    """A ``TypedDict`` is a mapping too, and has no origin to detect it by."""

    name: str


async def mapping_dict() -> dict[str, Any]: ...


async def mapping_abc() -> Mapping[str, int]: ...


async def mapping_bare_dict() -> dict: ...


async def mapping_bare_abc() -> Mapping: ...


async def mapping_typed_dict() -> MappingDoc: ...


async def optional_mapping() -> dict[str, Any] | None: ...


async def mapping_in_a_list() -> list[dict[str, int]]: ...


async def returns_nothing() -> None: ...


async def anything() -> Any: ...


async def unresolvable() -> Any: ...


# A genuine forward reference no module can resolve. Assigned rather than
# written literally so a deliberately dangling name does not become lint noise.
unresolvable.__annotations__["return"] = "UndeclaredDoc"


async def unannotated(): ...


def _round_trip(func: Any, result: Any) -> tuple[Any, Any, Any]:
    """Encode *result* through *func*'s codec and read it back from the payload.

    Returns:
        The value the caller gets on a miss and the value a later hit decodes,
        which must be equal for the codec to be correct.
    """
    codec = build_call_codec(func)
    assert codec is not None
    encoded = codec.encode(result)
    return encoded.value, codec.decode(encoded.payload), encoded.payload


class TestTheMsgspecGrammarIsTriedFirst:
    """A msgspec return type never reaches the pydantic branch."""

    def test_a_struct_list_uses_the_msgspec_codec(self) -> None:
        """``list[AcmeCodecStructDoc]`` is inside the grammar ``build_result_codec`` knows."""
        codec = build_call_codec(struct_list)
        assert codec is not None
        assert codec._loom_type.library == "msgspec"

    def test_a_struct_list_round_trips(self) -> None:
        """Hit and miss agree on value and type."""
        docs = [AcmeCodecStructDoc(name="a"), AcmeCodecStructDoc(name="b")]

        miss, hit, _ = _round_trip(struct_list, docs)

        assert miss == docs
        assert hit == docs


class TestPydanticReturnTypesRoundTrip:
    """One ``TypeAdapter`` covers every pydantic shape without triage."""

    @pytest.mark.parametrize(
        ("func", "result"),
        [
            (pydantic_doc, AcmeCodecPydanticDoc(name="a", score=1)),
            (root_doc, RootDoc(["a", "b"])),
            (generic_doc, GenericDoc[int](item=3)),
            (dataclass_doc, DataclassDoc(name="a")),
            (optional_pydantic_doc, AcmeCodecPydanticDoc(name="a", score=1)),
            (optional_pydantic_doc, None),
            (optional_pydantic_list, [AcmeCodecPydanticDoc(name="a", score=1)]),
            (optional_pydantic_list, None),
        ],
    )
    def test_the_value_survives_the_round_trip(self, func: Any, result: Any) -> None:
        """The value a miss returns equals the value a hit decodes."""
        miss, hit, _ = _round_trip(func, result)

        assert miss == result
        assert hit == result

    def test_a_pydantic_model_uses_the_pydantic_codec(self) -> None:
        """The branch is reached, not the msgspec one."""
        codec = build_call_codec(pydantic_doc)
        assert codec is not None
        assert codec._loom_type.library == "pydantic"

    def test_an_aliased_model_is_stored_under_its_aliases(self) -> None:
        """``by_alias=True`` is what makes the stored payload readable back."""
        doc = AliasedDoc.model_validate({"documentName": "a"})

        miss, hit, payload = _round_trip(aliased_doc, doc)

        assert payload == {"documentName": "a"}
        assert miss == doc
        assert hit == doc


class TestAnImpreciseFieldTypeChangesShapeSilently:
    """The documented limit of the write-path decode.

    ``validate_python`` is lax, so a field typed ``Any`` accepts whatever
    ``mode="json"`` produced: a ``datetime`` comes back a ``str`` on the miss
    as on the hit. Nothing raises, and the two paths agree with each other
    while disagreeing with the caller's own value. This is the reason the docs
    ask for precise field types instead of promising a total safety net.
    """

    def test_a_datetime_in_an_any_field_comes_back_a_string(self) -> None:
        """No exception, and both paths answer a ``str``."""
        doc = AcmeCodecLooseDoc(when=datetime(2026, 9, 8, tzinfo=UTC))

        miss, hit, _ = _round_trip(loose_doc, doc)

        assert isinstance(miss.when, str)
        assert isinstance(hit.when, str)
        assert miss == hit
        assert miss != doc


class TestAnnotationsOutsideTheGrammarHaveNoCodec:
    """Refused annotations answer ``None`` so the call runs uncached.

    A mapping is refused in every form: parameterised, bare, a ``TypedDict``,
    or nested inside a union or a list. The bare and ``TypedDict`` forms carry
    no ``get_origin``, and the nested ones carry the wrong one; all of them
    fall through the msgspec grammar and a ``TypeAdapter`` builds for them
    without complaint, so only a positive, recursive test keeps them out.

    A coroutine declared ``-> None`` is refused too: ``get_type_hints``
    normalises the annotation to ``NoneType``, for which a ``TypeAdapter``
    also builds, and there is nothing to cache.
    """

    @pytest.mark.parametrize(
        "func",
        [
            pytest.param(mapping_dict, id="dict[str, Any]"),
            pytest.param(mapping_abc, id="Mapping[str, int]"),
            pytest.param(mapping_bare_dict, id="bare dict"),
            pytest.param(mapping_bare_abc, id="bare Mapping"),
            pytest.param(mapping_typed_dict, id="TypedDict"),
            pytest.param(optional_mapping, id="dict[str, Any] | None"),
            pytest.param(mapping_in_a_list, id="list[dict[str, int]]"),
            pytest.param(returns_nothing, id="None"),
            pytest.param(anything, id="Any"),
            pytest.param(unresolvable, id="unresolvable forward reference"),
            pytest.param(unannotated, id="no annotation"),
        ],
    )
    def test_no_codec_is_built(self, func: Any) -> None:
        """A mapping is refused by decision; the rest cannot be described."""
        assert build_call_codec(func) is None
