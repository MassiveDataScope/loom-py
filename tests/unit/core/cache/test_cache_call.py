"""Binding contract of ``@cache_call``: the key, the wrapper and ``bind``.

The decorator declares and the composition root binds, so everything observable
about a cached call lives here: the key is the arguments and nothing else, a
hit and a miss answer the same shape, concurrent callers share one load, a
predicate can refuse to store an answer, and a cache failure never fails a call
that already produced its result.

The suites that observe TTLs and write failures drive a ``CountingCacheBackend``
or a double that raises, because ``_ConfiguredCalls`` takes a ``CacheBackend``
rather than a gateway; the round-trip suites drive the real ``CacheGateway``
over a serialized memory alias, which is what a deployment gets.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import hashlib
import logging
import random
import socket
from collections.abc import Iterator
from contextvars import ContextVar
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, NamedTuple
from uuid import UUID

import attrs
import msgspec
import pytest
from pydantic import BaseModel

from loom.core.cache import CacheConfig, CachedCalls, CacheGateway, CacheWriteError, cached_calls
from loom.core.cache._single_flight import SingleFlight
from loom.core.cache.calls import _ConfiguredCalls, _render, _UnconfiguredCalls
from loom.core.cache.decorators import cache_call
from loom.core.cache.result_codec import EncodedResult
from loom.core.di.container import LoomContainer
from loom.core.model.redaction import RedactedMap

from ._doubles import SERIALIZED_BACKEND, CountingCacheBackend

TTL = 100


class Doc(msgspec.Struct):
    """Return element inside the msgspec grammar."""

    title: str


class PydanticDoc(BaseModel):
    """Return type that only the pydantic branch can describe."""

    title: str


class OldDoc(msgspec.Struct):
    """Shape a previous deployment wrote under the key."""

    title: str


class NewDoc(msgspec.Struct):
    """Same read after the model gained a required field."""

    title: str
    author: str


class OldPydanticDoc(BaseModel):
    """Pydantic shape a previous deployment wrote under the key."""

    title: str


class NewPydanticDoc(BaseModel):
    """Same read after the pydantic model gained a required field."""

    title: str
    author: str


class LooseDoc(BaseModel):
    """Model whose ``Any`` field the JSON-mode dump changes shape of."""

    stale: bool
    moment: Any


@cache_call(version=3)
async def known_call(query: str, limit: int = 10) -> Doc:
    """Call whose key the suite asserts as a literal string."""
    return Doc(title=f"{query}:{limit}")


def _config(**overrides: Any) -> CacheConfig:
    """Cache configuration with an explicit TTL and no jitter by default."""
    defaults: dict[str, Any] = {"default_ttl": TTL, "ttl_jitter": 0.0}
    return CacheConfig(**{**defaults, **overrides})


def _calls(
    backend: Any,
    *,
    config: CacheConfig | None = None,
    rng: random.Random | None = None,
) -> _ConfiguredCalls:
    """Build the configured implementation over *backend*."""
    return _ConfiguredCalls(
        config or _config(),
        backend,
        SingleFlight(),
        rng or random.Random(),
    )


def _gateway(alias: str) -> CacheGateway:
    """Open a serialized memory alias, as a deployment's data gateway is."""
    CacheGateway.apply_config(
        CacheConfig(aiocache_alias=alias, aiocache_config=RedactedMap({alias: SERIALIZED_BACKEND}))
    )
    return CacheGateway(alias=alias)


def _warnings(caplog: pytest.LogCaptureFixture, event: str) -> list[logging.LogRecord]:
    """Warning records carrying *event*."""
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING and event in record.getMessage()
    ]


def _expected_key(func: Any, **arguments: Any) -> str:
    """Key a call of *func* with its string *arguments* is stored under.

    The rendering is written out here rather than taken from the renderer, so
    the digest still pins the format: every visited value is the two-element
    list ``[qualified type name, rendering]``, and a mapping is a list of pairs
    ordered by the encoding of the whole pair.
    """
    pairs = sorted(
        (
            [["builtins.str", name], ["builtins.str", value]]
            for name, value in (arguments or {"query": "q"}).items()
        ),
        key=msgspec.json.encode,
    )
    digest = hashlib.sha256(msgspec.json.encode(["builtins.dict", pairs])).hexdigest()
    return f"call:{func.__module__}.{func.__qualname__}:v1:{digest}"


@pytest.fixture(autouse=True)
def _capture_call_warnings(caplog: pytest.LogCaptureFixture) -> None:
    """Every warning this module asserts on comes from the calls module."""
    caplog.set_level(logging.WARNING, logger="loom.core.cache.calls")


@pytest.fixture
def _restore_global_random() -> Iterator[None]:
    """Give the process-wide generator back the state the test found it in."""
    state = random.getstate()
    try:
        yield
    finally:
        random.setstate(state)


class TestWrappingIsIdempotent:
    """AC1: a wrapper is never wrapped again, so a call writes once."""

    async def test_wrapping_a_wrapper_returns_the_very_same_object(self) -> None:
        """``functools.wraps`` copies the policy, so the marker cannot tell them apart."""

        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        calls = _calls(CountingCacheBackend())
        wrapped = calls.wrap(fetch)

        assert calls.wrap(wrapped) is wrapped

    async def test_a_call_through_a_rewrapped_function_writes_once(self) -> None:
        """A double wrap would store the same key twice, once per layer."""

        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        backend = CountingCacheBackend()
        calls = _calls(backend)
        rewrapped = calls.wrap(calls.wrap(fetch))

        await rewrapped("q")

        assert len(backend.set_ttls) == 1

    async def test_an_unmarked_coroutine_is_returned_unchanged(self) -> None:
        """Only a declared call is bound; everything else passes through."""

        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        assert _calls(CountingCacheBackend()).wrap(fetch) is fetch

    async def test_an_uncacheable_return_type_warns_once_and_returns_the_function(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A mapping return has no codec, so the coroutine runs uncached."""

        @cache_call()
        async def fetch(query: str) -> dict[str, Any]:
            return {"title": query}

        calls = _calls(CountingCacheBackend())

        assert calls.wrap(fetch) is fetch
        calls.wrap(fetch)

        assert len(_warnings(caplog, "CacheCallNotCacheable")) == 1


class TestTheKeyIsTheArguments:
    """AC2: equal arguments, however they were written, share one entry."""

    async def test_equal_arguments_run_the_body_once(self) -> None:
        runs = 0

        @cache_call()
        async def fetch(query: str) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=query)

        wrapped = _calls(_gateway("calls_equal_args")).wrap(fetch)

        first = await wrapped("q")
        second = await wrapped("q")

        assert runs == 1
        assert first == second == Doc(title="q")

    async def test_different_arguments_run_the_body_twice(self) -> None:
        runs = 0

        @cache_call()
        async def fetch(query: str) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=query)

        wrapped = _calls(_gateway("calls_other_args")).wrap(fetch)

        await wrapped("a")
        await wrapped("b")

        assert runs == 2

    async def test_an_omitted_default_and_an_explicit_one_share_the_key(self) -> None:
        """``apply_defaults`` binds the default, so the two calls agree."""
        runs = 0

        @cache_call()
        async def fetch(query: str, limit: int = 10) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=f"{query}:{limit}")

        wrapped = _calls(_gateway("calls_defaults")).wrap(fetch)

        await wrapped("q")
        await wrapped("q", limit=10)

        assert runs == 1

    async def test_a_positional_and_a_keyword_argument_share_the_key(self) -> None:
        """Binding normalises both spellings onto one parameter."""
        runs = 0

        @cache_call()
        async def fetch(query: str) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=query)

        wrapped = _calls(_gateway("calls_positional")).wrap(fetch)

        await wrapped("q")
        await wrapped(query="q")

        assert runs == 1

    async def test_keyword_arguments_in_a_different_order_share_the_key(self) -> None:
        """The ``**kwargs`` mapping is sorted like every other mapping."""
        runs = 0

        @cache_call()
        async def fetch(**criteria: int) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=str(sorted(criteria.items())))

        wrapped = _calls(_gateway("calls_kwargs_order")).wrap(fetch)

        await wrapped(a=1, b=2)
        await wrapped(b=2, a=1)

        assert runs == 1

    async def test_a_frozenset_argument_is_cacheable(self) -> None:
        """A set has a rendering at all, so a call taking one is not skipped."""
        runs = 0

        @cache_call()
        async def fetch(tags: frozenset[str]) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=str(sorted(tags)))

        wrapped = _calls(_gateway("calls_frozenset")).wrap(fetch)

        await wrapped(frozenset({"a", "b"}))
        await wrapped(frozenset({"b", "a"}))

        assert runs == 1

    def test_a_set_renders_sorted_rather_than_in_its_iteration_order(self) -> None:
        """``{1, 8}`` iterates ``8, 1``: the rendering must not inherit that.

        Two equal sets iterate identically, so a behavioural test cannot tell a
        sorted rendering from the raw one. The rendering itself can.
        """
        assert list(frozenset({1, 8})) == [8, 1]

        assert _render({"tags": frozenset({1, 8})}) == [
            "builtins.dict",
            [
                [
                    ["builtins.str", "tags"],
                    ["builtins.frozenset", [["builtins.int", 1], ["builtins.int", 8]]],
                ]
            ],
        ]


class Colour(enum.Enum):
    """Enum member whose value is exactly the string it renders to."""

    RED = "red"


class Shade(enum.StrEnum):
    """``isinstance(Shade.RED, str)`` is ``True``, so no ``isinstance`` rule can tag it."""

    RED = "red"


class Rank(enum.IntEnum):
    """The same trap on the integer side."""

    ONE = 1


class Query(msgspec.Struct):
    """Struct whose one field holds whatever the suite nests inside it."""

    when: Any


@dataclasses.dataclass(frozen=True)
class Window:
    """Stdlib dataclass, which the walk descends through its fields."""

    start: Any


class Endpoints(NamedTuple):
    """A ``tuple`` subclass, so it renders under its own concrete type."""

    left: str
    right: str


@attrs.define
class Sensor:
    """An ``attrs`` class, which neither structural branch of the walk knows.

    :func:`msgspec.to_builtins` expands it into a mapping, so it is the one
    shape that reaches the recursive branch of ``_render_leaf``.
    """

    name: str
    reading: Any


class IdentityKey(msgspec.Struct, eq=False):
    """Mapping key equal only to itself, though it renders like its twin.

    Two of them live side by side in one mapping and render identically, which
    is the only way to observe that pairs are ordered by the **whole** pair:
    ordering by the encoded key alone would leave two such pairs to
    ``list.sort``'s stability, and therefore to insertion order.
    """

    name: str


_ERASED_SCALARS: list[tuple[Any, Any]] = [
    (datetime(2020, 1, 1), "2020-01-01T00:00:00"),
    (date(2020, 1, 1), "2020-01-01"),
    (UUID(int=1), "00000000-0000-0000-0000-000000000001"),
    (Decimal("1"), "1"),
    (b"a", "YQ=="),
    (bytearray(b"a"), b"a"),
    (memoryview(b"a"), b"a"),
    (Colour.RED, "red"),
    (Shade.RED, "red"),
    (Rank.ONE, 1),
]
"""Each shape beside the scalar msgspec renders it to, which used to be one key."""

_ERASED_SCALAR_IDS = [
    "datetime",
    "date",
    "uuid",
    "decimal",
    "bytes",
    "bytearray",
    "memoryview",
    "enum",
    "str-enum",
    "int-enum",
]

_NESTED_ARGUMENTS: list[tuple[Any, Any]] = [
    (Query(when=datetime(2020, 1, 1)), Query(when="2020-01-01T00:00:00")),
    (Window(start=datetime(2020, 1, 1)), Window(start="2020-01-01T00:00:00")),
    ([datetime(2020, 1, 1)], ["2020-01-01T00:00:00"]),
    ({"when": datetime(2020, 1, 1)}, {"when": "2020-01-01T00:00:00"}),
    ({datetime(2020, 1, 1)}, {"2020-01-01T00:00:00"}),
    (Query(when=Colour.RED), Query(when="red")),
    (Query(when={"a"}), Query(when=["a"])),
]
"""A typed value and its erased twin, each nested one level down."""

_NESTED_ARGUMENT_IDS = [
    "datetime-in-a-struct",
    "datetime-in-a-dataclass",
    "datetime-in-a-list",
    "datetime-in-a-mapping-value",
    "datetime-in-a-set",
    "enum-in-a-struct",
    "set-in-a-struct",
]


async def _runs_and_entries(*arguments: Any) -> tuple[int, int]:
    """Call one wrapped coroutine once per argument, in order.

    Args:
        *arguments: The single argument of each successive call.

    Returns:
        How many times the body ran, and how many entries the backend holds.
    """
    runs = 0

    @cache_call()
    async def fetch(argument: Any) -> Doc:
        nonlocal runs
        runs += 1
        return Doc(title="fetched")

    backend = CountingCacheBackend()
    wrapped = _calls(backend).wrap(fetch)
    for argument in arguments:
        await wrapped(argument)
    return runs, len(backend.data)


async def _assert_separate_entries(first: Any, second: Any) -> None:
    """Assert the two arguments are two entries, and that each one is cached.

    The second half is the positive control, and it is what makes the first
    half evidence: a renderer that refused every argument would run the body
    twice for any pair at all, with nothing cached anywhere.
    """
    assert await _runs_and_entries(first, second) == (2, 2)
    assert await _runs_and_entries(first, first) == (1, 1)
    assert await _runs_and_entries(second, second) == (1, 1)


class TestAValueDoesNotShareTheKeyOfWhatItRendersTo:
    """AC1: the type of an argument separates entries, not only its content."""

    def test_every_shape_renders_to_a_different_value(self) -> None:
        """Ten types whose renderings used to collapse onto four scalars."""
        encodings = {msgspec.json.encode(_render(typed)) for typed, _ in _ERASED_SCALARS}

        assert len(encodings) == len(_ERASED_SCALARS)

    @pytest.mark.parametrize(("typed", "erased"), _ERASED_SCALARS, ids=_ERASED_SCALAR_IDS)
    def test_the_rendering_differs_from_the_scalar_it_used_to_erase_to(
        self, typed: Any, erased: Any
    ) -> None:
        assert _render(typed) != _render(erased)

    @pytest.mark.parametrize(("typed", "erased"), _ERASED_SCALARS, ids=_ERASED_SCALAR_IDS)
    async def test_the_two_are_two_entries_and_each_one_is_still_cached(
        self, typed: Any, erased: Any
    ) -> None:
        await _assert_separate_entries(typed, erased)


class TestANestedValueKeepsItsType:
    """AC2: the walk reaches a value before msgspec can flatten it.

    This is the case a leaf-only fix would have left broken, and the one
    ``builtin_types=`` cannot reach: a struct handed whole to
    :func:`msgspec.to_builtins` comes back with its ``datetime`` already a
    string, and its ``Enum`` and ``set`` already erased.
    """

    @pytest.mark.parametrize(("typed", "erased"), _NESTED_ARGUMENTS, ids=_NESTED_ARGUMENT_IDS)
    def test_the_renderings_differ(self, typed: Any, erased: Any) -> None:
        assert _render(typed) != _render(erased)

    @pytest.mark.parametrize(("typed", "erased"), _NESTED_ARGUMENTS, ids=_NESTED_ARGUMENT_IDS)
    async def test_the_two_are_two_entries_and_each_one_is_still_cached(
        self, typed: Any, erased: Any
    ) -> None:
        await _assert_separate_entries(typed, erased)


class TestTheShapeOfAContainerIsPartOfTheKey:
    """AC3: a list, a tuple, a set and a frozenset are four arguments."""

    def test_four_containers_with_the_same_members_render_four_ways(self) -> None:
        containers: list[Any] = [["a", "b"], ("a", "b"), {"a", "b"}, frozenset({"a", "b"})]

        encodings = {msgspec.json.encode(_render(container)) for container in containers}

        assert len(encodings) == len(containers)

    def test_a_named_tuple_renders_under_its_own_concrete_type(self) -> None:
        """It is a ``tuple``, so the tuple branch catches it and labels it as itself."""
        assert _render(Endpoints("a", "b")) != _render(("a", "b"))

    def test_a_struct_and_a_mapping_with_the_same_fields_differ(self) -> None:
        assert _render(Query(when="x")) != _render({"when": "x"})

    async def test_a_list_and_a_tuple_are_two_entries_and_each_one_is_still_cached(self) -> None:
        await _assert_separate_entries(["a", "b"], ("a", "b"))


class TestAMappingKeepsEveryPair:
    """AC4: two keys that render alike stay two pairs instead of collapsing."""

    def test_two_keys_that_used_to_collapse_stay_two_pairs(self) -> None:
        """The old rendering was ``{"red": 2}``: one pair short of the argument."""
        rendered = _render({Colour.RED: 1, "red": 2})

        assert len(rendered[1]) == 2

    def test_the_mapping_differs_from_the_one_it_used_to_render_as(self) -> None:
        assert _render({Colour.RED: 1, "red": 2}) != _render({"red": 2})

    async def test_the_two_mappings_are_two_entries_and_each_one_is_still_cached(self) -> None:
        await _assert_separate_entries({Colour.RED: 1, "red": 2}, {"red": 2})

    def test_two_keys_with_identical_renderings_are_still_two_pairs(self) -> None:
        """A ``dict`` built from the pairs would keep one of them and lose the other.

        The type label separates most keys on its own; two keys of one type
        that render alike are what the pair list itself is for.
        """
        first, second = IdentityKey("a"), IdentityKey("a")

        assert len(_render({first: 1, second: 2})[1]) == 2


class TestTheTypeLabelCannotBeForged:
    """AC5: a caller's own list is labelled too, so it cannot pose as a label."""

    def test_the_literal_pair_the_renderer_emits_is_itself_labelled_a_list(self) -> None:
        forged = _render(["datetime.datetime", "2020-01-01T00:00:00"])

        assert forged[0] == "builtins.list"

    async def test_the_forged_pair_and_the_datetime_are_two_entries(self) -> None:
        await _assert_separate_entries(
            datetime(2020, 1, 1), ["datetime.datetime", "2020-01-01T00:00:00"]
        )


class TestAnObjectMsgspecExpandsIsWalkedOnce:
    """FR-093: what ``to_builtins`` turns into a container is rendered in turn.

    An ``attrs`` class is the shape that gets here: a mapping, a set, a list, a
    tuple, a ``msgspec.Struct`` and a stdlib dataclass are all dispatched
    before the leaf, and a ``NamedTuple`` is a ``tuple``.
    """

    def test_the_expansion_is_discriminated_like_any_other_mapping(self) -> None:
        """Without the recursive branch the expansion stays a bare ``dict``."""
        rendered = _render(Sensor(name="a", reading=1))

        assert rendered[0].endswith(".Sensor")
        assert rendered[1] == _render({"name": "a", "reading": 1})

    def test_what_the_expansion_erased_stays_erased(self) -> None:
        """FR-093's stated limitation: msgspec flattens the fields before the walk sees them.

        The walk descends a struct and a dataclass itself, so it reaches their
        fields untouched; an ``attrs`` class it only sees expanded, with its
        ``datetime`` already a string. That is one key for two arguments, and
        it is the documented residual case rather than a defect.
        """
        assert _render(Sensor(name="a", reading=datetime(2020, 1, 1))) == _render(
            Sensor(name="a", reading="2020-01-01T00:00:00")
        )

    async def test_two_attrs_arguments_are_two_entries_and_each_one_is_cached(self) -> None:
        await _assert_separate_entries(Sensor(name="a", reading=1), Sensor(name="b", reading=1))


class TestThePairOrderOfAMappingIsTotal:
    """AC6: no part of a mapping's rendering depends on insertion order."""

    def test_two_keys_that_encode_identically_render_the_same_either_way(self) -> None:
        """Two pairs differing only in the value: only the whole pair orders them.

        That there really are two pairs is pinned by
        ``TestAMappingKeepsEveryPair``, so this assertion cannot hold for an
        empty reason.
        """
        first, second = IdentityKey("a"), IdentityKey("a")

        assert _render({first: 1, second: 2}) == _render({second: 2, first: 1})


class TestTheKeyIsTheDocumentedString:
    """FR-083: the key format and the full digest, asserted as values."""

    async def test_the_stored_key_is_the_documented_string(self) -> None:
        backend = CountingCacheBackend()

        await _calls(backend).wrap(known_call)("q")

        # The rendered arguments carry the bound default, one labelled pair per
        # parameter, ordered by the encoding of the whole pair.
        rendered = [
            "builtins.dict",
            [
                [["builtins.str", "limit"], ["builtins.int", 10]],
                [["builtins.str", "query"], ["builtins.str", "q"]],
            ],
        ]
        digest = hashlib.sha256(msgspec.json.encode(rendered)).hexdigest()
        expected = f"call:{known_call.__module__}.{known_call.__qualname__}:v3:{digest}"
        assert list(backend.data) == [expected]

    async def test_the_digest_is_the_full_sha256(self) -> None:
        """Not ``stable_hash``: these arguments are often chosen by a model."""
        backend = CountingCacheBackend()

        await _calls(backend).wrap(known_call)("q")

        digest = next(iter(backend.data)).rsplit(":", 1)[1]
        assert len(digest) == 64
        assert set(digest) <= set("0123456789abcdef")


class TestConcurrentCallersShareOneLoad:
    """AC3: a burst on a cold key runs the body once."""

    async def test_two_concurrent_calls_run_the_body_once(self) -> None:
        runs = 0

        @cache_call()
        async def fetch(query: str) -> Doc:
            nonlocal runs
            runs += 1
            await asyncio.sleep(0.01)
            return Doc(title=query)

        wrapped = _calls(_gateway("calls_single_flight")).wrap(fetch)

        first, second = await asyncio.gather(wrapped("q"), wrapped("q"))

        assert runs == 1
        assert first == second == Doc(title="q")


class TestAHitAnswersWhatAMissAnswered:
    """AC4: the declared type survives the round trip on both paths."""

    async def test_a_list_of_structs_round_trips(self) -> None:
        @cache_call()
        async def fetch(query: str) -> list[Doc]:
            return [Doc(title=query)]

        wrapped = _calls(_gateway("calls_struct_list")).wrap(fetch)

        assert await wrapped("q") == await wrapped("q") == [Doc(title="q")]

    async def test_a_pydantic_model_round_trips(self) -> None:
        """The L13 shape: a pydantic value used to raise on write and read as a miss."""

        @cache_call()
        async def fetch(query: str) -> PydanticDoc:
            return PydanticDoc(title=query)

        wrapped = _calls(_gateway("calls_pydantic")).wrap(fetch)

        assert await wrapped("q") == await wrapped("q") == PydanticDoc(title="q")

    async def test_an_optional_list_of_models_round_trips(self) -> None:
        @cache_call()
        async def fetch(query: str) -> list[PydanticDoc] | None:
            return [PydanticDoc(title=query)]

        wrapped = _calls(_gateway("calls_optional_models")).wrap(fetch)

        assert await wrapped("q") == await wrapped("q") == [PydanticDoc(title="q")]

    async def test_the_wrapper_carries_resolved_annotations(self) -> None:
        """The wrapper answers for itself, with the metadata a schema needs.

        ``functools.wraps`` would leave the strings of the declaring module
        behind; the resolved hints keep ``Annotated`` metadata, which is where
        a tool parameter's description lives.
        """

        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        wrapped = _calls(CountingCacheBackend()).wrap(fetch)

        assert wrapped.__annotations__ == {"query": str, "return": Doc}


class TestUnlessRefusesToStore:
    """AC5: an answer the caller calls worthless is returned but never stored."""

    async def test_an_empty_answer_is_returned_and_not_stored(self) -> None:
        runs = 0

        @cache_call(unless=lambda docs: not docs)
        async def fetch(query: str) -> list[Doc]:
            nonlocal runs
            runs += 1
            return []

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(fetch)

        assert await wrapped("q") == []
        assert await wrapped("q") == []
        assert runs == 2
        assert backend.data == {}
        assert backend.set_ttls == []

    async def test_a_non_empty_answer_is_stored(self) -> None:
        @cache_call(unless=lambda docs: not docs)
        async def fetch(query: str) -> list[Doc]:
            return [Doc(title=query)]

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(fetch)

        await wrapped("q")

        assert len(backend.data) == 1

    async def test_a_skipped_result_is_the_body_own_object(self) -> None:
        """FR-086c: the one path where the caller is not handed a decoded value.

        ``moment`` is typed ``Any``, so the codec's JSON-mode dump would turn
        the ``datetime`` into a ``str``. A skipped result never reaches the
        codec, so the caller gets the object the body built.
        """
        moment = datetime(2026, 9, 8, tzinfo=UTC)

        @cache_call(unless=lambda doc: doc.stale)
        async def fetch(*, stale: bool) -> LooseDoc:
            return LooseDoc(stale=stale, moment=moment)

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(fetch)

        skipped = await wrapped(stale=True)
        stored = await wrapped(stale=False)

        assert skipped.moment is moment
        assert isinstance(stored.moment, str)
        assert len(backend.data) == 1

    async def test_a_predicate_that_raises_propagates_and_stores_nothing(self) -> None:
        """A broken predicate is a bug in the caller, not something to swallow."""

        def explode(_result: object) -> bool:
            raise RuntimeError("broken predicate")

        @cache_call(unless=explode)
        async def fetch(query: str) -> list[Doc]:
            return [Doc(title=query)]

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(fetch)

        with pytest.raises(RuntimeError, match="broken predicate"):
            await wrapped("q")

        assert backend.data == {}


class TestTheKeyNamesTheFunctionAndItsVersion:
    """AC6: nothing but this function, at this version, reads its entries."""

    async def test_bumping_the_version_changes_the_key(self) -> None:
        @cache_call(version=1)
        async def fetch_v1(query: str) -> Doc:
            return Doc(title=query)

        @cache_call(version=2)
        async def fetch_v2(query: str) -> Doc:
            return Doc(title=query)

        fetch_v2.__qualname__ = fetch_v1.__qualname__
        backend = CountingCacheBackend()
        calls = _calls(backend)

        await calls.wrap(fetch_v1)("q")
        await calls.wrap(fetch_v2)("q")

        assert len(backend.data) == 2

    async def test_the_same_qualname_in_two_modules_gives_two_keys(self) -> None:
        """The module is in the key, so two homonyms never share an entry."""

        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        @cache_call()
        async def fetch_elsewhere(query: str) -> Doc:
            return Doc(title=query)

        fetch_elsewhere.__qualname__ = fetch.__qualname__
        fetch_elsewhere.__module__ = "another.module"
        backend = CountingCacheBackend()
        calls = _calls(backend)

        await calls.wrap(fetch)("q")
        await calls.wrap(fetch_elsewhere)("q")

        assert len(backend.data) == 2


class _Base:
    """Base of the bound toolset, contributing one inherited coroutine."""

    calls_made = 0

    @cache_call()
    async def search(self, query: str) -> Doc:
        type(self).calls_made += 1
        return Doc(title=query)

    @property
    def broken(self) -> str:
        """Property that must never be evaluated by discovery."""
        raise AssertionError("bind evaluated a property")


class _Tools(_Base):
    """Toolset class whose own methods follow its base's."""

    async def summarise(self, text: str) -> str:
        return text.upper()

    async def _hidden(self) -> str:
        return "private"

    def sync_helper(self) -> str:
        return "not a coroutine"


class _SyncOverride(_Base):
    """Subclass that shadows the base's coroutine with a property."""

    # The incompatible override is the fixture: a toolset that replaces an async
    # base tool with a plain attribute is exactly what FR-087 must unpublish.
    @property
    def search(self) -> str:  # type: ignore[override]  # pyright: ignore[reportIncompatibleMethodOverride]
        """Not awaitable, so it cannot be published as a tool."""
        return "not a coroutine"


class TestAnOverrideThatIsNotACoroutine:
    """FR-087: the method is unpublished, and the change is announced."""

    def test_the_shadowed_method_is_not_published_and_is_announced(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        calls = _calls(CountingCacheBackend())

        bound = calls.bind(_SyncOverride())
        calls.bind(_SyncOverride())

        assert bound == []
        records = _warnings(caplog, "CacheCallOverriddenBySync")
        assert len(records) == 1
        assert "_SyncOverride.search" in records[0].getMessage()


class TestBindPublishesTheCoroutineMethods:
    """AC7: binding an object yields its public coroutines, base first."""

    def test_the_methods_are_published_in_declaration_order(self) -> None:
        bound = _ConfiguredCalls(
            _config(), CountingCacheBackend(), SingleFlight(), random.Random()
        ).bind(_Tools())

        assert [method.__name__ for method in bound] == ["search", "summarise"]

    def test_a_property_is_never_evaluated(self) -> None:
        """Discovery reads the class attribute, so a raising getter is harmless."""
        calls = _calls(CountingCacheBackend())

        assert calls.bind(_Tools())

    async def test_an_unmarked_method_is_bound_and_unchanged(self) -> None:
        bound = _calls(CountingCacheBackend()).bind(_Tools())
        summarise = bound[1]

        assert await summarise("hi") == "HI"
        assert not getattr(summarise, "__cache_call_wrapped__", False)

    async def test_two_instances_share_one_entry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The key is the arguments, so a stateless toolset caches across instances."""
        calls = _calls(_gateway("calls_two_instances"))
        first = calls.bind(_Tools())[0]
        second = calls.bind(_Tools())[0]
        monkeypatch.setattr(_Tools, "calls_made", 0)

        await first("q")
        await second("q")

        assert _Tools.calls_made == 1


class _StaticTools:
    """Toolset whose tools are static and class methods, marked and not.

    An ``async`` ``staticmethod`` reaches ``bind`` as a descriptor rather than
    as a function, so discovery that asks the raw class entry whether it is a
    coroutine unpublishes it — the very silence ``CacheCallOverriddenBySync``
    exists to prevent.
    """

    static_calls = 0
    class_calls = 0

    @staticmethod
    @cache_call()
    async def cached_static(query: str) -> Doc:
        _StaticTools.static_calls += 1
        return Doc(title=query)

    @classmethod
    @cache_call()
    async def cached_class(cls, query: str) -> Doc:
        cls.class_calls += 1
        return Doc(title=query)

    @staticmethod
    async def plain_static(text: str) -> str:
        return text.upper()

    @classmethod
    async def plain_class(cls, text: str) -> str:
        return f"{cls.__name__}:{text}"

    async def instance_method(self, text: str) -> str:
        return text.lower()


class TestBindPublishesStaticAndClassMethods:
    """FR-087: a descriptor holds a coroutine, so the tool must be published."""

    def test_every_public_coroutine_is_published(self) -> None:
        bound = _calls(CountingCacheBackend()).bind(_StaticTools())

        assert [method.__name__ for method in bound] == [
            "cached_static",
            "cached_class",
            "plain_static",
            "plain_class",
            "instance_method",
        ]

    async def test_a_marked_static_method_is_cached(self, monkeypatch: pytest.MonkeyPatch) -> None:
        wrapped = _calls(_gateway("calls_static_method")).bind(_StaticTools())[0]
        monkeypatch.setattr(_StaticTools, "static_calls", 0)

        assert await wrapped("q") == Doc(title="q")
        assert await wrapped("q") == Doc(title="q")

        assert _StaticTools.static_calls == 1

    async def test_a_marked_class_method_is_cached(self, monkeypatch: pytest.MonkeyPatch) -> None:
        wrapped = _calls(_gateway("calls_class_method")).bind(_StaticTools())[1]
        monkeypatch.setattr(_StaticTools, "class_calls", 0)

        assert await wrapped("q") == Doc(title="q")
        assert await wrapped("q") == Doc(title="q")

        assert _StaticTools.class_calls == 1

    async def test_an_unmarked_static_method_is_bound_and_unchanged(self) -> None:
        bound = _calls(CountingCacheBackend()).bind(_StaticTools())[2]

        assert await bound("hi") == "HI"
        assert not getattr(bound, "__cache_call_wrapped__", False)

    async def test_an_unmarked_class_method_is_bound_and_unchanged(self) -> None:
        bound = _calls(CountingCacheBackend()).bind(_StaticTools())[3]

        assert await bound("hi") == "_StaticTools:hi"
        assert not getattr(bound, "__cache_call_wrapped__", False)

    def test_a_static_property_is_still_never_evaluated(self) -> None:
        """Unwrapping the descriptors must not start reading the instance."""
        assert _calls(CountingCacheBackend()).bind(_Tools())


_TENANT: ContextVar[str] = ContextVar("tenant")


class TestAmbientStateLeaksBetweenCallers:
    """AC8: why ``@cache_call`` demands a pure function of its arguments.

    This is a characterisation test, not a wish: a coroutine that reads a
    contextvar tenant serves the **first** caller's answer to every later one,
    because the key sees only the arguments and the load is detached into its
    own task. It is the reason the purity contract exists, and it fails the day
    ambient state starts reaching the key — which is the point.
    """

    async def test_the_first_callers_tenant_is_served_to_the_second(self) -> None:
        @cache_call()
        async def whoami() -> str:
            return _TENANT.get()

        wrapped = _calls(_gateway("calls_contextvar")).wrap(whoami)

        _TENANT.set("tenant-a")
        first = await wrapped()
        _TENANT.set("tenant-b")
        second = await wrapped()

        assert first == "tenant-a"
        assert second == "tenant-a"


class TestWithoutASection:
    """AC9: an unconfigured deployment learns at boot, not from its bill."""

    async def test_a_marked_function_runs_uncached_and_warns_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        runs = 0

        @cache_call()
        async def fetch(query: str) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=query)

        calls = _UnconfiguredCalls()
        wrapped = calls.wrap(fetch)
        calls.wrap(fetch)

        assert wrapped is fetch
        await wrapped("q")
        await wrapped("q")

        assert runs == 2
        records = _warnings(caplog, "CacheCallNotConfigured")
        assert len(records) == 1
        assert f"{fetch.__module__}.{fetch.__qualname__}" in records[0].getMessage()

    def test_an_unmarked_function_is_not_announced(self, caplog: pytest.LogCaptureFixture) -> None:
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        assert _UnconfiguredCalls().wrap(fetch) is fetch
        assert _warnings(caplog, "CacheCallNotConfigured") == []

    def test_a_bare_container_answers_with_the_pass_through(self) -> None:
        """``cached_calls`` must not raise for a container built outside the bootstraps."""
        assert isinstance(cached_calls(LoomContainer()), _UnconfiguredCalls)

    def test_a_registered_implementation_is_returned(self) -> None:
        container = LoomContainer()
        calls = _calls(CountingCacheBackend())
        container.register_instance(CachedCalls, calls)

        assert cached_calls(container) is calls

    def test_the_pass_through_binds_the_same_methods(self) -> None:
        bound = _UnconfiguredCalls().bind(_Tools())

        assert [method.__name__ for method in bound] == ["search", "summarise"]


class _RaisingBackend(CountingCacheBackend):
    """Backend whose writes fail the way a rejected serialization does.

    Its message deliberately spells neither ``key`` nor the key itself, so an
    assertion on the logged fields cannot be satisfied by the error's repr.
    """

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        raise CacheWriteError("the serializer refused the value")


class TestAFailedWriteNeverFailsTheCall:
    """AC10: the answer is already produced; the cache is the optional part."""

    async def test_a_write_error_is_logged_once_and_the_value_returned(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        wrapped = _calls(_RaisingBackend()).wrap(fetch)

        assert await wrapped("q") == Doc(title="q")
        assert await wrapped("q") == Doc(title="q")

        records = _warnings(caplog, "CacheCallWriteFailed")
        assert len(records) == 1
        message = records[0].getMessage()
        assert f"'function': '{fetch.__module__}.{fetch.__qualname__}'" in message
        assert f"'key': '{_expected_key(fetch)}'" in message
        assert "'value_type': 'Doc'" in message

    async def test_an_error_from_the_body_propagates_and_stores_nothing(self) -> None:
        @cache_call()
        async def fetch(query: str) -> Doc:
            raise RuntimeError("upstream down")

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(fetch)

        with pytest.raises(RuntimeError, match="upstream down"):
            await wrapped("q")

        assert backend.data == {}


class _ReadOutageBackend(CountingCacheBackend):
    """Backend whose reads fail the way an unreachable Redis does."""

    async def get_value(self, key: str, *, type: Any = None) -> Any:
        raise ConnectionError("redis is down")


class _WriteOutageBackend(CountingCacheBackend):
    """Backend that reads a miss and whose writes fail at the transport."""

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        raise ConnectionError("redis is down")


class TestABackendOutageDegradesToAnUncachedCall:
    """FR-089 taken to its conclusion: a cache problem is not an outage.

    ``CacheWriteError`` covers a value the serializer refused and nothing else,
    so guarding only that leaves every transport failure — a connection reset,
    a timeout — failing calls that a cache-less deployment would have served.
    """

    async def test_a_failing_read_runs_the_body_and_logs_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        runs = 0

        @cache_call()
        async def fetch(query: str) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title=query)

        wrapped = _calls(_ReadOutageBackend()).wrap(fetch)

        assert await wrapped("q") == Doc(title="q")
        assert await wrapped("q") == Doc(title="q")

        assert runs == 2
        records = _warnings(caplog, "CacheCallReadFailed")
        assert len(records) == 1
        message = records[0].getMessage()
        assert f"'function': '{fetch.__module__}.{fetch.__qualname__}'" in message
        assert f"'key': '{_expected_key(fetch)}'" in message
        assert "'error': 'ConnectionError'" in message

    async def test_a_failing_write_returns_the_value_and_logs_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        wrapped = _calls(_WriteOutageBackend()).wrap(fetch)

        assert await wrapped("q") == Doc(title="q")
        assert await wrapped("q") == Doc(title="q")

        records = _warnings(caplog, "CacheCallWriteFailed")
        assert len(records) == 1
        message = records[0].getMessage()
        assert f"'key': '{_expected_key(fetch)}'" in message
        assert "ConnectionError" in message

    async def test_an_error_from_the_body_still_propagates_under_an_outage(self) -> None:
        """The guards cover the two gateway calls, never the body between them."""

        @cache_call()
        async def fetch(query: str) -> Doc:
            raise RuntimeError("upstream down")

        wrapped = _calls(_ReadOutageBackend()).wrap(fetch)

        with pytest.raises(RuntimeError, match="upstream down"):
            await wrapped("q")


class TestTheWarningMemo:
    """The dedupe is per event, per function and per binder instance."""

    async def test_two_events_about_one_function_are_both_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Deduping on the function alone would hide the second failure."""

        @cache_call()
        async def fetch(sample: Any) -> Doc:
            return Doc(title="fetched")

        wrapped = _calls(_RaisingBackend()).wrap(fetch)

        await wrapped(float("nan"))
        await wrapped(1.0)

        assert len(_warnings(caplog, "CacheCallKeyUnrenderable")) == 1
        assert len(_warnings(caplog, "CacheCallWriteFailed")) == 1

    def test_one_event_about_two_functions_is_logged_twice(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Deduping on the event alone would announce only the first function."""

        @cache_call()
        async def first(query: str) -> dict[str, Any]:
            return {"title": query}

        @cache_call()
        async def second(query: str) -> dict[str, Any]:
            return {"title": query}

        calls = _calls(CountingCacheBackend())
        calls.wrap(first)
        calls.wrap(second)

        assert len(_warnings(caplog, "CacheCallNotCacheable")) == 2

    def test_a_second_binder_warns_again(self, caplog: pytest.LogCaptureFixture) -> None:
        """The memo belongs to the instance, not to the module."""

        @cache_call()
        async def fetch(query: str) -> dict[str, Any]:
            return {"title": query}

        _calls(CountingCacheBackend()).wrap(fetch)
        _calls(CountingCacheBackend()).wrap(fetch)

        assert len(_warnings(caplog, "CacheCallNotCacheable")) == 2


class _ExplodingCodec:
    """Codec whose read path fails for a reason that is not a mismatch."""

    def encode(self, result: Any) -> EncodedResult:
        return EncodedResult(msgspec.to_builtins(result), result)

    def decode(self, payload: Any) -> Any:
        raise RuntimeError("the codec is broken")


class TestTheDecodeGuardIsNotACatchAll:
    """A stale payload is a miss; a broken codec is a bug and must be seen."""

    async def test_an_error_that_is_not_a_validation_failure_propagates(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        monkeypatch.setattr(
            "loom.core.cache.calls.build_call_codec", lambda _func: _ExplodingCodec()
        )
        wrapped = _calls(CountingCacheBackend()).wrap(fetch)
        await wrapped("q")

        with pytest.raises(RuntimeError, match="the codec is broken"):
            await wrapped("q")

        assert _warnings(caplog, "CacheCallPayloadMismatch") == []


class Point(msgspec.Struct):
    """Argument whose float field the renderer has to reach."""

    value: float


class Unbounded(enum.Enum):
    """Enum whose members are floats, so the walk only sees them as a leaf.

    :func:`msgspec.to_builtins` hands back the raw value, which is why the
    rendering has to be checked as well as the argument.
    """

    NAN = float("nan")
    INFINITY = float("inf")
    NEGATIVE_INFINITY = float("-inf")
    FINITE = 1.5


@dataclasses.dataclass
class Pending:
    """Dataclass whose ``init=False`` field is never assigned any value.

    Reading it raises :class:`AttributeError`, which the walk owes the caller a
    cached-or-uncached answer for rather than the error.
    """

    name: str
    computed: int = dataclasses.field(init=False)


@dataclasses.dataclass
class Threshold:
    """Dataclass whose field has a class-level default.

    Passing the **class** itself is what the walk's ``isinstance(value, type)``
    guard is for: without it the default would be read off the class and the
    class would be keyed as though it were an instance of itself.
    """

    limit: int = 10


class TestAnArgumentWithoutACanonicalRendering:
    """AC11: a key that cannot be built runs the call, it does not break it."""

    @pytest.mark.parametrize(
        "argument",
        [
            pytest.param(float("nan"), id="nan"),
            pytest.param(float("inf"), id="infinity"),
            pytest.param(Point(value=float("nan")), id="nan-inside-a-struct"),
            pytest.param(Point(value=float("-inf")), id="infinity-inside-a-struct"),
            pytest.param(Unbounded.NAN, id="nan-as-an-enum-value"),
            pytest.param(Unbounded.INFINITY, id="infinity-as-an-enum-value"),
            pytest.param(Unbounded.NEGATIVE_INFINITY, id="negative-infinity-as-an-enum-value"),
        ],
    )
    async def test_a_non_finite_float_runs_the_call_uncached(
        self, argument: object, caplog: pytest.LogCaptureFixture
    ) -> None:
        """msgspec renders ``nan`` as ``null``, which would collide with ``None``."""
        runs = 0

        @cache_call()
        async def measure(sample: Any) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title="measured")

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(measure)

        assert await wrapped(argument) == Doc(title="measured")
        assert await wrapped(argument) == Doc(title="measured")

        assert runs == 2
        assert backend.data == {}
        assert len(_warnings(caplog, "CacheCallKeyUnrenderable")) == 1

    async def test_an_open_socket_runs_the_call_uncached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        runs = 0

        @cache_call()
        async def probe(connection: Any) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title="probed")

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(probe)

        with socket.socket() as connection:
            await wrapped(connection)
            await wrapped(connection)

        assert runs == 2
        assert backend.data == {}
        assert len(_warnings(caplog, "CacheCallKeyUnrenderable")) == 1

    async def test_a_self_referential_list_runs_the_call_uncached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The walk cannot terminate on a cycle, and exhausting the stack is not an answer."""
        cyclic: list[Any] = []
        cyclic.append(cyclic)

        assert await _runs_and_entries(cyclic, cyclic) == (2, 0)
        assert len(_warnings(caplog, "CacheCallKeyUnrenderable")) == 1

    async def test_a_self_referential_mapping_runs_the_call_uncached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        cyclic: dict[str, Any] = {}
        cyclic["self"] = cyclic

        assert await _runs_and_entries(cyclic, cyclic) == (2, 0)
        assert len(_warnings(caplog, "CacheCallKeyUnrenderable")) == 1

    async def test_an_unassigned_dataclass_field_runs_the_call_uncached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``getattr`` raises for the field, and no argument shape may reach the caller."""
        pending = Pending(name="a")

        assert await _runs_and_entries(pending, pending) == (2, 0)
        assert len(_warnings(caplog, "CacheCallKeyUnrenderable")) == 1

    async def test_a_dataclass_class_runs_the_call_uncached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A class is not an instance: msgspec refuses it, and the walk must not descend it.

        Walked as an instance it would key on its class-level defaults, so two
        distinct classes with the same defaults would share one entry.
        """
        assert await _runs_and_entries(Threshold, Threshold) == (2, 0)
        assert len(_warnings(caplog, "CacheCallKeyUnrenderable")) == 1

    async def test_a_mapping_keyed_by_none_is_cached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """It was refused only because a rendered mapping had to be a ``dict``.

        A mapping is now a list of pairs, which is valid JSON whatever its keys
        are, so a legitimate argument that JSON cannot use as an object key is
        cached like any other instead of running uncached forever.
        """
        runs = 0

        @cache_call()
        async def fetch(criteria: Any) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title="fetched")

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(fetch)

        assert await wrapped({None: "v"}) == Doc(title="fetched")
        assert await wrapped({None: "v"}) == Doc(title="fetched")

        assert runs == 1
        assert len(backend.data) == 1
        assert _warnings(caplog, "CacheCallKeyUnrenderable") == []

    async def test_a_finite_enum_float_is_cached(self) -> None:
        """The control: the refusal is about the value, not about the ``Enum``."""
        assert await _runs_and_entries(Unbounded.FINITE, Unbounded.FINITE) == (1, 1)

    async def test_a_finite_float_argument_is_cached(self) -> None:
        runs = 0

        @cache_call()
        async def measure(sample: float) -> Doc:
            nonlocal runs
            runs += 1
            return Doc(title="measured")

        wrapped = _calls(_gateway("calls_finite_float")).wrap(measure)

        await wrapped(1.5)
        await wrapped(1.5)

        assert runs == 1


class TestTheWrittenTtl:
    """AC11: the TTL a cached call writes, its override and its spread."""

    async def _write_once(
        self, backend: CountingCacheBackend, *, config: CacheConfig, argument: str = "q"
    ) -> None:
        @cache_call(ttl_key="web_search")
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        await _calls(backend, config=config).wrap(fetch)(argument)

    async def test_the_ttl_key_override_is_written(self) -> None:
        backend = CountingCacheBackend()

        await self._write_once(backend, config=_config(ttl={"web_search": 900}))

        assert backend.set_ttls == [900]

    async def test_without_an_override_the_default_ttl_is_written(self) -> None:
        backend = CountingCacheBackend()

        await self._write_once(backend, config=_config())

        assert backend.set_ttls == [TTL]

    async def test_a_call_without_a_ttl_key_writes_the_default(self) -> None:
        @cache_call()
        async def fetch(query: str) -> Doc:
            return Doc(title=query)

        backend = CountingCacheBackend()

        await _calls(backend).wrap(fetch)("q")

        assert backend.set_ttls == [TTL]

    async def test_the_written_ttls_are_not_all_the_same(self) -> None:
        """Deleting the spread would keep every band assertion green."""
        backend = CountingCacheBackend()
        config = _config(ttl_jitter=0.2)

        for index in range(20):
            await self._write_once(backend, config=config, argument=f"q{index}")

        assert len(set(backend.set_ttls)) > 1

    async def test_the_written_ttl_lands_inside_the_jitter_band(self) -> None:
        backend = CountingCacheBackend()
        config = _config(ttl_jitter=0.1)

        for index in range(20):
            await self._write_once(backend, config=config, argument=f"q{index}")

        assert len(backend.set_ttls) == 20
        assert all(ttl is not None and TTL * 0.9 <= ttl <= TTL * 1.1 for ttl in backend.set_ttls)

    @pytest.mark.usefixtures("_restore_global_random")
    async def test_the_generator_belongs_to_the_instance(self) -> None:
        """Seeding the process-wide ``random`` must not decide a cache TTL."""

        async def twenty(seed: int, global_seed: int) -> list[int | None]:
            backend = CountingCacheBackend()
            random.seed(global_seed)
            config = _config(ttl_jitter=0.2)
            calls = _calls(backend, config=config, rng=random.Random(seed))

            @cache_call()
            async def fetch(query: str) -> Doc:
                return Doc(title=query)

            wrapped = calls.wrap(fetch)
            for index in range(20):
                await wrapped(f"q{index}")
            return backend.set_ttls

        seeded = await twenty(1234, 1)

        assert seeded == await twenty(1234, 2)
        assert len(set(seeded)) > 1


class TestAPayloadThatNoLongerFits:
    """AC16: a stale entry is a miss, not an outage for a whole TTL."""

    async def test_a_stale_payload_is_re_stored_and_logged_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        @cache_call()
        async def read(query: str) -> OldDoc:
            return OldDoc(title=query)

        @cache_call()
        async def read_new(query: str) -> NewDoc:
            return NewDoc(title=query, author="ada")

        read_new.__qualname__ = read.__qualname__
        read_new.__module__ = read.__module__
        backend = CountingCacheBackend()
        calls = _calls(backend)

        await calls.wrap(read)("q")
        first = await calls.wrap(read_new)("q")
        second = await calls.wrap(read_new)("q")

        assert first == second == NewDoc(title="q", author="ada")
        assert len(backend.set_ttls) == 2
        records = _warnings(caplog, "CacheCallPayloadMismatch")
        assert len(records) == 1
        assert "call:" in records[0].getMessage()

    async def test_a_mismatch_logs_no_value_from_the_stored_payload(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A pydantic error spells ``input_value=``; the log must not repeat it."""
        stored_title = "another-callers-private-answer"

        @cache_call()
        async def read(query: str) -> OldPydanticDoc:
            return OldPydanticDoc(title=stored_title)

        @cache_call()
        async def read_new(query: str) -> NewPydanticDoc:
            return NewPydanticDoc(title=stored_title, author="ada")

        read_new.__qualname__ = read.__qualname__
        read_new.__module__ = read.__module__
        calls = _calls(CountingCacheBackend())

        await calls.wrap(read)("q")
        await calls.wrap(read_new)("q")

        records = _warnings(caplog, "CacheCallPayloadMismatch")
        assert len(records) == 1
        message = records[0].getMessage()
        assert "call:" in message
        assert stored_title not in message
        assert "input_value" not in message

    async def test_a_none_result_is_stored_and_read_back_as_a_miss(self) -> None:
        """``None`` is the backends' miss sentinel; declare ``unless`` when it costs."""
        runs = 0

        @cache_call()
        async def fetch(query: str) -> str | None:
            nonlocal runs
            runs += 1
            return None

        backend = CountingCacheBackend()
        wrapped = _calls(backend).wrap(fetch)

        assert await wrapped("q") is None
        assert await wrapped("q") is None

        assert runs == 2
        assert len(backend.set_ttls) == 2
