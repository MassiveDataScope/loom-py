"""Return types of a ``@cache_query`` read, on a miss and on a hit.

A cached custom read stores builtins, so without a codec the first call
returns whatever the method built and every later call returns the decoded
payload: a struct on the miss, a ``dict`` on the hit, and an ``AttributeError``
in the consumer that reads an attribute off it. The codec is derived from the
declared return annotation and applied on both paths, so the two calls are
indistinguishable in value *and* in type.

A method the grammar cannot describe is refused when the repository is
wrapped, so the application fails at boot rather than serving a hit whose
type differs from the miss.
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any

import pytest

from loom.core.cache import CacheConfig, CachedRepository
from loom.core.cache.decorators import cache_query
from loom.core.cache.result_codec import build_result_codec
from loom.core.model import BoundaryValidationError

from ._doubles import (
    CachedEnv,
    CodecRepository,
    CustomCallCountingRepository,
    EvolvedRepository,
    EvolvedStats,
    LateStats,
    LyingRepository,
    OutOfGrammarReadRepository,
    Stats,
    UnannotatedReadRepository,
    UnmarkedOverrideRepository,
    UnresolvableReadRepository,
    Widget,
    WidgetCreate,
    WidgetUpdate,
    rewrap_with_cache,
    wrap_with_cache,
)

ROW_COUNT = 3

Wrapper = CachedRepository[Widget, WidgetCreate, WidgetUpdate, Any]


def _widgets(count: int) -> list[Widget]:
    return [Widget(id=index, name=f"w{index}") for index in range(1, count + 1)]


def _expected_stats_list() -> list[Stats]:
    return [Stats(total=index, label=str(index + 1)) for index in range(ROW_COUNT)]


@pytest.fixture
def codec_env(cache_config: CacheConfig) -> CachedEnv[Widget]:
    """A cached repository over a double covering the codec grammar."""
    return wrap_with_cache(CodecRepository(_widgets(ROW_COUNT), Widget), cache_config)


@pytest.fixture
def codec_repository(codec_env: CachedEnv[Widget]) -> CodecRepository[Widget]:
    """The repository double behind ``codec_env``."""
    repository = codec_env.repository
    assert isinstance(repository, CodecRepository)
    return repository


async def _call_twice(wrapper: Wrapper, method_name: str, *args: Any) -> tuple[Any, Any]:
    """Call a cached read twice, so the second call is served from the cache."""
    method = getattr(wrapper, method_name)
    return await method(*args), await method(*args)


class TestDeclaredReturnTypeSurvivesTheCache:
    """Every read in the grammar answers the same value and type on both calls."""

    @pytest.mark.parametrize(
        ("method_name", "args", "expected"),
        [
            ("stats", (), Stats(total=ROW_COUNT, label="all")),
            ("stats_list", (), _expected_stats_list()),
            ("stats_tuple", (), tuple(_expected_stats_list())),
            ("stats_for", ("known",), Stats(total=ROW_COUNT, label="known")),
            ("stats_for", ("other",), None),
            ("total", (), ROW_COUNT),
            ("newest", (), Widget(id=ROW_COUNT, name=f"w{ROW_COUNT}")),
            ("late_stats", (), LateStats(count=ROW_COUNT)),
        ],
    )
    async def test_both_calls_return_the_declared_value(
        self,
        codec_env: CachedEnv[Widget],
        method_name: str,
        args: tuple[Any, ...],
        expected: Any,
    ) -> None:
        first, second = await _call_twice(codec_env.wrapper, method_name, *args)

        assert first == expected
        assert second == expected

    @pytest.mark.parametrize(
        ("method_name", "args", "expected_type"),
        [
            ("stats", (), Stats),
            ("stats_list", (), list),
            ("stats_tuple", (), tuple),
            ("stats_for", ("known",), Stats),
            ("total", (), int),
            ("newest", (), Widget),
            ("late_stats", (), LateStats),
        ],
    )
    async def test_both_calls_return_the_declared_type(
        self,
        codec_env: CachedEnv[Widget],
        method_name: str,
        args: tuple[Any, ...],
        expected_type: type,
    ) -> None:
        first, second = await _call_twice(codec_env.wrapper, method_name, *args)

        assert type(first) is expected_type
        assert type(second) is expected_type

    @pytest.mark.parametrize(
        ("method_name", "args"),
        [
            ("stats", ()),
            ("stats_list", ()),
            ("stats_tuple", ()),
            ("stats_for", ("known",)),
            ("total", ()),
            ("newest", ()),
            ("late_stats", ()),
        ],
    )
    async def test_the_second_call_is_served_from_the_cache(
        self,
        codec_env: CachedEnv[Widget],
        codec_repository: CodecRepository[Widget],
        method_name: str,
        args: tuple[Any, ...],
    ) -> None:
        await _call_twice(codec_env.wrapper, method_name, *args)

        assert codec_repository.custom_calls[method_name] == 1

    async def test_the_elements_of_a_cached_container_keep_their_type(
        self,
        codec_env: CachedEnv[Widget],
    ) -> None:
        first, second = await _call_twice(codec_env.wrapper, "stats_list")

        assert [type(item) for item in first] == [Stats] * ROW_COUNT
        assert [type(item) for item in second] == [Stats] * ROW_COUNT


class TestAbsentResults:
    """An optional read that finds nothing is answered, not cached."""

    async def test_none_is_returned_on_both_calls(self, codec_env: CachedEnv[Widget]) -> None:
        first, second = await _call_twice(codec_env.wrapper, "stats_for", "other")

        assert first is None
        assert second is None

    async def test_none_is_not_stored_so_the_read_runs_again(
        self,
        codec_env: CachedEnv[Widget],
        codec_repository: CodecRepository[Widget],
    ) -> None:
        await _call_twice(codec_env.wrapper, "stats_for", "other")

        assert codec_repository.custom_calls["stats_for"] == 2


class TestAuditFinding05:
    """The reported regression: a struct on the miss, a ``dict`` on the hit."""

    async def test_the_cached_call_still_exposes_the_struct_attributes(
        self,
        codec_env: CachedEnv[Widget],
    ) -> None:
        first, second = await _call_twice(codec_env.wrapper, "stats")

        assert first.total == ROW_COUNT
        assert second.total == ROW_COUNT
        assert second.label == "all"

    async def test_the_cached_call_does_not_return_a_mapping(
        self,
        codec_env: CachedEnv[Widget],
    ) -> None:
        _first, second = await _call_twice(codec_env.wrapper, "stats")

        assert not isinstance(second, dict)


class TestUnusableAnnotations:
    """A declared type outside the grammar is refused when the repository is wrapped."""

    @pytest.mark.parametrize(
        ("repository_type", "method_name"),
        [
            (UnannotatedReadRepository, "unannotated"),
            (UnresolvableReadRepository, "unresolvable"),
            (OutOfGrammarReadRepository, "out_of_grammar"),
        ],
    )
    def test_wrapping_names_the_method_and_what_to_annotate(
        self,
        cache_config: CacheConfig,
        repository_type: type[CustomCallCountingRepository[Widget]],
        method_name: str,
    ) -> None:
        repository = repository_type(_widgets(ROW_COUNT), Widget)

        with pytest.raises(RuntimeError) as excinfo:
            wrap_with_cache(repository, cache_config)

        message = str(excinfo.value)
        assert f"{repository_type.__name__}.{method_name}" in message
        assert "Annotate its return type" in message

    def test_a_repository_whose_reads_are_inside_the_grammar_wraps_without_warning(
        self,
        cache_config: CacheConfig,
    ) -> None:
        repository = CodecRepository(_widgets(ROW_COUNT), Widget)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            wrap_with_cache(repository, cache_config)

    def test_a_cached_read_absent_from_the_class_is_refused_when_reached(
        self,
        codec_env: CachedEnv[Widget],
    ) -> None:
        """A read set on the instance escapes the wrap-time scan of the class."""

        @cache_query(scope="list")
        async def attached() -> Stats:
            return Stats(total=0, label="attached")

        codec_env.repository.attached = attached  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="attached"):
            _ = codec_env.wrapper.attached


class TestCodecConstruction:
    """Codecs are built once, from the class dictionaries, never by attribute access."""

    def test_the_wrapper_of_a_cached_read_is_memoised(
        self,
        codec_env: CachedEnv[Widget],
    ) -> None:
        assert codec_env.wrapper.stats is codec_env.wrapper.stats

    def test_two_cached_reads_get_their_own_wrapper(
        self,
        codec_env: CachedEnv[Widget],
    ) -> None:
        assert codec_env.wrapper.stats is not codec_env.wrapper.total

    def test_construction_does_not_read_the_instance_attributes(
        self,
        cache_config: CacheConfig,
    ) -> None:
        repository = ExplodingAttributeRepository(_widgets(ROW_COUNT), Widget)

        wrap_with_cache(repository, cache_config)

        assert repository.property_reads == 0


class ExplodingAttributeRepository(CodecRepository[Widget]):
    """Double whose lazy attribute must not be evaluated at construction.

    A repository is free to expose a property that opens a connection or
    builds a session; walking the instance with ``getattr`` to find the cached
    reads would trigger it while the application is still booting.

    Attributes:
        property_reads: Number of times the lazy attribute was evaluated.
    """

    def __init__(self, rows: list[Widget], row_type: type[Widget]) -> None:
        super().__init__(rows, row_type)
        self.property_reads = 0

    @property
    def connection(self) -> str:
        """Attribute that counts, and reports, every evaluation."""
        self.property_reads += 1
        return "connected"


class TestResultContradictingItsAnnotation:
    """A read that does not survive the round trip fails on the first call."""

    @pytest.fixture
    def lying_env(self, cache_config: CacheConfig) -> CachedEnv[Widget]:
        return wrap_with_cache(LyingRepository(_widgets(ROW_COUNT), Widget), cache_config)

    async def test_a_result_the_declared_struct_rejects_raises(
        self,
        lying_env: CachedEnv[Widget],
    ) -> None:
        with pytest.raises(BoundaryValidationError):
            await lying_env.wrapper.wrong_shape()

    async def test_a_result_no_payload_can_describe_raises(
        self,
        lying_env: CachedEnv[Widget],
    ) -> None:
        with pytest.raises(TypeError):
            await lying_env.wrapper.unrenderable()

    @pytest.mark.parametrize(
        ("method_name", "error"),
        [("wrong_shape", BoundaryValidationError), ("unrenderable", TypeError)],
    )
    async def test_nothing_is_stored_so_the_next_call_reaches_the_repository(
        self,
        lying_env: CachedEnv[Widget],
        method_name: str,
        error: type[Exception],
    ) -> None:
        repository = lying_env.repository
        assert isinstance(repository, LyingRepository)
        method = getattr(lying_env.wrapper, method_name)

        for _attempt in range(2):
            with pytest.raises(error):
                await method()

        assert repository.custom_calls[method_name] == 2
        assert lying_env.backend.set_ttls == []


class TestSubclassOfTheDeclaredType:
    """The declared type is the contract: a subclass is narrowed to it."""

    @pytest.fixture
    def lying_env(self, cache_config: CacheConfig) -> CachedEnv[Widget]:
        return wrap_with_cache(LyingRepository(_widgets(ROW_COUNT), Widget), cache_config)

    async def test_both_calls_return_the_declared_type_not_the_subclass(
        self,
        lying_env: CachedEnv[Widget],
    ) -> None:
        first, second = await _call_twice(lying_env.wrapper, "narrowing")

        assert type(first) is Stats
        assert type(second) is Stats

    async def test_the_added_fields_are_dropped_on_both_calls(
        self,
        lying_env: CachedEnv[Widget],
    ) -> None:
        first, second = await _call_twice(lying_env.wrapper, "narrowing")

        assert first == Stats(total=ROW_COUNT, label="detail")
        assert second == Stats(total=ROW_COUNT, label="detail")


class TestPayloadWrittenByAnEarlierVersion:
    """A cached payload that no longer fits its type is a miss, not an outage."""

    @pytest.fixture
    async def warm_env(self, codec_env: CachedEnv[Widget]) -> CachedEnv[Widget]:
        await codec_env.wrapper.stats()
        return codec_env

    def _evolved(
        self,
        warm_env: CachedEnv[Widget],
        cache_config: CacheConfig,
    ) -> tuple[Wrapper, EvolvedRepository[Widget]]:
        """The later deployment, in front of the warm cache of the earlier one."""
        repository = EvolvedRepository(_widgets(ROW_COUNT), Widget)
        return rewrap_with_cache(warm_env, repository, cache_config), repository

    async def test_the_later_version_reads_the_key_the_earlier_one_wrote(
        self,
        warm_env: CachedEnv[Widget],
        cache_config: CacheConfig,
    ) -> None:
        wrapper, _repository = self._evolved(warm_env, cache_config)
        stored = set(warm_env.backend.data)

        await wrapper.stats()

        assert set(warm_env.backend.data) == stored

    async def test_the_undecodable_entry_does_not_reach_the_caller(
        self,
        warm_env: CachedEnv[Widget],
        cache_config: CacheConfig,
    ) -> None:
        wrapper, _repository = self._evolved(warm_env, cache_config)

        assert await wrapper.stats() == EvolvedStats(total=ROW_COUNT, label="all", source="v2")

    async def test_the_stale_entry_is_overwritten_so_the_next_call_hits(
        self,
        warm_env: CachedEnv[Widget],
        cache_config: CacheConfig,
    ) -> None:
        wrapper, repository = self._evolved(warm_env, cache_config)

        first, second = await wrapper.stats(), await wrapper.stats()

        assert first == second
        assert repository.custom_calls["stats"] == 1


class TestUnmarkedOverride:
    """An override that drops the marker takes the name out of the cache."""

    @pytest.fixture
    def override_env(self, cache_config: CacheConfig) -> CachedEnv[Widget]:
        repository = UnmarkedOverrideRepository(_widgets(ROW_COUNT), Widget)

        return wrap_with_cache(repository, cache_config)

    def test_the_base_annotation_of_an_overridden_read_is_not_refused(
        self,
        override_env: CachedEnv[Widget],
    ) -> None:
        assert override_env.wrapper.out_of_grammar is not None

    async def test_the_overridden_read_is_not_cached(
        self,
        override_env: CachedEnv[Widget],
    ) -> None:
        repository = override_env.repository
        assert isinstance(repository, UnmarkedOverrideRepository)

        await _call_twice(override_env.wrapper, "stats")

        assert repository.custom_calls["stats"] == 2


def _returns_non_optional_union() -> Stats | int:
    raise NotImplementedError


def _returns_nested_generic() -> list[list[Stats]]:
    raise NotImplementedError


def _returns_bare_list() -> list:
    raise NotImplementedError


def _returns_sequence() -> Sequence[Stats]:
    raise NotImplementedError


def _returns_any() -> Any:
    raise NotImplementedError


def _returns_optional_list() -> list[Stats] | None:
    raise NotImplementedError


class TestGrammarBoundaries:
    """Where the grammar stops, asserted on the builder rather than end to end."""

    @pytest.mark.parametrize(
        "method",
        [
            _returns_non_optional_union,
            _returns_nested_generic,
            _returns_bare_list,
            _returns_sequence,
            _returns_any,
        ],
    )
    def test_an_annotation_outside_the_grammar_gets_no_codec(
        self,
        method: Any,
    ) -> None:
        assert build_result_codec(method, model=Widget) is None

    def test_an_optional_list_of_structs_is_inside_the_grammar(self) -> None:
        codec = build_result_codec(_returns_optional_list, model=Widget)

        assert codec is not None
        assert codec.decode([{"total": 1, "label": "a"}]) == [Stats(total=1, label="a")]
        assert codec.decode(None) is None
