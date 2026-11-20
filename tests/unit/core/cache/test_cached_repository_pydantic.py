"""``CachedRepository`` round-trips a pydantic-model row through the cache.

Every earlier suite only wraps a repository whose ``model`` is a loom
``msgspec.Struct``. Here the wrapped repository's ``model`` is a strict
``pydantic.BaseModel`` and declares no ``to_output_from_payload`` builder, so
the miss path's ``to_payload`` renders the instance as builtins before it
reaches the backend and a cache hit's builtins payload decodes back through
the pydantic-backed ``LoomType`` the wrapper resolves once in ``__init__``
(``self._pydantic_output_type``) rather than through ``to_struct``.

``TestToPayload`` tests ``to_payload`` on its own: a pydantic instance, a
list of them, a ``msgspec.Struct`` and an arbitrary object it does not
describe.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.cache import CacheConfig, CachedRepository, GenerationalDependencyResolver
from loom.core.cache.result_codec import to_payload
from loom.core.stores.repository import FilterParams, PageParams, PageResult

from ._doubles import CountingCacheStore

pydantic = pytest.importorskip("pydantic")


class AcmeRepositoryPydanticDoc(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    name: str


class PydanticRowRepository:
    """Minimal read-only repository double whose model is a pydantic ``BaseModel``."""

    entity_name = "docs"

    def __init__(self, rows: dict[int, AcmeRepositoryPydanticDoc]) -> None:
        self.model = AcmeRepositoryPydanticDoc
        self.storage = dict(rows)
        self.get_by_id_calls = 0

    async def get_by_id(
        self, obj_id: int, profile: str = "default"
    ) -> AcmeRepositoryPydanticDoc | None:
        self.get_by_id_calls += 1
        return self.storage.get(obj_id)

    async def get_by(
        self, field: str, value: Any, profile: str = "default"
    ) -> AcmeRepositoryPydanticDoc | None:
        raise NotImplementedError

    async def exists_by(self, field: str, value: Any) -> bool:
        raise NotImplementedError

    async def count(self) -> int:
        raise NotImplementedError

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[AcmeRepositoryPydanticDoc]:
        raise NotImplementedError


def _wrap(
    repository: PydanticRowRepository,
) -> tuple[CachedRepository[Any, Any, Any, Any], CountingCacheStore]:
    backend = CountingCacheStore()
    resolver = GenerationalDependencyResolver(backend)
    wrapper: CachedRepository[Any, Any, Any, Any] = CachedRepository(
        repository,
        config=CacheConfig(enabled=True, default_ttl=100, default_list_ttl=50),
        cache=backend,
        dependency_resolver=resolver,
    )
    return wrapper, backend


class TestPydanticModelRoundTrip:
    """``get_by_id`` round-trips a pydantic model through the real miss and hit paths."""

    async def test_a_miss_then_a_hit_return_an_equal_model_and_load_once(self) -> None:
        doc = AcmeRepositoryPydanticDoc(id=1, name="a")
        repository = PydanticRowRepository({1: doc})
        wrapper, _backend = _wrap(repository)

        first = await wrapper.get_by_id(1)
        second = await wrapper.get_by_id(1)

        assert first == doc
        assert second == doc
        assert isinstance(second, AcmeRepositoryPydanticDoc)
        assert repository.get_by_id_calls == 1

    async def test_a_subclass_instance_with_an_extra_field_is_cached_in_the_declared_shape(
        self,
    ) -> None:
        """A subclass instance is stored through the declared model's own fields.

        Otherwise the second call would decode ``extra_field`` back at the
        declared strict model and fail with ``BoundaryValidationError``.
        """

        class SubPydanticDoc(AcmeRepositoryPydanticDoc):
            extra_field: str = "unused"

        doc = SubPydanticDoc(id=1, name="a")
        repository = PydanticRowRepository({1: doc})  # type: ignore[dict-item]
        wrapper, _backend = _wrap(repository)

        first = await wrapper.get_by_id(1)
        second = await wrapper.get_by_id(1)

        assert first == doc
        assert second == AcmeRepositoryPydanticDoc(id=1, name="a")
        assert type(second) is AcmeRepositoryPydanticDoc
        assert repository.get_by_id_calls == 1


class AcmePayloadStructDoc(msgspec.Struct):
    """Struct instance, still rendered through ``msgspec.to_builtins``."""

    name: str


class TestToPayload:
    """``to_payload`` renders every shape it knows and leaves the rest untouched."""

    def test_a_pydantic_instance_renders_as_builtins(self) -> None:
        doc = AcmeRepositoryPydanticDoc(id=1, name="a")

        assert to_payload(doc) == {"id": 1, "name": "a"}

    def test_a_list_of_pydantic_instances_renders_as_builtins(self) -> None:
        docs = [
            AcmeRepositoryPydanticDoc(id=1, name="a"),
            AcmeRepositoryPydanticDoc(id=2, name="b"),
        ]

        assert to_payload(docs) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_a_struct_renders_as_builtins(self) -> None:
        assert to_payload(AcmePayloadStructDoc(name="a")) == {"name": "a"}

    def test_an_arbitrary_object_is_returned_untouched(self) -> None:
        sentinel = object()

        assert to_payload(sentinel) is sentinel
