"""A repository double that records every call and can fail each one.

:class:`RecordingRepository` wraps any repository, :class:`~loom.testing.InMemoryRepository`
or a real store, so a test asserts what its caller sent and drives the caller's
reject, retry and exit paths without a double of its own.

Usage::

    readings = RecordingRepository(InMemoryRepository(Reading), fails_with=Conflict("taken"))

    await writer.write(readings, batch)

    sent = {"data": tuple(batch), "on_conflict": OnConflict.ERROR}
    assert readings.calls == [RepositoryCall("create_many", sent)]
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, TypeAlias, TypeVar, cast

import msgspec

from loom.core.stores.repository import (
    BulkCreatable,
    Countable,
    Creatable,
    CursorResult,
    Deletable,
    FieldReadable,
    FilterParams,
    Listable,
    OnConflict,
    PageParams,
    PageResult,
    QuerySpec,
    Readable,
    RepoFor,
    Updatable,
)

T = TypeVar("T", bound=msgspec.Struct)

AnyRepository: TypeAlias = (
    Readable[T]
    | FieldReadable[T]
    | Creatable[T]
    | BulkCreatable[T]
    | Updatable[T]
    | Deletable[T]
    | Listable[T]
    | Countable[T]
)
"""A repository of ``T`` with at least one standard capability."""


@dataclass(frozen=True, slots=True)
class RepositoryCall:
    """One call a :class:`RecordingRepository` received.

    Attributes:
        method: The port method called, as ``"create_many"``.
        arguments: Its arguments by parameter name, defaults included; the
            ``data`` of ``create_many`` is a tuple, copied when the call is made.
    """

    method: str
    arguments: Mapping[str, Any] = field(default_factory=dict)


class RecordingRepository(RepoFor[T], Generic[T]):
    """Record each call of the repository port, then forward it or fail it.

    Every method of :class:`~loom.core.stores.repository.RepoFor` appends a
    :class:`RepositoryCall` to :attr:`calls` before anything else, so a call
    that fails is recorded too. With ``fails_with`` every call raises that
    error and the wrapped repository is not called; without it the call goes
    to the wrapped repository's method of the same name, and a method the
    wrapped repository lacks raises ``AttributeError`` there.

    The class declares every standard capability, so a harness binds it under
    all of them whatever the wrapped repository implements.

    Args:
        inner: The repository each call is forwarded to.
        fails_with: The error every call raises, after it is recorded.

    Attributes:
        calls: The calls received, in order.
    """

    def __init__(self, inner: AnyRepository[T], *, fails_with: Exception | None = None) -> None:
        self._inner = cast(RepoFor[T], inner)
        self._fails_with = fails_with
        self.calls: list[RepositoryCall] = []

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> T | None:
        """Record the call, then forward it."""
        self._receive("get_by_id", obj_id=obj_id, profile=profile)
        return await self._inner.get_by_id(obj_id, profile)

    async def get_by(self, field: str, value: Any, profile: str = "default") -> T | None:
        """Record the call, then forward it."""
        self._receive("get_by", field=field, value=value, profile=profile)
        return await self._inner.get_by(field, value, profile)

    async def exists_by(self, field: str, value: Any) -> bool:
        """Record the call, then forward it."""
        self._receive("exists_by", field=field, value=value)
        return await self._inner.exists_by(field, value)

    async def count(self) -> int:
        """Record the call, then forward it."""
        self._receive("count")
        return await self._inner.count()

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[T]:
        """Record the call, then forward it."""
        self._receive(
            "list_paginated",
            page_params=page_params,
            filter_params=filter_params,
            profile=profile,
        )
        return await self._inner.list_paginated(page_params, filter_params, profile)

    async def list_with_query(
        self, query: QuerySpec, profile: str = "default"
    ) -> PageResult[T] | CursorResult[T]:
        """Record the call, then forward it."""
        self._receive("list_with_query", query=query, profile=profile)
        return await self._inner.list_with_query(query, profile)

    async def create(self, data: msgspec.Struct) -> T:
        """Record the call, then forward it."""
        self._receive("create", data=data)
        return await self._inner.create(data)

    async def create_many(
        self, data: Sequence[msgspec.Struct], *, on_conflict: OnConflict = OnConflict.ERROR
    ) -> tuple[T, ...]:
        """Record the call with a copy of *data*, then forward it."""
        self._receive("create_many", data=tuple(data), on_conflict=on_conflict)
        return await self._inner.create_many(data, on_conflict=on_conflict)

    async def update(self, obj_id: Any, data: msgspec.Struct) -> T | None:
        """Record the call, then forward it."""
        self._receive("update", obj_id=obj_id, data=data)
        return await self._inner.update(obj_id, data)

    async def delete(self, obj_id: Any) -> bool:
        """Record the call, then forward it."""
        self._receive("delete", obj_id=obj_id)
        return await self._inner.delete(obj_id)

    def _receive(self, method: str, **arguments: Any) -> None:
        """Record the call, then raise ``fails_with`` when one is set."""
        self.calls.append(RepositoryCall(method, arguments))
        if self._fails_with is not None:
            raise self._fails_with
