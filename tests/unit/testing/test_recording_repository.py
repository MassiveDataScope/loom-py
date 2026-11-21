"""RecordingRepository: what a caller sent, and every call failing on demand."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

import msgspec
import pytest

from loom.core.errors import Conflict, UnsupportedQuery
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.repository import OnConflict, PageParams, QuerySpec, RepoFor
from loom.testing import InMemoryRepository, RecordingRepository, RepositoryCall


class _Reading(BaseModel):
    __tablename__ = "recording_readings"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    value: int = ColumnField()


class _NewReading(msgspec.Struct):
    value: int


class _StoreDown(Exception):
    pass


class _AnswersWithItsName:
    """Wrapped repository whose every method answers with its own name."""

    def __getattr__(self, method: str) -> Callable[..., Awaitable[str]]:
        async def answer(*args: Any, **kwargs: Any) -> str:
            return method

        return answer


_PORT_METHODS = frozenset(
    name
    for protocol in inspect.getmro(RepoFor)
    for name, member in vars(protocol).items()
    if inspect.iscoroutinefunction(member)
)


_FIRST_PAGE = PageParams(page=1, limit=10)
_QUERY = QuerySpec()
_PORT_CALLS: dict[str, tuple[tuple[Any, ...], RepositoryCall]] = {
    "get_by_id": ((1,), RepositoryCall("get_by_id", {"obj_id": 1, "profile": "default"})),
    "get_by": (
        ("value", 3),
        RepositoryCall("get_by", {"field": "value", "value": 3, "profile": "default"}),
    ),
    "exists_by": (("value", 3), RepositoryCall("exists_by", {"field": "value", "value": 3})),
    "count": ((), RepositoryCall("count")),
    "list_paginated": (
        (_FIRST_PAGE,),
        RepositoryCall(
            "list_paginated",
            {"page_params": _FIRST_PAGE, "filter_params": None, "profile": "default"},
        ),
    ),
    "list_with_query": (
        (_QUERY,),
        RepositoryCall("list_with_query", {"query": _QUERY, "profile": "default"}),
    ),
    "create": ((_NewReading(1),), RepositoryCall("create", {"data": _NewReading(1)})),
    "create_many": (
        ([_NewReading(1)],),
        RepositoryCall("create_many", {"data": (_NewReading(1),), "on_conflict": OnConflict.ERROR}),
    ),
    "update": (
        (1, _NewReading(2)),
        RepositoryCall("update", {"obj_id": 1, "data": _NewReading(2)}),
    ),
    "delete": ((1,), RepositoryCall("delete", {"obj_id": 1})),
}


@pytest.fixture
def readings() -> InMemoryRepository[_Reading]:
    return InMemoryRepository(_Reading)


@pytest.fixture
def recorder(readings: InMemoryRepository[_Reading]) -> RecordingRepository[_Reading]:
    return RecordingRepository(readings)


class TestRecording:
    def test_nothing_is_recorded_before_a_call(
        self, recorder: RecordingRepository[_Reading]
    ) -> None:
        assert recorder.calls == []

    @pytest.mark.asyncio
    async def test_calls_are_recorded_in_order_with_the_rows_sent(
        self, recorder: RecordingRepository[_Reading]
    ) -> None:
        batch = [_NewReading(1), _NewReading(2)]

        await recorder.create_many(batch, on_conflict=OnConflict.IGNORE)
        await recorder.get_by_id(1)

        assert recorder.calls == [
            RepositoryCall("create_many", {"data": tuple(batch), "on_conflict": OnConflict.IGNORE}),
            RepositoryCall("get_by_id", {"obj_id": 1, "profile": "default"}),
        ]

    @pytest.mark.asyncio
    async def test_a_batch_changed_after_the_call_keeps_the_rows_sent(
        self, recorder: RecordingRepository[_Reading]
    ) -> None:
        batch = [_NewReading(1)]

        await recorder.create_many(batch)
        batch.append(_NewReading(2))

        assert recorder.calls[0].arguments["data"] == (_NewReading(1),)

    @pytest.mark.asyncio
    async def test_a_call_is_forwarded_to_the_wrapped_repository(
        self, recorder: RecordingRepository[_Reading], readings: InMemoryRepository[_Reading]
    ) -> None:
        created = await recorder.create(_NewReading(5))

        assert await readings.get_by_id(created.id) == _Reading(id=created.id, value=5)


class TestFailure:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [Conflict("taken"), UnsupportedQuery("no such filter"), _StoreDown("unreachable")],
        ids=["conflict", "unsupported-query", "store-down"],
    )
    async def test_every_call_raises_the_error_and_is_recorded(
        self, readings: InMemoryRepository[_Reading], error: Exception
    ) -> None:
        recorder = RecordingRepository(readings, fails_with=error)

        for _ in range(2):
            with pytest.raises(type(error)):
                await recorder.create_many([_NewReading(1)])

        assert [call.method for call in recorder.calls] == ["create_many", "create_many"]

    @pytest.mark.asyncio
    async def test_a_failed_call_never_reaches_the_wrapped_repository(
        self, readings: InMemoryRepository[_Reading]
    ) -> None:
        recorder = RecordingRepository(readings, fails_with=_StoreDown("unreachable"))

        with pytest.raises(_StoreDown):
            await recorder.create(_NewReading(1))

        assert await readings.get_by_id(1) is None


class TestPortCoverage:
    def test_the_table_names_every_port_method(self) -> None:
        assert set(_PORT_CALLS) == _PORT_METHODS

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", sorted(_PORT_CALLS))
    async def test_each_port_method_is_recorded_and_forwarded(self, method: str) -> None:
        recorder: RecordingRepository[Any] = RecordingRepository(_AnswersWithItsName())  # type: ignore[arg-type]
        args, expected = _PORT_CALLS[method]

        answer = await getattr(recorder, method)(*args)

        assert (answer, recorder.calls) == (method, [expected])
