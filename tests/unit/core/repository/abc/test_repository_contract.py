from __future__ import annotations

import asyncio

import msgspec


class AcmeRepositoryOutput(msgspec.Struct):
    id: int
    name: str


class DummyCreate(msgspec.Struct):
    id: int
    name: str


class DummyUpdate(msgspec.Struct, kw_only=True):
    name: str | msgspec.UnsetType = msgspec.UNSET


class InMemoryRepository:
    def __init__(self) -> None:
        self._items: dict[int, AcmeRepositoryOutput] = {}

    async def create(self, data: DummyCreate) -> AcmeRepositoryOutput:
        obj = AcmeRepositoryOutput(id=data.id, name=data.name)
        self._items[obj.id] = obj
        return obj

    async def get_by_id(self, obj_id: int, profile: str = "default") -> AcmeRepositoryOutput | None:
        _ = profile
        return self._items.get(obj_id)

    async def update(self, obj_id: int, data: DummyUpdate) -> AcmeRepositoryOutput | None:
        existing = self._items.get(obj_id)
        if existing is None:
            return None
        fields = msgspec.to_builtins(data)
        obj = AcmeRepositoryOutput(id=obj_id, name=fields.get("name", existing.name))
        self._items[obj_id] = obj
        return obj

    async def delete(self, obj_id: int) -> bool:
        return self._items.pop(obj_id, None) is not None


class TestRepositoryContractExample:
    def test_struct_contract_flow(self) -> None:
        async def _run() -> None:
            repo = InMemoryRepository()
            created = await repo.create(DummyCreate(id=7, name="original"))
            assert created.id == 7
            assert created.name == "original"

            loaded = await repo.get_by_id(7)
            assert loaded is not None
            assert loaded.id == 7

            updated = await repo.update(7, DummyUpdate(name="changed"))
            assert updated is not None
            assert updated.id == 7
            assert updated.name == "changed"

            deleted = await repo.delete(7)
            assert deleted is True

        asyncio.run(_run())
