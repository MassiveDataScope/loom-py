from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import msgspec
import pytest

from loom.core.model.projection import (
    PROJECTION_DEFAULT_MISSING,
    Projection,
)
from loom.core.projection.plan import build_projection_plan, execute_projection_plan


@dataclass
class _Obj:
    id: int
    value: int
    rel: list[int]


class _StructObj(msgspec.Struct):
    id: int
    rel: list[int]


@dataclass(frozen=True, slots=True)
class _BackendLoader:
    async def load_many(
        self,
        backend_context: Any,
        parent_ids: Sequence[object],
    ) -> dict[object, Any]:
        await asyncio.sleep(0)
        _ = backend_context
        result: dict[object, Any] = {}
        for parent_id in parent_ids:
            assert isinstance(parent_id, int)
            result[parent_id] = parent_id * 2
        return result


@dataclass(frozen=True, slots=True)
class _EmptyBackendLoader:
    async def load_many(
        self,
        backend_context: Any,
        parent_ids: Sequence[object],
    ) -> dict[object, Any]:
        await asyncio.sleep(0)
        _ = backend_context
        _ = parent_ids
        return {}


@dataclass(frozen=True, slots=True)
class _MemoryDependsOnBackend:
    relation: str = "rel"

    def load_from_object(
        self,
        obj: Any,
        context: dict[str, Any] | None = None,
    ) -> int:
        if context is None:
            return -1
        backend_value = int(context["base"])
        return backend_value + len(getattr(obj, self.relation))


@dataclass(frozen=True, slots=True)
class _HybridLoader:
    relation: str = "rel"

    async def load_many(
        self,
        backend_context: Any,
        parent_ids: Sequence[object],
    ) -> dict[object, Any]:
        await asyncio.sleep(0)
        _ = backend_context
        result: dict[object, Any] = {}
        for parent_id in parent_ids:
            assert isinstance(parent_id, int)
            result[parent_id] = 100 + parent_id
        return result

    def load_from_object(
        self,
        obj: Any,
        context: dict[str, Any] | None = None,
    ) -> int:
        _ = context
        return 10 + len(getattr(obj, self.relation))


class TestProjectionRuntime:
    @pytest.mark.asyncio
    async def test_runtime_respects_projection_dependencies(self) -> None:
        projections = {
            "base": Projection(loader=_BackendLoader()),
            "derived": Projection(
                loader=_MemoryDependsOnBackend(),
                depends_on=("projection:base",),
            ),
        }
        plan = build_projection_plan(projections)

        values = await execute_projection_plan(
            plan,
            objs=[_Obj(id=1, value=10, rel=[1, 2]), _Obj(id=2, value=20, rel=[1])],
            id_attr="id",
            backend_context=object(),
        )

        assert values[0]["base"] == 2
        assert values[0]["derived"] == 4
        assert values[1]["base"] == 4
        assert values[1]["derived"] == 5

    @pytest.mark.asyncio
    async def test_runtime_omits_value_when_default_not_defined(self) -> None:
        projections = {
            "score": Projection(
                loader=_EmptyBackendLoader(),
                default=PROJECTION_DEFAULT_MISSING,
            )
        }
        plan = build_projection_plan(projections)
        values = await execute_projection_plan(
            plan,
            objs=[_Obj(id=1, value=0, rel=[])],
            id_attr="id",
            backend_context=object(),
        )
        assert "score" not in values[0]

    def test_runtime_detects_projection_cycles(self) -> None:
        projections = {
            "a": Projection(
                loader=_MemoryDependsOnBackend(),
                depends_on=("projection:b",),
            ),
            "b": Projection(
                loader=_MemoryDependsOnBackend(),
                depends_on=("projection:a",),
            ),
        }

        with pytest.raises(ValueError, match="cycle"):
            build_projection_plan(projections)

    @pytest.mark.asyncio
    async def test_runtime_backend_loader_requires_backend_context(self) -> None:
        plan = build_projection_plan({"base": Projection(loader=_BackendLoader())})
        objs = [_Obj(id=1, value=0, rel=[])]
        with pytest.raises(RuntimeError, match="requires backend context"):
            await execute_projection_plan(
                plan,
                objs=objs,
                id_attr="id",
                backend_context=None,
            )

    @pytest.mark.asyncio
    async def test_runtime_memory_loader_used_when_load_from_object_present(self) -> None:
        plan = build_projection_plan({"score": Projection(loader=_HybridLoader())})
        values = await execute_projection_plan(
            plan,
            objs=[_Obj(id=1, value=0, rel=[1, 2])],
            id_attr="id",
            backend_context=object(),
        )
        assert values[0]["score"] == 12

    @pytest.mark.asyncio
    async def test_runtime_memory_loader_works_for_msgspec_struct(self) -> None:
        plan = build_projection_plan({"score": Projection(loader=_HybridLoader())})
        values = await execute_projection_plan(
            plan,
            objs=[_StructObj(id=1, rel=[1, 2])],
            id_attr="id",
            backend_context=object(),
        )
        assert values[0]["score"] == 12
