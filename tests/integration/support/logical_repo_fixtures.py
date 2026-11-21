from __future__ import annotations

import asyncio
from typing import Protocol

from loom.core.response import Response
from loom.core.stores.repository import repository_for
from loom.core.use_case.use_case import UseCase


class TaskView(Response, frozen=True, kw_only=True):  # type: ignore[misc]
    task_id: str
    state: str


class TaskViewRepo(Protocol):
    async def get_by_id(self, obj_id: str, profile: str = "default") -> TaskView | None: ...


@repository_for(TaskView)
class TaskViewRepository(TaskViewRepo):
    def __init__(self) -> None:
        self._items = {
            "t-1": TaskView(task_id="t-1", state="done"),
        }

    async def get_by_id(self, obj_id: str, profile: str = "default") -> TaskView | None:
        _ = profile
        return await asyncio.sleep(0, result=self._items.get(obj_id))


class GetTaskViewUseCase(UseCase[TaskView, TaskView | None, TaskViewRepo]):
    async def execute(self, task_id: str) -> TaskView | None:
        return await self.main_repo.get_by_id(task_id)
