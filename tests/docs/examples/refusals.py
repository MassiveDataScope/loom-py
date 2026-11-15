"""Bindings that the kernel refuses when it starts."""

from __future__ import annotations

from typing import Any, Protocol

from loom.core.di.container import LoomContainer
from loom.core.use_case import UseCase
from loom.core.use_case.factory import UseCaseFactory


class ReminderClock(Protocol):
    """A port no module binds."""

    def now(self) -> float: ...


class ScheduleReminder(UseCase[Any, float]):
    """Declares a port nothing serves."""

    def __init__(self, clock: ReminderClock) -> None:
        super().__init__()
        self._clock = clock

    async def execute(self) -> float:
        return self._clock.now()


def factory_module(container: LoomContainer) -> None:
    """Binds a key the kernel owns."""
    container.register_instance(UseCaseFactory, object())
