"""Doubles shared by the REST unit tests."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.observability.event import LifecycleEvent
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase


class CreateUserUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class ListUsersUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class RecordingLifecycleObserver:
    """Captures every lifecycle event emitted through the observability runtime."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


def compile_use_cases(
    *use_case_types: type[UseCase[Any, Any]],
) -> tuple[UseCaseCompiler, UseCaseRegistry]:
    compiler = UseCaseCompiler()
    for uc in use_case_types:
        compiler.compile(uc)
    registry = UseCaseRegistry.build(list(use_case_types))
    return compiler, registry


def route_paths(app: FastAPI) -> set[str]:
    return {str(getattr(route, "path", "")) for route in app.routes}
