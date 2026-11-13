"""Fixtures of the characterisation: the executors a path constructs."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.executor import RuntimeExecutor
from tests.helpers.composition import fixture_project

__all__ = ["executors", "fixture_project"]


@pytest.fixture
def executors(monkeypatch: pytest.MonkeyPatch) -> list[RuntimeExecutor]:
    """Every ``RuntimeExecutor`` constructed during the test, in order."""
    built: list[RuntimeExecutor] = []
    construct = RuntimeExecutor.__init__

    def counted(self: RuntimeExecutor, *args: Any, **kwargs: Any) -> None:
        built.append(self)
        construct(self, *args, **kwargs)

    monkeypatch.setattr(RuntimeExecutor, "__init__", counted)
    return built
