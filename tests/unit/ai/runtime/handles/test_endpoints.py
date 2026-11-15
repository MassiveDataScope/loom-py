"""No HTTP or A2A endpoint opens an execution: the runtime opens one per agent run."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.helpers.source_tree import SRC, parsed

_ENDPOINTS = tuple(
    sorted((*(SRC / "ai" / "fastapi").glob("*.py"), *(SRC / "ai" / "a2a").glob("*.py")))
)
_OPENERS = frozenset({"ExecutionScope", "entered"})


def _openers_called(tree: ast.AST) -> list[str]:
    called = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
        if name in _OPENERS:
            called.append(name)
    return called


class TestEndpointModules:
    @pytest.mark.parametrize("module", _ENDPOINTS, ids=[path.stem for path in _ENDPOINTS])
    def test_open_no_execution(self, module: Path) -> None:
        assert _openers_called(parsed(module)) == []

    def test_the_check_reads_every_endpoint_module(self) -> None:
        stems = {path.stem for path in _ENDPOINTS}

        assert {"endpoints", "streaming", "_handlers", "server"} <= stems

    @pytest.mark.parametrize(
        "source",
        [
            "ExecutionScope(caller)",
            "execution.ExecutionScope(caller)",
            "ExecutionContext.entered(caller)",
        ],
        ids=["plain", "qualified", "entered"],
    )
    def test_the_check_sees_every_way_to_open_one(self, source: str) -> None:
        assert _openers_called(ast.parse(source)) != []
