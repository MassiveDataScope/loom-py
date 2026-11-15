"""The ``Mcp()`` marker at compile time: the binding only, not its resolution.

Mirrors the compilation half of ``test_agent_marker.py``. Resolution (an
executor callable that turns the binding into a handle) is PR3's scope, not
this one: this compiler runs whether or not the AI pillar is installed, so it
cannot validate the server name either — the same reason ``_collect_agent``
gives.
"""

from __future__ import annotations

from typing import Any

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.markers import Mcp
from loom.core.use_case.use_case import UseCase


class DocsSearchUseCase(UseCase[object, str]):
    """Declares one Mcp() marker."""

    async def execute(
        self,
        docs: Any = Mcp("docs-server", include=["search"]),
    ) -> str:
        return docs.server


class TwoServersUseCase(UseCase[object, str]):
    """Two distinct named MCP servers in one signature."""

    async def execute(
        self,
        first: Any = Mcp("docs-server", include=["search"]),
        second: Any = Mcp("billing-server", include=["lookup", "refund"]),
    ) -> str:
        return f"{first.server}:{second.server}"


class NoMcpUseCase(UseCase[object, str]):
    """Declares no Mcp() marker at all."""

    async def execute(self) -> str:
        return "no servers here"


class TestMcpMarkerCompilation:
    def test_the_marker_compiles_into_an_mcp_binding(self) -> None:
        plan = UseCaseCompiler().compile(DocsSearchUseCase)

        assert [mb.name for mb in plan.mcp_bindings] == ["docs"]
        assert plan.mcp_bindings[0].server == "docs-server"
        assert plan.mcp_bindings[0].include == ("search",)

    def test_the_mcp_parameter_is_not_a_primitive_parameter(self) -> None:
        """It must not fall through to ``ParamBinding``, or callers would supply it."""
        plan = UseCaseCompiler().compile(DocsSearchUseCase)

        assert [pb.name for pb in plan.param_bindings] == []

    def test_two_markers_in_one_signature_compile_in_declaration_order(self) -> None:
        plan = UseCaseCompiler().compile(TwoServersUseCase)

        assert [(mb.name, mb.server, mb.include) for mb in plan.mcp_bindings] == [
            ("first", "docs-server", ("search",)),
            ("second", "billing-server", ("lookup", "refund")),
        ]

    def test_a_plan_with_no_marker_keeps_mcp_bindings_empty(self) -> None:
        plan = UseCaseCompiler().compile(NoMcpUseCase)

        assert plan.mcp_bindings == ()
