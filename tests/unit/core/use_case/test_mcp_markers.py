"""The AI-free scan for declared ``Mcp()`` bindings (S3).

A composition root must know whether any compiled use case declares a
marker before deciding whether to import the AI pillar at all — importing
it just to discover nothing to check would be the containment leak a
deployment with no ``ai:`` section is meant to avoid.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.markers import Mcp
from loom.core.use_case.mcp_markers import declaring_mcp_bindings
from loom.core.use_case.use_case import UseCase

_SRC = Path(__file__).resolve().parents[4] / "src"


class DocsSearchUseCase(UseCase[object, str]):
    """Declares one Mcp() marker."""

    async def execute(self, docs: Any = Mcp("docs-server", include=["search"])) -> str:
        return docs.server


class TwoServersUseCase(UseCase[object, str]):
    """Two distinct named MCP servers in one signature."""

    async def execute(
        self,
        first: Any = Mcp("docs-server", include=["search"]),
        second: Any = Mcp("billing-server", include=["lookup"]),
    ) -> str:
        return f"{first.server}:{second.server}"


class NoMcpUseCase(UseCase[object, str]):
    """Declares no Mcp() marker at all."""

    async def execute(self) -> str:
        return "no servers here"


class TestMcpMarkers:
    def test_returns_only_declaring_use_cases_in_input_order(self) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(NoMcpUseCase)
        compiler.compile(DocsSearchUseCase)
        compiler.compile(TwoServersUseCase)

        result = declaring_mcp_bindings(
            [NoMcpUseCase, DocsSearchUseCase, TwoServersUseCase], compiler
        )

        assert [uc_type for uc_type, _ in result] == [DocsSearchUseCase, TwoServersUseCase]

    def test_carries_each_use_cases_own_bindings(self) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(DocsSearchUseCase)
        compiler.compile(TwoServersUseCase)

        result = declaring_mcp_bindings([DocsSearchUseCase, TwoServersUseCase], compiler)

        servers = {uc_type: tuple(b.server for b in bindings) for uc_type, bindings in result}
        assert servers[DocsSearchUseCase] == ("docs-server",)
        assert servers[TwoServersUseCase] == ("docs-server", "billing-server")

    def test_a_use_case_never_compiled_is_absent_even_when_listed(self) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(DocsSearchUseCase)

        result = declaring_mcp_bindings([DocsSearchUseCase, NoMcpUseCase], compiler)

        assert [uc_type for uc_type, _ in result] == [DocsSearchUseCase]

    def test_empty_when_no_use_case_declares_a_marker(self) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(NoMcpUseCase)

        result = declaring_mcp_bindings([NoMcpUseCase], compiler)

        assert result == []


_SCRIPT = """
import json
import sys

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.markers import Mcp
from loom.core.use_case.mcp_markers import declaring_mcp_bindings
from loom.core.use_case.use_case import UseCase


class DocsSearchUseCase(UseCase[object, str]):
    async def execute(self, docs=Mcp("docs-server", include=["search"])) -> str:
        return docs.server


compiler = UseCaseCompiler()
compiler.compile(DocsSearchUseCase)
declaring_mcp_bindings([DocsSearchUseCase], compiler)

leaked = [
    name for name in sys.modules if name == "loom.ai" or name.startswith("loom.ai.")
]
print(json.dumps(sorted(leaked)))
"""


def _run_in_clean_interpreter(script: str) -> subprocess.CompletedProcess[str]:
    """Run ``script`` in a fresh interpreter that can see the repository ``src``."""
    env = {**os.environ, "PYTHONPATH": str(_SRC)}
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


class TestMcpMarkersImportContainment:
    def test_the_module_imports_nothing_from_loom_ai(self) -> None:
        """Importing this module and calling ``declaring_mcp_bindings`` must not
        pull in ``loom.ai``.

        Runs in a clean subprocess, following the precedent set by
        ``tests/unit/core/plugins/test_import_containment.py`` and
        ``tests/unit/core/cache/test_cache_import_containment.py``: the pytest
        interpreter running this suite has already imported ``loom.ai`` (and
        everything it depends on) for other suites, so those modules stay
        cached regardless of what this module imports. Evicting only
        ``loom.ai*`` and ``mcp_markers`` from ``sys.modules`` and reimporting
        would leave any transitive dependency (e.g. ``loom.core.engine.compiler``
        hiding an ``import loom.ai``) cached and unexecuted, so that leak would
        go undetected. A subprocess starts with none of that baggage, so the
        check catches a leak anywhere in the import chain, not only in this
        module's own top-level statements.
        """
        result = _run_in_clean_interpreter(_SCRIPT)
        assert result.returncode == 0, result.stderr

        leaked = json.loads(result.stdout.strip().splitlines()[-1])
        assert leaked == [], f"loom.ai modules leaked in: {leaked}"
