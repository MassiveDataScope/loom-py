"""A shared MCP session that died after it opened serves the next call again.

The worker opens one session per server at start-up and every run and every
``Mcp()`` marker speaks through it. When the server behind it goes away after
that -- a transport error under an in-flight call, a redeploy, a child that
exits -- the holder the host keeps open also keeps the dead client from ever
being re-entered, so every later call failed until the process was restarted.
Driven over a real stdio child, which the test can kill.
"""

from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

import pytest

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from tests.integration.ai.conftest import _until

pytest.importorskip("fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests")

_SCRIPT = str(Path(__file__).parent / "mcp_stdio_server.py")


def _capability(pid_file: Path) -> CompiledMcpCapability:
    return CompiledMcpCapability(
        server="orders",
        transport="stdio",
        command=sys.executable,
        args=(_SCRIPT, str(pid_file), "recovery"),
        timeout_ms=5_000,
    )


def _kill(pid_file: Path) -> int:
    pid = int(pid_file.read_text(encoding="utf-8"))
    os.kill(pid, signal.SIGKILL)
    return pid


class TestASessionThatDiedAfterOpening:
    async def test_the_call_after_the_failed_one_is_served_by_a_new_session(
        self, tmp_path: Path
    ) -> None:
        pid_file = tmp_path / "server.pid"
        shared = SharedMcpToolsets()

        async with shared.open(_capability(pid_file)) as session:
            first = await session.call_tool("read_orders", {"customer": "acme"})
            assert first.ok

            dead = _kill(pid_file)
            pid_file.unlink()
            with pytest.raises(Exception):  # noqa: B017, PT011 -- whatever the dead transport raises
                await session.call_tool("read_orders", {"customer": "acme"})

            again = await session.call_tool("read_orders", {"customer": "acme"})

            assert again.ok
            await _until(pid_file.exists)
            assert int(pid_file.read_text(encoding="utf-8")) != dead
