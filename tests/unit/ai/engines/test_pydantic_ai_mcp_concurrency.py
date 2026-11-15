"""``_ToolsetSession`` declares itself safe for concurrent calls.

The runtime's ``mcp_session_for`` decides whether to serialise a session by
checking whether it subclasses :class:`~loom.ai.abc.ConcurrentMcpSession`
(``loom/ai/runtime/_mcp.py``). This pins the engine's own half of that
contract: its session must keep declaring it, or every grant view
(``handle.mcp()``, a use case's ``Mcp()``) queues behind its own neighbours
again. The second test measures that end to end, through the real dispatch
a worker wires at start-up: several concurrent calls over the session
``mcp_session_for`` hands back for a declared, already-open connection run
in parallel, not queued.

``TestCancellingDoesNotBlockTheSession`` pins the other side of not holding a
lock: ``_ToolsetSession.call_tool`` does **not** shield its round trip from
the caller's own cancellation -- cancelling one call returns to the caller
immediately, and a neighbour sharing the same ``MCPToolset`` keeps running
unaffected, because the underlying JSON-RPC client matches every in-flight
response to its own request id rather than needing the session to hold a
lock over it. Bounding a tool call by the plan's ``tool_timeout_ms`` is
exercised where that timeout is actually enforced -- inside
``capability_call`` on the ``kind: python`` route
(``tests/unit/ai/engines/test_python_capability.py``).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from types import SimpleNamespace
from typing import Any

import anyio
import pytest

from loom.ai.abc import ConcurrentMcpSession
from loom.ai.engines.pydantic_ai._mcp import _OpenToolset, _ToolsetSession
from loom.ai.runtime._mcp import mcp_session_for

_CALL_DELAY = 0.05


class _FakeMcpClient:
    """Stands in for the fastmcp client's ``call_tool_mcp``, with a known delay."""

    def __init__(self, delay: float, *, is_error: bool = False) -> None:
        self._delay = delay
        self._is_error = is_error
        self.finished: list[str] = []

    async def call_tool_mcp(self, name: str, arguments: Mapping[str, Any]) -> SimpleNamespace:
        await asyncio.sleep(self._delay)
        self.finished.append(name)
        return SimpleNamespace(is_error=self._is_error, structured_content=None)


class _FakeToolset:
    """Stands in for ``MCPToolset``: a real refcount behind a real lock.

    A fake whose ``__aexit__`` returns without awaiting anything can never
    exercise the path that matters -- ``MCPToolset``'s own ``__aexit__``
    takes ``_enter_lock`` and, at refcount zero, awaits closing its exit
    stack. Mirroring that shape is what lets a test on this double catch a
    refcount left unbalanced by a cancelled call, rather than passing by
    construction because there was never anything to await.
    """

    def __init__(self, delay: float, *, is_error: bool = False) -> None:
        self.client = _FakeMcpClient(delay, is_error=is_error)
        self._lock = anyio.Lock()
        self.running_count = 0

    async def __aenter__(self) -> _FakeToolset:
        async with self._lock:
            self.running_count += 1
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        async with self._lock:
            self.running_count -= 1
            if self.running_count == 0:
                await asyncio.sleep(0)


class TestTheEngineAdapterDeclaresConcurrency:
    """``_ToolsetSession`` reference-counts a shared ``MCPToolset``: no lock needed."""

    def test_toolset_session_declares_a_concurrent_mcp_session(self) -> None:
        # ``object()`` basta: la anotacion del toolset es solo de tipos (future
        # annotations), y el constructor no toca el objeto que recibe.
        session = _ToolsetSession(_OpenToolset.of(object()))  # type: ignore[arg-type]
        assert isinstance(session, ConcurrentMcpSession)

    async def test_calls_under_one_grant_really_run_in_parallel(self) -> None:
        # El mismo camino que ``AgentRuntime`` recorre al abrir una conexion:
        # la sesion del motor, pasada por el unico punto que decide envolver o
        # no (``mcp_session_for``).
        toolset = _FakeToolset(delay=_CALL_DELAY)
        engine_session = _ToolsetSession(_OpenToolset.of(toolset))  # type: ignore[arg-type]
        session = mcp_session_for(engine_session, label="crm")
        start = time.monotonic()
        await asyncio.gather(*(session.call_tool(f"tool{i}", {}) for i in range(8)))
        elapsed = time.monotonic() - start
        # Ocho llamadas en paralelo se parecen a una, no a ocho en fila.
        assert elapsed < _CALL_DELAY * 3


class TestTheErrorResultIsTranslated:
    """``call_tool`` traduce el ``is_error`` del protocolo al ``ok`` propio."""

    @pytest.fixture
    def error_result(self) -> Callable[..., Awaitable[Any]]:
        async def translate(*, is_error: bool) -> Any:
            toolset = _FakeToolset(delay=0.0, is_error=is_error)
            session = _ToolsetSession(_OpenToolset.of(toolset))  # type: ignore[arg-type]
            return await session.call_tool("tool", {})

        return translate

    async def test_is_error_false_translates_to_ok_true(
        self, error_result: Callable[..., Awaitable[Any]]
    ) -> None:
        result = await error_result(is_error=False)
        assert result.ok is True

    async def test_is_error_true_translates_to_ok_false(
        self, error_result: Callable[..., Awaitable[Any]]
    ) -> None:
        result = await error_result(is_error=True)
        assert result.ok is False


class TestCancellingDoesNotBlockTheSession:
    """``_ToolsetSession.call_tool`` ya no escuda el viaje (B1, regresion de
    ``tool_timeout_ms`` en la ruta ``kind: python``): sin candado que
    sostener, cancelar una llamada corta de inmediato en lugar de esperar a
    que el remoto termine. Lo que antes garantizaba
    :func:`~loom.ai._concurrency.shield_and_drain` en esta clase ahora solo
    hace falta donde hay un candado real -- :class:`~loom.ai.runtime._mcp.SharedMcpSession`
    (ver ``tests/unit/ai/runtime/test_mcp_session.py``).
    """

    @pytest.fixture
    def slow_session(self) -> tuple[_FakeToolset, _ToolsetSession]:
        toolset = _FakeToolset(delay=0.1)
        session = _ToolsetSession(_OpenToolset.of(toolset))  # type: ignore[arg-type]
        return toolset, session

    async def test_cancel_returns_control_immediately(
        self, slow_session: tuple[_FakeToolset, _ToolsetSession]
    ) -> None:
        toolset, session = slow_session
        task = asyncio.create_task(session.call_tool("victim", {}))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        # Orden, no reloj: sin escudo, la cancelacion ya regreso al llamante
        # antes de que el remoto -- que tarda 0.1s -- llegase a responder.
        assert "victim" not in toolset.client.finished

    async def test_cancelling_one_does_not_block_its_concurrent_neighbour(
        self, slow_session: tuple[_FakeToolset, _ToolsetSession]
    ) -> None:
        toolset, session = slow_session
        start = time.monotonic()
        victim = asyncio.create_task(session.call_tool("victim", {}))
        neighbour = asyncio.create_task(session.call_tool("neighbour", {}))
        await asyncio.sleep(0.01)
        victim.cancel()
        await neighbour
        elapsed = time.monotonic() - start
        # La vecina corria en paralelo desde el principio: termina alrededor
        # de su propio delay, sin que la cancelacion de la otra la retrase.
        assert elapsed < 0.15
        try:
            await victim
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        assert "neighbour" in toolset.client.finished

    async def test_the_session_stays_usable_after_cancelling_one_call(
        self, slow_session: tuple[_FakeToolset, _ToolsetSession]
    ) -> None:
        _toolset, session = slow_session
        task = asyncio.create_task(session.call_tool("victim", {}))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        result = await session.call_tool("after", {})
        assert result.ok is True

    async def test_the_toolset_refcount_returns_to_zero_after_cancel(
        self, slow_session: tuple[_FakeToolset, _ToolsetSession]
    ) -> None:
        toolset, session = slow_session
        task = asyncio.create_task(session.call_tool("victim", {}))
        await asyncio.sleep(0.01)
        # Todavia en curso: la llamada ya tomo su referencia al entrar en el
        # ``async with`` de ``call_tool``.
        assert toolset.running_count == 1
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        # ``async with`` sale igual con una cancelacion que sin ella: el
        # refcount vuelve a su punto de partida, sin fuga.
        assert toolset.running_count == 0
