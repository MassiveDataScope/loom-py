"""``AgentHost.native``'s guard: the chain and admission a hand-driven run may rejoin.

A run driven through ``AgentHandle.native()`` pushes nothing onto loom's own
call chain and takes no admission permit on its own — that is left to the
caller, through the zero-argument guard factory ``native()`` hands back on
the object it returns. These tests exercise that guard directly, against a
stub engine, so the cycle, depth and admission bounds it enforces are pinned
independently of any one engine adapter.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any

import pytest

from loom.ai.abc import HealthStatus
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.execution import AgentHost
from loom.ai.execution._chain import current_chain
from loom.core.di import LoomContainer
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    StubDepsFactory,
    make_ai_config,
    make_plan,
)

_AUTHENTICATED = Identity(subject="ada", mechanism="test")


@dataclass(frozen=True, slots=True)
class _NativeAccess:
    """The one attribute these tests need from an engine's native form."""

    guard: Callable[[], AbstractAsyncContextManager[None]]


class _NativeCapableEngine:
    """A stub engine publishing ``native()``, for the guard alone."""

    def native(
        self,
        *,
        identity: Identity,
        state: object | None = None,
        guard: Callable[[], AbstractAsyncContextManager[None]],
    ) -> _NativeAccess:
        del identity, state
        return _NativeAccess(guard=guard)

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


def _host(
    *, max_agent_depth: int = 1, max_concurrent_runs: int = 8, names: tuple[str, ...]
) -> AgentHost:
    engines: dict[str, Any] = {name: _NativeCapableEngine() for name in names}
    config = make_ai_config(
        max_agent_depth=max_agent_depth, max_concurrent_runs=max_concurrent_runs
    )
    return AgentHost(
        plans=[make_plan(name) for name in names],
        config=config,
        engine_plugin=CountingEngineProvider(engines=engines),  # type: ignore[arg-type]
        deps=StubDepsFactory(),
        container=LoomContainer(),
    )


def _access(host: AgentHost, name: str) -> _NativeAccess:
    native = host.native(name, identity=_AUTHENTICATED)
    assert isinstance(native, _NativeAccess)
    return native


class TestCycleDetection:
    async def test_driving_the_same_agent_twice_nested_raises_the_cycle_code(self) -> None:
        host = _host(max_agent_depth=2, names=("a",))
        async with host:
            with ExecutionScope(_AUTHENTICATED):
                outer = _access(host, "a")
                async with outer.guard():
                    inner = _access(host, "a")
                    with pytest.raises(AgentRunError) as excinfo:
                        async with inner.guard():
                            pass

        assert excinfo.value.code is AgentRunErrorCode.AGENT_CALL_CYCLE


class TestDepthBound:
    async def test_exceeding_max_agent_depth_raises_the_too_deep_code(self) -> None:
        host = _host(max_agent_depth=2, names=("a", "b", "c"))
        async with host:
            with ExecutionScope(_AUTHENTICATED):
                a = _access(host, "a")
                async with a.guard():
                    b = _access(host, "b")
                    async with b.guard():
                        c = _access(host, "c")
                        with pytest.raises(AgentRunError) as excinfo:
                            async with c.guard():
                                pass

        assert excinfo.value.code is AgentRunErrorCode.AGENT_CALL_TOO_DEEP


class TestAdmission:
    async def test_exceeding_max_concurrent_runs_raises_too_many_runs(self) -> None:
        host = _host(max_agent_depth=2, max_concurrent_runs=1, names=("a", "b"))
        async with host:
            with ExecutionScope(_AUTHENTICATED):
                a = _access(host, "a")
                async with a.guard():
                    b = _access(host, "b")
                    with pytest.raises(AgentRunError) as excinfo:
                        async with b.guard():
                            pass

        assert excinfo.value.code is AgentRunErrorCode.TOO_MANY_RUNS


class TestChainRestoration:
    async def test_the_chain_is_restored_after_the_body(self) -> None:
        host = _host(names=("a",))
        async with host:
            with ExecutionScope(_AUTHENTICATED):
                assert current_chain() == ()
                access = _access(host, "a")
                async with access.guard():
                    assert current_chain() == ("a",)
                assert current_chain() == ()

    async def test_the_chain_is_restored_even_when_the_body_raises(self) -> None:
        host = _host(names=("a",))
        async with host:
            with ExecutionScope(_AUTHENTICATED):
                access = _access(host, "a")
                with pytest.raises(ValueError):
                    async with access.guard():
                        raise ValueError("boom")
                assert current_chain() == ()
