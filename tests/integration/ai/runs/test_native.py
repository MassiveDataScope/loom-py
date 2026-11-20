"""A hand-driven run rejoins the runtime's bounds as the identity acting where it is entered."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.abc import AgentHandle
from loom.ai.execution._handle import AgentMarkerResolver
from loom.core.errors import DelegationExpired
from loom.core.execution import (
    ExecutionRequired,
    ExecutionScope,
    current_execution,
    execution_identity,
)
from loom.core.stores.sql.service import NullSqlQueryService
from loom.core.use_case import Agent

from .doubles import AGENT, ALICE, BOB, AcmeNativeAccess, AcmeNativeEngine


def _resolve(runtime: Any) -> Any:
    return AgentMarkerResolver(
        runtime, sql_query_service=NullSqlQueryService(), observability=None
    ).resolve(Agent(AGENT), AgentHandle[Any])


class TestTheNativeGuard:
    async def test_runs_its_body_as_the_identity_acting_where_it_is_entered(
        self, make_runtime: Any
    ) -> None:
        runtime = make_runtime(AcmeNativeEngine())
        handle = _resolve(runtime)

        async with runtime:
            with ExecutionScope(ALICE):
                access = handle.native()
            assert isinstance(access, AcmeNativeAccess)
            with ExecutionScope(BOB):
                async with access.guard():
                    inside = execution_identity()
            after = current_execution()

        assert inside == BOB
        assert after is None

    async def test_refuses_its_body_outside_any_execution(self, make_runtime: Any) -> None:
        runtime = make_runtime(AcmeNativeEngine())
        handle = _resolve(runtime)

        async with runtime:
            with ExecutionScope(ALICE):
                access = handle.native()
            with pytest.raises(ExecutionRequired):
                async with access.guard():
                    pass

    async def test_refuses_its_body_past_the_deadline_of_the_live_execution(
        self, make_runtime: Any, past_the_window: Callable[[], None]
    ) -> None:
        runtime = make_runtime(AcmeNativeEngine())
        handle = _resolve(runtime)

        async with runtime:
            with ExecutionScope(ALICE):
                access = handle.native()
                past_the_window()
                with pytest.raises(DelegationExpired):
                    async with access.guard():
                        pass

    async def test_the_native_call_fails_closed_outside_any_execution(
        self, make_runtime: Any
    ) -> None:
        runtime = make_runtime(AcmeNativeEngine())
        handle = _resolve(runtime)

        async with runtime:
            with pytest.raises(ExecutionRequired):
                handle.native()
