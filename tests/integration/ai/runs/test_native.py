"""A hand-driven run rejoins the runtime's bounds as an execution acting as the handle's caller."""

from __future__ import annotations

from typing import Any

import pytest

from loom.ai.abc import AgentHandle
from loom.ai.runtime._handle import AgentMarkerResolver
from loom.core.execution import (
    ExecutionRequired,
    ExecutionScope,
    current_execution,
    execution_identity,
)
from loom.core.sql.service import NullSqlQueryService
from loom.core.use_case import Agent

from .doubles import AGENT, ALICE, AcmeNativeAccess, AcmeNativeEngine


class TestTheNativeGuard:
    async def test_runs_its_body_as_the_caller_of_the_native_call(self, make_runtime: Any) -> None:
        runtime = make_runtime(AcmeNativeEngine())
        handle: Any = AgentMarkerResolver(
            runtime, sql_query_service=NullSqlQueryService(), observability=None
        ).resolve(Agent(AGENT), AgentHandle[Any])

        async with runtime:
            with ExecutionScope(ALICE):
                access = handle.native()
            assert isinstance(access, AcmeNativeAccess)
            async with access.guard():
                inside = execution_identity()
            after = current_execution()

        assert inside == ALICE
        assert after is None

    async def test_the_native_call_fails_closed_outside_any_execution(
        self, make_runtime: Any
    ) -> None:
        runtime = make_runtime(AcmeNativeEngine())
        handle: Any = AgentMarkerResolver(
            runtime, sql_query_service=NullSqlQueryService(), observability=None
        ).resolve(Agent(AGENT), AgentHandle[Any])

        async with runtime:
            with pytest.raises(ExecutionRequired):
                handle.native()
