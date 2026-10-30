"""The escape hatch over a real ``AgentRuntime`` and a real ``PydanticAIEngineProvider``.

``tests/unit/ai/engines/test_pydantic_ai_escape_hatch.py`` drives ``native_agent``
against a hand-written handle that reimplements the marker-resolved path, and
its spend-cap test used a plan holding no capability at all. Neither would
notice a mutation that pins a fixed identity in ``_BoundAgentHandle.native``
or drops the artefact's grants from what the hatch hands back. This module
closes both gaps: a real ``AgentRuntime`` over a real
``PydanticAIEngineProvider``, resolving a marker exactly as
``agent_marker_resolver`` does, with a plan granting one real capability.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest

from loom.ai.abc import AgentHandle
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider, native_agent
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import agent_marker_resolver
from loom.composition._ai import AgentDepsFactory
from loom.core.bootstrap import create_kernel
from loom.core.identity import ANONYMOUS, Identity
from loom.core.sql.service import NullSqlQueryService
from tests.integration.ai.conftest import make_ai_config, make_plan
from tests.integration.ai.test_capabilities import (
    ScriptedToolModel,
    WhoAmIUseCase,
    whoami_capability,
)

_AGENT_NAME = "triage"
_ANALYST = Identity(subject="analyst-1", roles=("analyst",), mechanism="test")


@asynccontextmanager
async def _noop_guard() -> AsyncIterator[None]:
    """A guard entered by no test in this module: not what is under test here."""
    yield


def _as_handle(handle: object) -> AgentHandle[Any]:
    return cast("AgentHandle[Any]", handle)


async def _runtime_and_model() -> tuple[AgentRuntime, ScriptedToolModel]:
    kernel = create_kernel(config=object(), use_cases=[WhoAmIUseCase])
    model = ScriptedToolModel(calls=(("usecase_whoami", {}),))
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model.as_model())
    plan = make_plan(_AGENT_NAME, capabilities=(whoami_capability(),))
    runtime = AgentRuntime(
        plans=[plan],
        config=make_ai_config(),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=AgentDepsFactory(kernel.app),
        container=kernel.container,
    )
    return runtime, model


class TestTheHatchOverTheRealRuntime:
    async def test_the_hatch_hands_over_the_agent_the_runtime_itself_runs(self) -> None:
        runtime, _ = await _runtime_and_model()

        async with runtime:
            resolver = agent_marker_resolver(
                runtime, sql_query_service=NullSqlQueryService(), observability=None
            )
            handle = resolver(_AGENT_NAME, _ANALYST)

            async with native_agent(_as_handle(handle)) as access:
                engine = runtime._slots[_AGENT_NAME].engine  # noqa: SLF001
                assert isinstance(engine, PydanticAIEngine)
                assert access.agent is engine._agent  # noqa: SLF001

    async def test_a_capability_call_made_with_the_bundle_runs_as_the_authenticated_caller(
        self,
    ) -> None:
        runtime, model = await _runtime_and_model()

        async with runtime:
            resolver = agent_marker_resolver(
                runtime, sql_query_service=NullSqlQueryService(), observability=None
            )
            handle = resolver(_AGENT_NAME, _ANALYST)

            async with native_agent(_as_handle(handle)) as access:
                await access.agent.run("hello", deps=access.deps)

        assert _ANALYST.subject in model.shown

    async def test_a_bundle_built_for_an_anonymous_caller_is_refused_at_the_tool_boundary(
        self,
    ) -> None:
        """``_guards.authenticated_caller`` refuses at the call boundary itself.

        ``AgentHandle.native()`` already refuses an anonymous caller before
        the engine is ever asked (see ``TestAnonymousIdentity`` in
        ``tests/unit/ai/runtime/test_agent_handle.py``); this pins the
        engine's own, independent guard, reached only by driving the engine
        directly with an unauthenticated identity — the defense in depth a
        driver relying on a hand-built bundle would still need.
        """
        runtime, model = await _runtime_and_model()

        async with runtime:
            engine = runtime._slots[_AGENT_NAME].engine  # noqa: SLF001
            assert isinstance(engine, PydanticAIEngine)
            access = engine.native(identity=ANONYMOUS, guard=_noop_guard)

            with pytest.raises(AgentRunError) as excinfo:
                await access.native.agent.run("hello", deps=access.native.deps)

        assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert _ANALYST.subject not in model.shown
