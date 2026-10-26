"""State reaches the dependency bundle through the pydantic-ai engine (T303).

``PydanticAIEngine.run``/``run_stream`` receive ``state`` already resolved
by :class:`~loom.ai.runtime.AgentRuntime` and forward it to
:class:`~loom.ai.abc.DepsFactory` unchanged (FR-009). This is the last hop
before it reaches the composition root's ``AgentDeps.state``
(``loom/composition/_ai.py``), which T301 already covers untouched.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from loom.ai.abc import AgentEngine
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import answering_model, build_engine, encode, make_plan

_IDENTITY = Identity(subject="caller")
_PROMPT = "assess this"
_ANSWER: Mapping[str, str] = {"answer": "ok"}


@dataclass
class _RecordingDeps:
    """Records every ``state`` this factory was asked to build a bundle for."""

    states: list[object | None] = field(default_factory=list)

    def build(
        self, identity: Identity, container: LoomContainer, state: Any | None = None
    ) -> object:
        """Record *state* and return a bundle carrying it, structurally like the real one."""
        del identity, container
        self.states.append(state)
        return _Bundle(state=state)


@dataclass(frozen=True)
class _Bundle:
    state: Any | None


def _engine(deps: _RecordingDeps) -> AgentEngine:
    model = answering_model(encode(_ANSWER))
    return build_engine(make_plan(), model, deps=deps)  # type: ignore[arg-type]


class TestStateReachesTheDependencyBundle:
    async def test_run_forwards_state_unchanged(self) -> None:
        deps = _RecordingDeps()
        engine = _engine(deps)

        await engine.run(_PROMPT, identity=_IDENTITY, state={"marca": "civic", "km": 12})

        assert deps.states == [{"marca": "civic", "km": 12}]

    async def test_run_stream_forwards_state_unchanged(self) -> None:
        deps = _RecordingDeps()
        engine = _engine(deps)

        async with engine.run_stream(
            _PROMPT, identity=_IDENTITY, state={"marca": "civic", "km": 3}
        ) as events:
            async for _ in events:
                pass

        assert deps.states == [{"marca": "civic", "km": 3}]

    async def test_a_run_with_no_state_forwards_none(self) -> None:
        deps = _RecordingDeps()
        engine = _engine(deps)

        await engine.run(_PROMPT, identity=_IDENTITY)

        assert deps.states == [None]
