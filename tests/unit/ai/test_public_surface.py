"""Public surface and engine-neutrality contract for ``loom.ai`` (T034, T035).

Pins two properties of the pillar's public API:

* ``loom.ai.__all__`` exposes exactly the engine-neutral surface — no private
  names, no engine or vendor types, and every listed name resolves.
* The ``AgentEngine`` protocol takes a single prompt and, optionally, the
  conversation the run continues as loom's opaque ``Conversation`` (FR-034),
  and the run's state: runs take a ``prompt``, a keyword-only ``identity``, a
  keyword-only ``conversation`` defaulting to ``None`` and a keyword-only
  ``state`` defaulting to ``None``; no ``message``/``history``-style
  parameter exists, and ``run_stream`` is an async context manager.
"""

from __future__ import annotations

import inspect
from types import ModuleType

import pytest

import loom.ai
import loom.ai.declarative
import loom.ai.execution
from loom.ai.abc import AgentEngine

_REQUIRED_EXPORTS = frozenset(
    {
        "InferenceTarget",
        "AiConfig",
        "A2AConfig",
        "AgentEndpointConfig",
        "TextDeltaEvent",
        "ToolCallEvent",
        "ToolResultEvent",
        "ErrorEvent",
        "FinalEvent",
        "AgentEvent",
        "AgentResult",
        "AgentUsage",
        "Conversation",
        "HealthStatus",
        "AgentEngine",
        "AgentEngineProvider",
        "ToolsetFactory",
        "ToolsetContext",
        "McpSession",
        "McpToolInfo",
        "McpToolCallResult",
        "DepsFactory",
        "AgentAnswer",
        "AgentHandle",
        "McpHandle",
        "SqlGrantHandle",
    }
)

_ENGINE_NAME_FRAGMENTS = ("Pydantic", "OpenAI", "Bedrock", "LangChain", "Fake")

# Names retired when ``mcp``/``a2a``/``skills`` collapsed onto one flat
# include/exclude filter.  A re-export would resurrect a vocabulary the
# artifact contract no longer has.
_RETIRED_EXPORTS = frozenset({"ToolFilter"})

_FORBIDDEN_RUN_PARAMS = frozenset(
    {"message", "messages", "history", "chat_history", "message_history"}
)

_RUN_PARAMS = frozenset({"self", "prompt", "identity", "conversation", "state"})


def _run_signature(method_name: str) -> inspect.Signature:
    """Signature of an ``AgentEngine`` protocol method."""
    return inspect.signature(getattr(AgentEngine, method_name))


class TestPublicExports:
    def test_all_contains_the_public_surface(self) -> None:
        """Every engine-neutral name of the fixed surface is exported."""
        assert set(loom.ai.__all__) >= _REQUIRED_EXPORTS

    def test_mcp_session_is_one_object_under_both_public_names(self) -> None:
        """``loom.ai.McpSession`` and ``loom.ai.execution.McpSession`` are the same Protocol."""
        assert loom.ai.McpSession is loom.ai.execution.McpSession

    def test_all_contains_no_private_names(self) -> None:
        """No underscore-prefixed symbol leaks into the public surface."""
        assert [name for name in loom.ai.__all__ if name.startswith("_")] == []

    def test_all_contains_no_engine_types(self) -> None:
        """Vendor and engine types never appear in the neutral surface (FR-034)."""
        leaked = [
            name
            for name in loom.ai.__all__
            if any(fragment in name for fragment in _ENGINE_NAME_FRAGMENTS)
        ]

        assert leaked == []

    def test_every_name_in_all_resolves(self) -> None:
        """``__all__`` never advertises a name the module cannot deliver."""
        missing = [name for name in loom.ai.__all__ if not hasattr(loom.ai, name)]

        assert missing == []

    def test_all_contains_no_retired_names(self) -> None:
        """``ToolFilter`` was retired with the nested filter; it must not come back."""
        assert set(loom.ai.__all__) & _RETIRED_EXPORTS == set()

    @pytest.mark.parametrize("module", [loom.ai, loom.ai.declarative], ids=["ai", "declarative"])
    def test_retired_names_are_not_reachable_on_the_module(
        self,
        module: ModuleType,
    ) -> None:
        """Absence from ``__all__`` is not enough: the attribute must be gone too."""
        reachable = [name for name in _RETIRED_EXPORTS if hasattr(module, name)]

        assert reachable == []

    def test_loom_ai_does_not_export_identity(self) -> None:
        """Identity comes from the caller, never from the AI pillar (FR-043)."""
        identity_like = [name for name in loom.ai.__all__ if "identity" in name.lower()]

        assert identity_like == []


class TestAgentEngineProtocol:
    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_exposes_no_message_list_parameter(
        self,
        method_name: str,
    ) -> None:
        """No message-list parameter: the history rides inside ``Conversation`` (FR-034)."""
        parameters = set(_run_signature(method_name).parameters)

        assert parameters & _FORBIDDEN_RUN_PARAMS == set()

    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_the_parameter_set_is_exact(
        self,
        method_name: str,
    ) -> None:
        """The run contract is exactly prompt, identity, conversation and state (AC14)."""
        parameters = set(_run_signature(method_name).parameters)

        assert parameters == _RUN_PARAMS

    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_conversation_is_keyword_only_defaulting_to_none(
        self,
        method_name: str,
    ) -> None:
        """``conversation`` is optional and keyword-only: ``None`` means single-shot."""
        conversation = _run_signature(method_name).parameters["conversation"]

        assert conversation.kind is inspect.Parameter.KEYWORD_ONLY
        assert conversation.default is None

    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_conversation_is_annotated_with_the_neutral_type(
        self,
        method_name: str,
    ) -> None:
        """The parameter is typed with loom's ``Conversation``, never an engine type."""
        conversation = _run_signature(method_name).parameters["conversation"]

        assert "Conversation" in str(conversation.annotation)

    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_identity_is_keyword_only(
        self,
        method_name: str,
    ) -> None:
        """Every run takes the caller identity as an explicit keyword."""
        identity = _run_signature(method_name).parameters["identity"]

        assert identity.kind is inspect.Parameter.KEYWORD_ONLY

    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_state_is_keyword_only_defaulting_to_none(
        self,
        method_name: str,
    ) -> None:
        """``state`` is optional and keyword-only: ``None`` means no invocation state."""
        state = _run_signature(method_name).parameters["state"]

        assert state.kind is inspect.Parameter.KEYWORD_ONLY
        assert state.default is None

    def test_run_stream_is_annotated_as_an_async_context_manager(
        self,
    ) -> None:
        """``run_stream`` is annotated as ``AbstractAsyncContextManager`` (R-008)."""
        annotations = getattr(AgentEngine.run_stream, "__annotations__", {})

        assert "AbstractAsyncContextManager" in str(annotations.get("return"))


class TestDeclaredNamesAreImportable:
    def test_every_name_declared_in_all_is_importable(self) -> None:
        """A name in ``__all__`` that does not resolve is worse than an absent one.

        ``from loom.ai import *`` raises on it, and a reader takes the list as the
        declared surface.  The list grew by seven names when the error vocabulary
        was published; this pins that they exist.
        """
        import loom.ai as pillar

        assert [name for name in pillar.__all__ if not hasattr(pillar, name)] == []
