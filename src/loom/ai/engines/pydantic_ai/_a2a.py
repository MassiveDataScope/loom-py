"""Outbound A2A transport: this agent calling a remote one.

Two entry points, one connection recipe, both served by the
:mod:`~loom.ai.engines.pydantic_ai.extras.a2a` island, the one module of the
engine that imports the ``a2a-sdk``:

* :func:`create_a2a_client` is the :data:`~loom.ai.execution.A2AClientFactory`
  the runtime opens at start-up. Entering it fetches the remote card, applies
  the grant's skill filter to it and hands back a usable session, so an
  unreachable or mismatched agent fails start-up as ``A2A_AGENT_UNREACHABLE``
  instead of surfacing on the first delegation.
* :func:`send_to_remote_agent` performs one delegation.

This module loads the island when a grant names it — :func:`require_a2a_sdk`
at build, the two entry points at call — so a deployment that declares no
``a2a`` grant never imports the SDK, and one that does fails start-up naming
the ``ai-a2a`` extra when it is missing.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Protocol, cast

from loom.ai.compiler import CompiledA2ACapability
from loom.ai.registry import require_provider_sdk

if TYPE_CHECKING:
    from a2a.client import Client

_ISLAND = "loom.ai.engines.pydantic_ai.extras.a2a"
_EXTRA = "ai-a2a"


class _A2ASdk(Protocol):
    """The two names the A2A island publishes."""

    def create_a2a_client(
        self, capability: CompiledA2ACapability
    ) -> AbstractAsyncContextManager[Client]: ...

    async def send_to_remote_agent(self, capability: CompiledA2ACapability, prompt: str) -> str: ...


def _sdk() -> _A2ASdk:
    """Load the island; the cast records the contract every island keeps."""
    return cast(_A2ASdk, require_provider_sdk("a2a", _ISLAND, _EXTRA))


def require_a2a_sdk() -> None:
    """Fail the build when the ``ai-a2a`` extra is not installed.

    Raises:
        AgentCompilationError: With ``PROVIDER_NOT_INSTALLED`` naming the
            extra when the island cannot be imported.
    """
    _sdk()


def create_a2a_client(capability: CompiledA2ACapability) -> AbstractAsyncContextManager[Client]:
    """Open one session against a remote agent, card fetched and checked.

    Satisfies :data:`~loom.ai.execution.A2AClientFactory`. See the island's
    ``create_a2a_client`` for what entering the context does and raises.

    Example::

        runtime = AgentRuntime(..., a2a_client_factory=create_a2a_client)
    """
    return _sdk().create_a2a_client(capability)


async def send_to_remote_agent(capability: CompiledA2ACapability, prompt: str) -> str:
    """Delegate one prompt to a remote agent and return its reply text.

    See the island's ``send_to_remote_agent`` for the contract: the reply is
    untrusted data, presented as a tool value and never as instruction.
    """
    return await _sdk().send_to_remote_agent(capability, prompt)
