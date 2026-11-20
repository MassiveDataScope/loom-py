"""The A2A client (``ai-a2a`` extra): one session against a remote agent, and one delegation.

The remote agent's credential is applied to the HTTP client itself, not to a
single request, so **every** call carries it — the card fetch included. The
card is the first request made, and an agent that authenticates its card
endpoint would otherwise fail start-up before a skill was ever called. The
credential is resolved by :mod:`loom.ai.remote_auth`, the same registry the
MCP transport uses; this module only carries the result to the client.

**The remote agent's reply is untrusted input.** It is returned as a
tool *value*, never merged into instructions; it is never logged; and no byte
of it reaches an error message, so a remote agent cannot dictate what the
calling model or the caller is told about a failure.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Final

import httpx
from a2a.client import A2ACardResolver, Client, ClientConfig, ClientFactory
from a2a.helpers import get_stream_response_text, new_text_message
from a2a.types.a2a_pb2 import AgentCard, Role, SendMessageRequest

from loom.ai._filters import matches, select_names
from loom.ai.compiler import CompiledA2ACapability
from loom.ai.remote_auth import headers_from_ref, shared_a2a_auth

__all__ = ["build_a2a_http_client", "create_a2a_client", "send_to_remote_agent"]

_ACCEPTED_OUTPUT_MODES: Final[tuple[str, ...]] = ("text/plain", "application/json")
"""Output modes named explicitly: some servers refuse a request that omits them."""


def build_a2a_http_client(capability: CompiledA2ACapability) -> httpx.AsyncClient:
    """Build the HTTP client of one grant, carrying the credential it declared.

    Fixed headers from ``headers_ref``, or the object the named strategy
    builds — one instance per configured agent, shared by every agent granted
    it.  Both are set on the client, so the card fetch presents them exactly as
    the delegations that follow do.  A remote agent that declares neither is
    connected exactly as before, which is what lets one artifact move between
    environments unchanged.

    Args:
        capability: Compiled grant carrying the validated remote agent URL and
            the credential resolved for it.

    Returns:
        The unopened client the card resolver and the session both speak
        through; the caller closes it.

    Raises:
        AgentCompilationError: When the ``headers_ref`` payload is not one
            ``Name=value`` pair, or the named strategy cannot be built.
    """
    component = f"a2a agent '{capability.agent}'"
    return httpx.AsyncClient(
        headers=headers_from_ref(component, capability.headers_ref),
        auth=shared_a2a_auth(capability.agent, capability.auth),
    )


@asynccontextmanager
async def create_a2a_client(capability: CompiledA2ACapability) -> AsyncIterator[Client]:
    """Open one session against a remote agent, card fetched and checked.

    Nothing happens until the context is entered, so the host's start-up
    deadline bounds the whole of it and a failure is reported as a coded
    start-up issue.

    Args:
        capability: Compiled grant carrying the validated ``https://`` URL, the
            credential the deployment declared for the remote agent, and the
            ``include``/``exclude`` filter selecting the skills the artifact
            delegates to.

    Yields:
        The connected client, closed together with its HTTP client on exit.

    Raises:
        AgentCompilationError: When the grant's credential cannot be resolved.
        AgentCardResolutionError: When the card cannot be fetched, decoded or
            validated.
        ValueError: When the card advertises no transport this client speaks,
            or advertises no skill the grant's filter selects.
    """
    async with build_a2a_http_client(capability) as http_client:
        card = await A2ACardResolver(http_client, capability.url).get_agent_card()
        _reject_ungranted_card(capability, card)
        config = ClientConfig(
            streaming=False,
            httpx_client=http_client,
            accepted_output_modes=list(_ACCEPTED_OUTPUT_MODES),
        )
        async with ClientFactory(config).create(card) as client:
            yield client


async def send_to_remote_agent(capability: CompiledA2ACapability, prompt: str) -> str:
    """Delegate one prompt to a remote agent and return its reply text.

    Opens :func:`create_a2a_client` per call, because ``build_toolsets`` is
    synchronous and the engine has no channel to the session the host
    opened: sharing it would need a lifecycle handoff that does not exist yet.
    The cost is one card fetch per delegation, bounded by ``tool_timeout_ms``
    like the call it precedes, and it buys a stateless toolset — no cached
    client, no connection nobody closes.

    Args:
        capability: Compiled grant naming the remote agent.
        prompt: Text the calling model asks the remote agent to act on.

    Returns:
        The concatenated text of the remote agent's reply. It is untrusted
        data: the caller presents it as a tool value and never as instruction.

    Raises:
        Exception: Whatever the transport raises. The caller maps it to a coded
            failure; this function deliberately does not, so that the mapping
            lives with every other capability's mapping.
    """
    async with create_a2a_client(capability) as client:
        return await _reply_text(client, prompt)


async def _reply_text(client: Client, prompt: str) -> str:
    """Send one message and join the text of every response it produces."""
    request = SendMessageRequest(message=new_text_message(prompt, role=Role.ROLE_USER))
    chunks = [get_stream_response_text(response) async for response in client.send_message(request)]
    return "\n".join(chunk for chunk in chunks if chunk)


def _reject_ungranted_card(capability: CompiledA2ACapability, card: AgentCard) -> None:
    """Refuse a card the grant's skill filter does not usefully select from.

    Two failures are distinguished, most specific first: an ``include`` pattern
    that matches no advertised skill — the artifact asked for something this
    remote does not have — and, failing that, a filter whose ``exclude`` leaves
    nothing selected. Only patterns the artifact already carries are reported:
    the card is untrusted input, so no text of it reaches the
    start-up issue.
    """
    if not capability.include and not capability.exclude:
        return
    advertised = tuple(skill.id for skill in card.skills)
    unmatched = tuple(
        pattern for pattern in capability.include if not _matches_any(pattern, advertised)
    )
    if unmatched:
        raise ValueError(
            f"the card advertises nothing matching the granted skills: {', '.join(unmatched)}"
        )
    if not select_names(advertised, include=capability.include, exclude=capability.exclude):
        raise ValueError("the card advertises no skill matching the granted filter")


def _matches_any(pattern: str, names: Sequence[str]) -> bool:
    """Report whether one granted pattern selects at least one advertised name."""
    return any(matches(name, (pattern,)) for name in names)
