"""What actually reaches the wire when a remote A2A agent declares a credential.

Nothing here asserts that an object was constructed. A local HTTP server serves
the agent card and **records the headers of every request it receives**, so each
test reads the credential off the request the remote agent would have seen. A
client that builds cleanly while sending nothing is exactly the defect this
module exists to catch: ``headers_ref`` was declared, documented, validated and
compiled, and then silently dropped on the floor by the transport.

The card fetch is what is asserted on deliberately: ``A2ACardResolver`` makes
the **first** request of the session, so an agent that authenticates its card
endpoint fails at start-up before any skill is ever called.

The server speaks plaintext on the loopback interface, which is the one place
loom allows it (``_warn_plaintext_loopback``): the traffic cannot leave the
machine, so no certificate is needed to prove that a header travelled.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from loom.ai.compiler import CompiledA2ACapability, CompiledRemoteAuth
from loom.ai.engines.pydantic_ai._a2a import create_a2a_client
from loom.ai.engines.pydantic_ai.extras.a2a import build_a2a_http_client
from loom.ai.remote_auth import shared_a2a_auth
from loom.core.model.redaction import Secret

from ...helpers.remote_auth_plugin import third_party_strategy

pytest.importorskip("a2a.client", reason="the A2A SDK is not installed: uv sync --extra ai-a2a")

_AGENT = "market"
_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhLWIifQ.s_g-9"

_CARD: dict[str, Any] = {
    "protocolVersion": "0.3.0",
    "name": _AGENT,
    "description": "Quotes instruments for the wiring tests.",
    "version": "1.0.0",
    "preferredTransport": "JSONRPC",
    "capabilities": {"streaming": False},
    "defaultInputModes": ["text/plain"],
    "defaultOutputModes": ["text/plain"],
    "skills": [
        {"id": "quote", "name": "quote", "description": "Quote an instrument.", "tags": ["market"]}
    ],
}
"""Minimal card the SDK accepts; its ``url`` is filled in with the live port."""


@dataclass
class _RecordingAgent:
    """A remote agent that answers with a card and remembers who asked.

    Attributes:
        url: Base URL the capability points at.
        requests: Headers of every request received, in arrival order.
    """

    url: str
    requests: list[dict[str, str]] = field(default_factory=list)

    def card_request(self) -> dict[str, str]:
        """Return the headers of the card fetch, the first request of a session."""
        assert self.requests, "the client made no request at all"
        return self.requests[0]


@pytest.fixture
def remote_agent() -> Iterator[_RecordingAgent]:
    """Serve the agent card on the loopback interface, recording every request."""
    agent = _RecordingAgent(url="")

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server's naming contract
            agent.requests.append({key.lower(): value for key, value in self.headers.items()})
            body = json.dumps({**_CARD, "url": agent.url}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - base signature
            """Silence the per-request logging of the stdlib server."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    agent.url = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield agent
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture(autouse=True)
def _isolated_sharing() -> Iterator[None]:
    """Empty the per-endpoint sharing map: identity assertions must not leak between tests."""
    from loom.ai import remote_auth

    remote_auth._STRATEGIES._by_endpoint.clear()
    yield
    remote_auth._STRATEGIES._by_endpoint.clear()


async def _card_request(
    capability: CompiledA2ACapability, agent: _RecordingAgent
) -> dict[str, str]:
    """Open one session and return the headers the card fetch carried."""
    async with create_a2a_client(capability):
        pass
    return agent.card_request()


class TestHeadersRefReachesTheCardRequest:
    """The field was declared and compiled; it must now travel."""

    async def test_the_card_request_carries_the_header(self, remote_agent: _RecordingAgent) -> None:
        capability = CompiledA2ACapability(
            agent=_AGENT, url=remote_agent.url, headers_ref=Secret("X-API-Key=abc123")
        )

        headers = await _card_request(capability, remote_agent)

        assert headers["x-api-key"] == "abc123"

    async def test_carries_no_credential_when_the_agent_declares_none(
        self, remote_agent: _RecordingAgent
    ) -> None:
        """The no-auth path is unchanged, which is what keeps artifacts portable."""
        capability = CompiledA2ACapability(agent=_AGENT, url=remote_agent.url)

        headers = await _card_request(capability, remote_agent)

        assert "x-api-key" not in headers
        assert "authorization" not in headers


class TestBearerStrategy:
    """``kind: bearer`` is the shape configuration cannot express by hand."""

    async def test_the_card_request_carries_authorization_bearer(
        self, remote_agent: _RecordingAgent
    ) -> None:
        capability = CompiledA2ACapability(
            agent=_AGENT,
            url=remote_agent.url,
            auth=CompiledRemoteAuth(kind="bearer", settings=(("token_ref", _TOKEN),)),
        )

        headers = await _card_request(capability, remote_agent)

        assert headers["authorization"] == f"Bearer {_TOKEN}"


class TestThirdPartyStrategy:
    """The extension point is only real if someone who is not loom can use it."""

    async def test_the_request_carries_what_the_installed_strategy_composes(
        self, remote_agent: _RecordingAgent, tmp_path: Path
    ) -> None:
        capability = CompiledA2ACapability(
            agent=_AGENT,
            url=remote_agent.url,
            auth=CompiledRemoteAuth(
                kind="agent-session",
                settings=(
                    ("session_url", "https://market.example.com/auth/agent/session"),
                    ("bootstrap_ref", "/agents/prod/agent-sales"),
                ),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            headers = await _card_request(capability, remote_agent)

        assert headers["authorization"] == "Agent /agents/prod/agent-sales"


class TestOneCredentialPerRemoteAgent:
    """Two agents pointing at the same remote share one credential, by identity."""

    async def test_two_grants_of_the_same_remote_share_the_instance(
        self, remote_agent: _RecordingAgent
    ) -> None:
        """Asserted on the credential loom resolves, not on ``client.auth``.

        The built-in strategies return a callable and ``httpx`` wraps a callable
        in a ``FunctionAuth`` of its own, one per client, so two clients sharing
        one credential no longer share one ``client.auth`` object. What has to
        be a single object is the credential itself — the thing a renewing
        strategy holds a token in — and both clients are built from it.
        """
        auth = CompiledRemoteAuth(kind="bearer", settings=(("token_ref", _TOKEN),))
        capability = CompiledA2ACapability(agent=_AGENT, url=remote_agent.url, auth=auth)
        other = CompiledA2ACapability(
            agent=_AGENT, url=remote_agent.url, auth=auth, include=("quote",)
        )

        async with (
            build_a2a_http_client(capability) as first,
            build_a2a_http_client(other) as second,
        ):
            presented = (first.auth, second.auth)

        assert None not in presented
        assert shared_a2a_auth(_AGENT, capability.auth) is shared_a2a_auth(_AGENT, other.auth)
