"""What actually reaches the MCP client when a server declares a credential.

Everything here asserts on the transport the engine hands to
``pydantic_ai.mcp.MCPToolset`` — its headers and its ``auth`` object — rather
than on the absence of an exception. A toolset that constructs cleanly while
sending no credential is exactly the failure this module exists to catch.

The two strategies loom ships are pinned twice over: the transport keeps the
object the strategy built, and that object puts the header on the wire through
**both** HTTP libraries in play — ``httpx2``, which the MCP transport speaks,
and ``httpx``, which loom's own A2A client speaks. One credential, two
flavours, is the whole point of returning a callable.

The MCP client lives in the optional ``mcp-tests`` group, so the module skips
where it is absent, as ``test_mcp_end_to_end.py`` does.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.ai.compiler import CompiledMcpCapability, CompiledRemoteAuth
from loom.ai.engines.pydantic_ai._mcp import build_mcp_toolset
from loom.ai.remote_auth import shared_mcp_auth

from ...helpers.remote_auth_plugin import third_party_strategy

pytest.importorskip(
    "pydantic_ai.mcp",
    reason="the MCP client is not installed: uv sync --group mcp-tests",
    exc_type=ImportError,
)

_URL = "https://orders.example.com/mcp"
_CATALOG_URL = "https://catalog.example.com/mcp"

_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhLWIifQ.s_g-9"

_BEARER = CompiledMcpCapability(
    server="catalog",
    url=_CATALOG_URL,
    auth=CompiledRemoteAuth(kind="bearer", settings=(("token_ref", _TOKEN),)),
)

_STATIC = CompiledMcpCapability(
    server="catalog",
    url=_CATALOG_URL,
    auth=CompiledRemoteAuth(kind="static", settings=(("headers_ref", "X-API-Key=abc123"),)),
)

_FLAVOURS = ("httpx2", "httpx")
"""The HTTP libraries an auth object built here has to satisfy.

``httpx2`` is what fastmcp's transport drives for MCP; ``httpx`` is what loom
builds the A2A client with. Neither is a loom dependency, so each parametrised
case skips when its library is absent.
"""


def _header_on_the_wire(flavour: str, auth: Any, name: str) -> str | None:
    """Drive one request through a client of ``flavour`` and read the header it sent.

    Args:
        flavour: Module name of the HTTP library to drive, skipped when absent.
        auth: The object the strategy built, handed to the client's ``auth=``.
        name: Header whose value the assertion is about.

    Returns:
        The value the client put on the request, or ``None`` when it sent none.
    """
    library = pytest.importorskip(flavour)
    sent: dict[str, str | None] = {}

    def _capture(request: Any) -> Any:
        sent["value"] = request.headers.get(name)
        return library.Response(200)

    with library.Client(auth=auth, transport=library.MockTransport(_capture)) as client:
        client.post(_CATALOG_URL)
    return sent["value"]


def _transport(capability: CompiledMcpCapability) -> Any:
    """Build the toolset and return the transport its client will speak through."""
    return build_mcp_toolset(capability, init_timeout=10.0).client.transport


@pytest.fixture(autouse=True)
def _isolated_sharing() -> Any:
    """Empty the per-endpoint sharing map: identity assertions must not leak between tests."""
    from loom.ai import remote_auth

    remote_auth._STRATEGIES._by_endpoint.clear()
    yield
    remote_auth._STRATEGIES._by_endpoint.clear()


class TestHeadersRefReachesTheClient:
    """The compile-time refusal is gone; the header must actually travel."""

    def test_the_client_carries_the_header_when_the_server_declares_headers_ref(self) -> None:
        capability = CompiledMcpCapability(
            server="orders", url=_URL, headers_ref="X-API-Key=abc123"
        )

        assert _transport(capability).headers == {"X-API-Key": "abc123"}

    def test_the_client_carries_no_credential_when_the_server_declares_none(self) -> None:
        """The no-auth path is unchanged, which is what keeps artifacts portable."""
        capability = CompiledMcpCapability(server="orders", url=_URL)

        transport = _transport(capability)

        assert not getattr(transport, "headers", None)
        assert getattr(transport, "auth", None) is None


class TestOauthStrategy:
    """``kind: oauth`` runs the MCP client's own flow, not one loom wrote."""

    def test_the_client_mounts_its_own_oauth_flow(self) -> None:
        from fastmcp.client.auth.oauth import OAuth

        capability = CompiledMcpCapability(
            server="catalog",
            url="https://catalog.example.com/mcp",
            auth=CompiledRemoteAuth(kind="oauth"),
        )

        assert isinstance(_transport(capability).auth, OAuth)


class TestBearerStrategy:
    """``kind: bearer`` is the shape configuration cannot express by hand."""

    def test_the_transport_keeps_the_object_the_strategy_built(self) -> None:
        """fastmcp special-cases only OAuth and strings; everything else passes through."""
        built = shared_mcp_auth(_BEARER.server, _BEARER.auth)

        assert _transport(_BEARER).auth is built

    @pytest.mark.parametrize("flavour", _FLAVOURS)
    def test_the_request_carries_authorization_bearer_across_both_libraries(
        self, flavour: str
    ) -> None:
        """Asserted on the wire, through the client the transport really drives."""
        auth = _transport(_BEARER).auth

        assert _header_on_the_wire(flavour, auth, "Authorization") == f"Bearer {_TOKEN}"


class TestStaticStrategy:
    """``kind: static`` is the ``auth`` block's spelling of the ``headers_ref`` shorthand."""

    def test_the_transport_keeps_the_object_the_strategy_built(self) -> None:
        built = shared_mcp_auth(_STATIC.server, _STATIC.auth)

        assert _transport(_STATIC).auth is built

    @pytest.mark.parametrize("flavour", _FLAVOURS)
    def test_the_request_carries_the_fixed_header_across_both_libraries(self, flavour: str) -> None:
        auth = _transport(_STATIC).auth

        assert _header_on_the_wire(flavour, auth, "X-API-Key") == "abc123"


class TestThirdPartyStrategy:
    """The extension point is only real if someone who is not loom can use it."""

    def test_the_client_receives_the_strategy_built_with_its_own_settings(
        self, tmp_path: Path
    ) -> None:
        capability = CompiledMcpCapability(
            server="orders",
            url=_URL,
            auth=CompiledRemoteAuth(
                kind="agent-session",
                settings=(
                    ("session_url", "https://orders.example.com/auth/agent/session"),
                    ("bootstrap_ref", "/agents/prod/agent-sales"),
                ),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            auth = _transport(capability).auth

        assert (type(auth).__name__, auth.bootstrap_ref) == (
            "AgentSessionAuth",
            "/agents/prod/agent-sales",
        )

    def test_two_agents_of_the_same_server_share_a_single_instance(self, tmp_path: Path) -> None:
        """Identity, not equality: one credential per server, however many agents hold it."""
        capability = CompiledMcpCapability(
            server="orders",
            url=_URL,
            auth=CompiledRemoteAuth(
                kind="agent-session",
                settings=(
                    ("session_url", "https://orders.example.com/auth/agent/session"),
                    ("bootstrap_ref", "/agents/prod/agent-sales"),
                ),
            ),
        )
        # Two grants of the same server, as two agents' plans carry them: the
        # filters differ, the server does not.
        first_agent = CompiledMcpCapability(
            server=capability.server, url=capability.url, auth=capability.auth, include=("read_*",)
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            shared = _transport(capability).auth
            other = _transport(first_agent).auth

        assert shared is other
