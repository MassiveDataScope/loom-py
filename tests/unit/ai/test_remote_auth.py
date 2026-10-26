"""Outbound authentication: what the deployment declares, and what loom refuses.

Two layers are pinned here and nothing else:

* ``loom.ai.config`` — the compile-time refusals, applied identically to an
  MCP server and to an A2A agent. An unregistered strategy, a literal secret
  anywhere in the ``auth`` block, and ``headers_ref`` together with ``auth``
  are all faults of the deployment, so they must be found while the
  configuration is decoded rather than at the first message in production.
* ``loom.ai.remote_auth`` — resolution itself: the strategy name is looked up in
  a real entry-point group, constructed from its settings, and the instance is
  shared per endpoint.

The third-party strategy is installed as a genuine distribution (see
``tests.helpers.remote_auth_plugin``) rather than by patching the loader: the
extension point only means something if someone who is not loom can use it.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.ai.compiler import CompiledRemoteAuth
from loom.ai.config import A2AAgentConfig, AiConfig, McpServerConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.remote_auth import (
    _checked,
    bearer_token,
    headers_from_ref,
    is_strategy_registered,
    registered_strategy_names,
    shared_a2a_auth,
    shared_mcp_auth,
    standard_oauth,
    static_headers,
)
from loom.core.model.redaction import Dsn, RedactedMap, Secret

from ...helpers.remote_auth_plugin import third_party_strategy

_URL = "https://knowledge.example.com/mcp"
_AGENT_URL = "https://market.example.com/a2a"


def _config_with(server: McpServerConfig) -> AiConfig:
    """Build a valid ``AiConfig`` around the one server under test."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": InferenceTarget(provider="openai", model="gpt-test")},
        mcp_servers={"knowledge": server},
    )


def _config_with_agent(agent: A2AAgentConfig) -> AiConfig:
    """Build a valid ``AiConfig`` around the one remote agent under test."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": InferenceTarget(provider="openai", model="gpt-test")},
        a2a_agents={"market": agent},
    )


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    return [issue.code for issue in error.issues]


@pytest.fixture(autouse=True)
def _isolated_sharing() -> Iterator[None]:
    """Empty the per-endpoint sharing map so one test cannot seed another.

    Reaching into the private map is deliberate: the sharing is process-wide by
    design, and a test asserting *identity* would otherwise depend on whichever
    test ran first.
    """
    from loom.ai import remote_auth

    remote_auth._STRATEGIES._by_endpoint.clear()
    yield
    remote_auth._STRATEGIES._by_endpoint.clear()


class TestAuthBlockConfiguration:
    """``ai.mcp_servers.<name>.auth`` is refused before anything connects."""

    def test_fails_with_auth_strategy_unknown_when_the_strategy_is_not_registered(
        self,
    ) -> None:
        """A name nobody registers must fail at compile, not at the first message."""
        server = McpServerConfig(url=Dsn(_URL), auth=RedactedMap({"kind": "nobody-registers-this"}))

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN in _codes(excinfo.value)

    def test_the_message_names_the_strategy_and_the_registered_ones(self) -> None:
        """The operator must be able to act on the message without reading loom."""
        server = McpServerConfig(url=Dsn(_URL), auth=RedactedMap({"kind": "nobody-registers-this"}))

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        message = str(excinfo.value)
        assert "nobody-registers-this" in message
        assert "oauth" in message
        assert "static" in message

    def test_fails_with_auth_strategy_unknown_when_the_block_declares_no_kind(self) -> None:
        """A block without ``kind`` names no strategy at all."""
        server = McpServerConfig(
            url=Dsn(_URL), auth=RedactedMap({"session_url": "https://auth.example.com/token"})
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN in _codes(excinfo.value)

    def test_fails_with_credentials_inline_when_a_setting_carries_a_literal_secret(self) -> None:
        """The inline-credential rule covers the whole block, not just ``headers_ref``."""
        server = McpServerConfig(
            url=Dsn(_URL),
            auth=RedactedMap({"kind": "oauth", "bootstrap_ref": "sk-abc123def456ghi789"}),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_the_message_does_not_contain_the_secret_it_rejects(self) -> None:
        """The rejection must not leak the very secret it rejects."""
        literal = "sk-abc123def456ghi789"
        server = McpServerConfig(
            url=Dsn(_URL), auth=RedactedMap({"kind": "oauth", "bootstrap_ref": literal})
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert literal not in str(excinfo.value)

    def test_fails_with_auth_conflict_alongside_headers_ref(self) -> None:
        """Two credentials on one connection is ambiguous, so it is refused."""
        server = McpServerConfig(
            url=Dsn(_URL),
            headers_ref=Secret("X-API-Key=abc123"),
            auth=RedactedMap({"kind": "oauth"}),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_AUTH_CONFLICT in _codes(excinfo.value)

    def test_accepts_a_jwt_shaped_token_as_a_setting(self) -> None:
        """A JWT is base64url with dots: the inline-credential test must let it through."""
        server = McpServerConfig(
            url=Dsn(_URL),
            auth=RedactedMap(
                {"kind": "bearer", "token_ref": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhLWIifQ.s_g-9"}
            ),
        )

        config = _config_with(server)

        assert config.mcp_servers["knowledge"].auth is not None

    def test_rejects_a_setting_with_spaces_even_a_composed_bearer_header(self) -> None:
        """The composed header is exactly what must not live in configuration."""
        server = McpServerConfig(
            url=Dsn(_URL),
            auth=RedactedMap({"kind": "bearer", "token_ref": "Bearer eyJhbGciOiJIUzI1NiJ9"}),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_accepts_the_server_when_the_strategy_is_registered(self) -> None:
        """``oauth`` ships with loom, so it decodes without complaint."""
        server = McpServerConfig(url=Dsn(_URL), auth=RedactedMap({"kind": "oauth"}))

        config = _config_with(server)

        assert config.mcp_servers["knowledge"].auth == {"kind": "oauth"}

    def test_accepts_a_third_party_strategy_when_its_distribution_is_installed(
        self, tmp_path: Path
    ) -> None:
        """A deployment's own strategy is as valid as loom's own."""
        server = McpServerConfig(
            url=Dsn(_URL),
            auth=RedactedMap(
                {
                    "kind": "agent-session",
                    "session_url": "https://orders.example.com/auth/agent/session",
                    "bootstrap_ref": "/agents/prod/agent-sales",
                }
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            config = _config_with(server)

        assert config.mcp_servers["knowledge"].auth is not None


class TestHeadersFromRef:
    """``headers_ref`` reaches loom already resolved; this reads its payload."""

    def test_returns_the_header_when_the_value_is_a_name_value_pair(self) -> None:
        assert headers_from_ref("server 'kb'", "X-API-Key=abc123") == {"X-API-Key": "abc123"}

    def test_returns_empty_when_the_server_declares_no_credential(self) -> None:
        assert headers_from_ref("server 'kb'", None) == {}

    @pytest.mark.parametrize(
        "payload", ["just-a-name", "=abc123", "X-API-Key="], ids=["no_pair", "no_name", "no_value"]
    )
    def test_fails_with_headers_ref_invalid_when_the_value_is_not_a_pair(
        self, payload: str
    ) -> None:
        """A payload loom cannot turn into a header would silently send nothing."""
        with pytest.raises(AgentCompilationError) as excinfo:
            headers_from_ref("server 'kb'", payload)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_HEADERS_REF_INVALID]

    def test_the_message_does_not_contain_the_value_it_rejects(self) -> None:
        payload = "sk-abc123def456ghi789"

        with pytest.raises(AgentCompilationError) as excinfo:
            headers_from_ref("server 'kb'", payload)

        assert payload not in str(excinfo.value)


class TestStrategiesLoomRegisters:
    """Both are thin delegations: loom implements no login flow of its own."""

    def test_oauth_returns_the_sentinel_the_mcp_client_understands(self) -> None:
        assert standard_oauth() == "oauth"

    def test_static_adds_the_header_to_every_request(self) -> None:
        """The strategy is exercised as a client drives it, not by inspection."""
        httpx = pytest.importorskip("httpx")
        auth = static_headers(headers_ref="X-API-Key=abc123")

        request = auth(httpx.Request("GET", "https://knowledge.example.com/mcp"))

        assert request.headers["X-API-Key"] == "abc123"

    def test_bearer_presents_the_token_in_the_authorization_header(self) -> None:
        """The header the strategy composes is what configuration cannot carry."""
        httpx = pytest.importorskip("httpx")
        auth = bearer_token(token_ref="eyJhbGci.eyJzdWIi-abc_123")

        request = auth(httpx.Request("GET", "https://catalog.example.com/mcp"))

        assert request.headers["Authorization"] == "Bearer eyJhbGci.eyJzdWIi-abc_123"

    @pytest.mark.parametrize(
        "build",
        [
            lambda: bearer_token(token_ref="a.b-c_1"),
            lambda: static_headers(headers_ref="X-API-Key=abc123"),
        ],
        ids=["bearer", "static"],
    )
    def test_the_strategy_returns_the_request_it_receives(self, build: Any) -> None:
        """Both clients wrap the callable as ``yield self._func(request)``.

        A callable that mutated the request but returned ``None`` would send
        ``None`` instead of it, and no header assertion would notice.
        """
        httpx = pytest.importorskip("httpx")
        request = httpx.Request("GET", "https://knowledge.example.com/mcp")

        assert build()(request) is request

    def test_no_strategy_imports_an_http_library(self) -> None:
        """Checked in a fresh interpreter, which is the only place it means anything.

        ``loom.ai.config`` imports this module at load time, so an in-process
        check finds whatever the running test session already imported and
        passes however the strategies are written.
        """
        probe = subprocess.run(  # noqa: S603 - the interpreter running the suite
            [sys.executable, "-c", _NO_HTTP_IMPORT_PROBE],
            capture_output=True,
            text=True,
            check=False,
        )

        assert probe.returncode == 0, probe.stderr

    def test_loom_registers_oauth_bearer_and_static_and_nothing_else(self) -> None:
        """Loom hard-codes no vendor: the three names it ships are generic."""
        assert registered_strategy_names() == ["bearer", "oauth", "static"]


class TestWhatLoomAcceptsFromAStrategy:
    """``_checked`` accepts what both clients accept, and nothing else."""

    def test_accepts_a_callable(self) -> None:
        """The shape both clients wrap in their own ``FunctionAuth``."""

        def auth(request: Any) -> Any:
            return request

        assert _checked("callable-strategy", auth) is auth

    def test_accepts_an_object_with_auth_flow(self) -> None:
        """A class written against either flavour satisfies the same probe."""

        class _Flavoured:
            def auth_flow(self, request: Any) -> Any:
                yield request

        built = _Flavoured()

        assert _checked("agent-session", built) is built

    def test_accepts_the_mcp_clients_sentinel(self) -> None:
        assert _checked("oauth", "oauth") == "oauth"

    def test_fails_with_auth_strategy_invalid_when_it_is_none_of_the_three_shapes(self) -> None:
        """An object no client can use would otherwise connect unauthenticated."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _checked("agent-session", object())

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]


class TestSharedResolutionPerServer:
    """One instance per server: the credential belongs to the deployment."""

    def test_returns_none_when_the_server_declares_no_strategy(self) -> None:
        assert shared_mcp_auth("knowledge", None) is None

    def test_builds_the_third_party_strategy_with_its_settings(self, tmp_path: Path) -> None:
        """Settings become keyword arguments of the registered object."""
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            built = shared_mcp_auth("orders", auth)

        assert (built.session_url, built.bootstrap_ref) == (  # type: ignore[union-attr]
            "https://orders.example.com/auth/agent/session",
            "/agents/prod/agent-sales",
        )

    def test_shares_a_single_instance_when_two_calls_name_the_same_server(
        self, tmp_path: Path
    ) -> None:
        """Identity, not equality: a renewing strategy holds the live token."""
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            first = shared_mcp_auth("orders", auth)
            second = shared_mcp_auth("orders", auth)

        assert first is second

    def test_does_not_share_across_different_servers(self, tmp_path: Path) -> None:
        """Two servers are two credentials, however alike their settings look."""
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            orders = shared_mcp_auth("orders", auth)
            catalog = shared_mcp_auth("catalog", auth)

        assert orders is not catalog

    def test_fails_with_auth_strategy_invalid_when_the_strategy_rejects_its_settings(
        self, tmp_path: Path
    ) -> None:
        """A settings key the strategy does not take is a deployment fault, named as one."""
        auth = CompiledRemoteAuth(kind="agent-session", settings=(("unexpected", "value"),))

        with third_party_strategy(tmp_path, name="agent-session"):  # noqa: SIM117
            with pytest.raises(AgentCompilationError) as excinfo:
                shared_mcp_auth("orders", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]

    def test_is_strategy_registered_is_false_for_an_empty_name(self) -> None:
        assert is_strategy_registered("") is False


class TestStrategyResolutionFailures:
    """Every resolution failure reaches the deployment as a coded issue.

    The loader is faked here, unlike the rest of this module: two distributions
    claiming one strategy name cannot be installed side by side in the test
    environment, and the strategy that is missing at start-up was, by
    definition, present at decode.
    """

    @staticmethod
    def _install(monkeypatch: pytest.MonkeyPatch, dist_names: tuple[str, ...]) -> None:
        """Register ``kind='agent-session'`` once per name in ``dist_names``."""

        class _Dist:
            def __init__(self, name: str) -> None:
                self.name = name

        class _EntryPoint:
            group = "loom.ai.remote_auth"

            def __init__(self, dist_name: str) -> None:
                self.name = "agent-session"
                self.dist = _Dist(dist_name)

            def load(self) -> Any:
                return lambda request: request

        class _EntryPoints:
            def select(self, *, group: str) -> tuple[_EntryPoint, ...]:
                if group != "loom.ai.remote_auth":
                    return ()
                return tuple(_EntryPoint(name) for name in dist_names)

        from loom.core.plugins import entrypoints as entrypoints_module

        monkeypatch.setattr(entrypoints_module, "entry_points", _EntryPoints)

    def test_fails_with_auth_strategy_invalid_when_two_distributions_register_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._install(monkeypatch, ("loom-auth-alpha", "loom-auth-beta"))
        auth = CompiledRemoteAuth(kind="agent-session", settings=())

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_mcp_auth("orders", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]

    def test_the_message_names_both_distributions_on_a_duplicate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._install(monkeypatch, ("loom-auth-alpha", "loom-auth-beta"))
        auth = CompiledRemoteAuth(kind="agent-session", settings=())

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_mcp_auth("orders", auth)

        message = str(excinfo.value)
        assert "loom-auth-alpha" in message
        assert "loom-auth-beta" in message

    def test_fails_with_auth_strategy_invalid_when_it_is_no_longer_registered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A distribution uninstalled between decode and start-up is named, not crashed on."""
        self._install(monkeypatch, ())
        auth = CompiledRemoteAuth(kind="agent-session", settings=())

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_mcp_auth("orders", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]


class TestAuthBlockConfigurationForARemoteAgent:
    """``ai.a2a_agents.<name>.auth`` is held to exactly the MCP rules."""

    def test_fails_with_auth_strategy_unknown_when_the_strategy_is_not_registered(self) -> None:
        """Otherwise the agent would connect unauthenticated at the first delegation."""
        agent = A2AAgentConfig(
            url=Dsn(_AGENT_URL), auth=RedactedMap({"kind": "nobody-registers-this"})
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN in _codes(excinfo.value)

    def test_the_message_names_the_strategy_and_the_registered_ones(self) -> None:
        agent = A2AAgentConfig(
            url=Dsn(_AGENT_URL), auth=RedactedMap({"kind": "nobody-registers-this"})
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        message = str(excinfo.value)
        assert "nobody-registers-this" in message
        assert "bearer" in message
        assert "static" in message

    def test_fails_with_credentials_inline_when_a_setting_carries_a_literal_secret(self) -> None:
        agent = A2AAgentConfig(
            url=Dsn(_AGENT_URL),
            auth=RedactedMap({"kind": "bearer", "token_ref": "sk-abc123def456ghi789"}),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_the_message_does_not_contain_the_secret_it_rejects(self) -> None:
        literal = "sk-abc123def456ghi789"
        agent = A2AAgentConfig(
            url=Dsn(_AGENT_URL), auth=RedactedMap({"kind": "bearer", "token_ref": literal})
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert literal not in str(excinfo.value)

    def test_fails_with_auth_conflict_alongside_headers_ref(self) -> None:
        """Two credentials on one connection is as ambiguous here as it is for MCP."""
        agent = A2AAgentConfig(
            url=Dsn(_AGENT_URL),
            headers_ref=Secret("X-API-Key=abc123"),
            auth=RedactedMap({"kind": "bearer"}),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert AgentErrorCode.MCP_AUTH_CONFLICT in _codes(excinfo.value)

    def test_accepts_the_agent_when_the_strategy_is_registered(self) -> None:
        agent = A2AAgentConfig(
            url=Dsn(_AGENT_URL), auth=RedactedMap({"kind": "bearer", "token_ref": "a.b-c_1"})
        )

        config = _config_with_agent(agent)

        assert config.a2a_agents["market"].auth == {"kind": "bearer", "token_ref": "a.b-c_1"}

    def test_accepts_a_third_party_strategy_when_its_distribution_is_installed(
        self, tmp_path: Path
    ) -> None:
        """One group: a strategy registered for MCP is offered to A2A unchanged."""
        agent = A2AAgentConfig(
            url=Dsn(_AGENT_URL),
            auth=RedactedMap(
                {
                    "kind": "agent-session",
                    "session_url": "https://market.example.com/auth/agent/session",
                    "bootstrap_ref": "/agents/prod/agent-sales",
                }
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            config = _config_with_agent(agent)

        assert config.a2a_agents["market"].auth is not None


class TestSharedResolutionPerRemoteAgent:
    """One instance per configured agent, from the same registry MCP uses."""

    def test_returns_none_when_the_agent_declares_no_strategy(self) -> None:
        assert shared_a2a_auth("market", None) is None

    def test_builds_the_third_party_strategy_with_its_settings(self, tmp_path: Path) -> None:
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://market.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            built = shared_a2a_auth("market", auth)

        assert (built.session_url, built.bootstrap_ref) == (  # type: ignore[attr-defined]
            "https://market.example.com/auth/agent/session",
            "/agents/prod/agent-sales",
        )

    def test_shares_a_single_instance_when_two_agents_name_the_same_remote(
        self,
    ) -> None:
        """Identity, not equality: the credential belongs to the deployment."""
        auth = CompiledRemoteAuth(kind="bearer", settings=(("token_ref", "a.b-c_1"),))

        first = shared_a2a_auth("market", auth)
        second = shared_a2a_auth("market", auth)

        assert first is second

    def test_does_not_share_with_a_mcp_server_of_the_same_name(self) -> None:
        """A server and an agent registered alike are two endpoints, two credentials."""
        auth = CompiledRemoteAuth(kind="bearer", settings=(("token_ref", "a.b-c_1"),))

        assert shared_a2a_auth("orders", auth) is not shared_mcp_auth("orders", auth)

    def test_fails_with_auth_strategy_invalid_when_the_strategy_is_the_oauth_sentinel(
        self,
    ) -> None:
        """``oauth`` delegates to the MCP client's flow; A2A must refuse, not connect bare."""
        auth = CompiledRemoteAuth(kind="oauth")

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_a2a_auth("market", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]

    def test_fails_with_auth_strategy_invalid_when_the_strategy_rejects_its_settings(
        self, tmp_path: Path
    ) -> None:
        auth = CompiledRemoteAuth(kind="agent-session", settings=(("unexpected", "value"),))

        with third_party_strategy(tmp_path, name="agent-session"):  # noqa: SIM117
            with pytest.raises(AgentCompilationError) as excinfo:
                shared_a2a_auth("market", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]


_NO_HTTP_IMPORT_PROBE = """
import sys

from loom.ai.remote_auth import bearer_token, static_headers

bearer_token(token_ref="a.b-c_1")
static_headers(headers_ref="X-API-Key=abc123")

resident = sorted(name for name in ("httpx", "httpx2") if name in sys.modules)
assert not resident, f"remote_auth imported an HTTP library: {resident}"
"""
"""Source of the fresh interpreter that pins AC2.

Kept as a module constant so the probe reads as code rather than as an
argument, and so the test body stays about the assertion.
"""
