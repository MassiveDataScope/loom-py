"""Deployment configuration contract for ``loom.ai.config`` (T031).

Pins the Tier-2 validation rules of the data model: provider-incomplete model
bindings, literal secrets in ``credentials_ref``, empty A2A exposure, the
mandatory per-endpoint ``auth``, and the documented ``AiConfig`` defaults.

Since artifacts *name* remote servers and agents instead of locating them, this
is the only layer where a URL, a credential reference or a per-call deadline
exists — so it is also the only layer where they can be rejected.

Assertions are made on error *codes* (``AgentErrorCode``), never on wording,
except where the contract is precisely about the message (secret absence).
"""

from __future__ import annotations

from typing import get_args

import msgspec
import pytest

from loom.ai.config import (
    A2AAgentConfig,
    A2AConfig,
    AgentEndpointConfig,
    AiConfig,
    McpServerConfig,
)
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import OUTPUT_MODES, InferenceTarget, OutputMode
from loom.core.model.redaction import Dsn, RedactedMap, Secret


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


def _complete_target() -> InferenceTarget:
    """Build a binding complete for its provider."""
    return InferenceTarget(provider="openai", model="gpt-test")


def _config(models: dict[str, InferenceTarget]) -> AiConfig:
    """Build a minimal ``AiConfig`` around the given model bindings."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models=models,
    )


def _config_with(**overrides: object) -> AiConfig:
    """Build a valid ``AiConfig`` with the registry under test substituted in."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": _complete_target()},
        **overrides,  # type: ignore[arg-type]
    )


_UNSAFE_URLS: list[str] = [
    "http://knowledge.example.com/mcp",
    "https://user:pass@knowledge.example.com/mcp",
    "https://knowledge.example.com/mcp?token=abc",
    "not a url",
    "https:///mcp",
]
_UNSAFE_URL_IDS: list[str] = [
    "plain_http",
    "userinfo",
    "query_string",
    "malformed",
    "no_host",
]


class TestModelBindingValidation:
    def test_fails_with_inference_target_incomplete_when_bedrock_declares_no_region(
        self,
    ) -> None:
        """A bedrock binding without ``region`` is unusable and must be rejected."""
        incomplete = InferenceTarget(provider="bedrock", model="anthropic.claude-v1")

        with pytest.raises(AgentCompilationError) as excinfo:
            _config({"default": incomplete})

        assert AgentErrorCode.INFERENCE_TARGET_INCOMPLETE in _codes(excinfo.value)

    @pytest.mark.parametrize("mode", ["prompted", "xml"])
    def test_fails_with_output_mode_unknown_when_the_mode_is_neither_tool_nor_native(
        self, mode: str
    ) -> None:
        """Only ``tool`` and ``native`` are offered; the issue names the role and both."""
        target = InferenceTarget(provider="openai", model="gpt-test", output_mode=mode)

        with pytest.raises(AgentCompilationError) as excinfo:
            _config({"reporting": target})

        issue = next(
            issue
            for issue in excinfo.value.issues
            if issue.code is AgentErrorCode.OUTPUT_MODE_UNKNOWN
        )
        assert "reporting" in issue.message
        assert "tool, native" in issue.message

    @pytest.mark.parametrize("mode", ["tool", "native", None])
    def test_accepts_the_binding_when_output_mode_is_valid_or_undeclared(
        self, mode: str | None
    ) -> None:
        """A declared valid mode, or none at all, loads without issues."""
        target = InferenceTarget(provider="openai", model="gpt-test", output_mode=mode)

        config = _config({"default": target})

        assert config.models["default"].output_mode == mode

    def test_a_binding_can_ask_for_whole_answers_instead_of_streamed_ones(self) -> None:
        """``streaming: false`` loads for that role alone; every other binding streams."""
        document = b"""
engine: pydantic-ai
specs: ["ai/agents/*/agent.yaml"]
models:
  reporting:
    provider: openai
    model: gpt-test
    streaming: false
  default:
    provider: openai
    model: gpt-test
"""

        config = msgspec.yaml.decode(document, type=AiConfig)

        assert config.models["reporting"].streaming is False
        assert config.models["default"].streaming is True
        assert "streaming=False" in repr(config.models["reporting"])

    def test_fails_with_output_mode_unknown_when_the_mode_arrives_via_yaml(self) -> None:
        """The decode path reports the loom issue, not a raw msgspec error.

        The struct field is deliberately ``str``: a ``Literal`` would make
        msgspec refuse the value during the decode, before ``__post_init__``
        could name the role in an ``OUTPUT_MODE_UNKNOWN`` issue.
        """
        document = b"""
engine: pydantic-ai
specs: ["ai/agents/*/agent.yaml"]
models:
  reporting:
    provider: openai
    model: gpt-test
    output_mode: prompted
"""

        with pytest.raises(AgentCompilationError) as excinfo:
            msgspec.yaml.decode(document, type=AiConfig)

        issue = next(
            issue
            for issue in excinfo.value.issues
            if issue.code is AgentErrorCode.OUTPUT_MODE_UNKNOWN
        )
        assert "reporting" in issue.message
        assert "tool, native" in issue.message

    def test_the_mode_constants_derive_from_the_type(self) -> None:
        """``OUTPUT_MODES`` and ``OutputMode`` cannot drift: one derives from the other."""
        assert get_args(OutputMode) == OUTPUT_MODES

    def test_the_repr_shows_output_mode_when_declared(self) -> None:
        """``output_mode`` is not a secret, so the redacting repr shows it."""
        target = InferenceTarget(provider="openai", model="gpt-test", output_mode="native")

        assert "output_mode='native'" in repr(target)


class TestLiteralSecretRejection:
    @pytest.mark.parametrize(
        "literal_secret",
        [
            "AKIAIOSFODNN7EXAMPLE",
            "sk-abc123def456ghi789",
            "this value has spaces",
            "https://user:pass@vault.example.com/creds",
        ],
        ids=["aws_access_key", "sk_token", "with_spaces", "url_userinfo"],
    )
    def test_fails_when_credentials_ref_is_a_literal_secret(
        self,
        literal_secret: str,
    ) -> None:
        """A literal secret in ``credentials_ref`` is never accepted (FR-018)."""
        target = InferenceTarget(
            provider="openai",
            model="gpt-test",
            credentials_ref=literal_secret,
        )

        with pytest.raises(AgentCompilationError):
            _config({"default": target})

    @pytest.mark.parametrize(
        "literal_secret",
        [
            "AKIAIOSFODNN7EXAMPLE",
            "sk-abc123def456ghi789",
            "this value has spaces",
            "https://user:pass@vault.example.com/creds",
        ],
        ids=["aws_access_key", "sk_token", "with_spaces", "url_userinfo"],
    )
    def test_the_message_does_not_contain_the_secret_when_a_literal_is_rejected(
        self,
        literal_secret: str,
    ) -> None:
        """The rejection itself must not leak the full secret it rejects."""
        target = InferenceTarget(
            provider="openai",
            model="gpt-test",
            credentials_ref=literal_secret,
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config({"default": target})

        assert literal_secret not in str(excinfo.value)


class TestA2AConfig:
    def test_fails_with_a2a_expose_empty_when_expose_is_empty(self) -> None:
        """Empty ``expose`` means none, never all (FR-041a); it must be rejected."""
        address = Dsn("https://agents.example.com")
        with pytest.raises(AgentCompilationError) as excinfo:
            A2AConfig(base_url=address, expose=())

        assert AgentErrorCode.A2A_EXPOSE_EMPTY in _codes(excinfo.value)


class TestAgentEndpointConfig:
    def test_fails_construction_when_auth_is_not_declared(self) -> None:
        """``auth`` has no default: omitting it must fail at construction."""
        with pytest.raises(TypeError):
            AgentEndpointConfig(enabled=True)  # type: ignore[call-arg]

    def test_allow_anonymous_is_false_when_not_declared(self) -> None:
        """Anonymous access is opt-in per agent (FR-045a)."""
        endpoint = AgentEndpointConfig(enabled=True, auth="oidc")

        assert endpoint.allow_anonymous is False

    def test_a_blank_auth_fails_the_config_with_endpoint_auth_missing(self) -> None:
        """An exposed agent must name its authentication; whitespace names none."""
        endpoints = {"triage": AgentEndpointConfig(enabled=True, auth="  ")}

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(endpoints=endpoints)

        (issue,) = excinfo.value.issues
        assert issue.code is AgentErrorCode.ENDPOINT_AUTH_MISSING
        assert issue.message == "triage: HTTP exposure requires a named authentication"
        assert (issue.component, issue.field) == ("triage", "auth")


class TestAiConfigDefaults:
    @pytest.fixture
    def config(self) -> AiConfig:
        """Build a minimal valid config relying on every default."""
        return _config({"default": _complete_target()})

    def test_startup_timeout_ms_defaults_when_not_declared(
        self,
        config: AiConfig,
    ) -> None:
        assert config.startup_timeout_ms == 10000

    def test_max_concurrent_runs_defaults_when_not_declared(
        self,
        config: AiConfig,
    ) -> None:
        assert config.max_concurrent_runs == 8

    def test_max_prompt_bytes_defaults_when_not_declared(
        self,
        config: AiConfig,
    ) -> None:
        assert config.max_prompt_bytes == 65536

    def test_health_cache_ttl_ms_defaults_when_not_declared(
        self,
        config: AiConfig,
    ) -> None:
        assert config.health_cache_ttl_ms == 5000

    def test_remote_clients_defaults_to_required_when_not_declared(
        self,
        config: AiConfig,
    ) -> None:
        """Start-up keeps failing on an unreachable remote unless opted out (D4)."""
        assert config.remote_clients == "required"

    def test_a2a_is_none_when_not_declared(self, config: AiConfig) -> None:
        """Absent ``a2a`` means no card and no A2A endpoints (FR-041)."""
        assert config.a2a is None

    def test_endpoints_is_empty_when_not_declared(self, config: AiConfig) -> None:
        """An agent absent from ``endpoints`` is never mounted (FR-029a)."""
        assert dict(config.endpoints) == {}


class TestMcpServerRegistry:
    """``ai.mcp_servers`` is where an artifact's ``server:`` name is located."""

    @pytest.mark.parametrize("url", _UNSAFE_URLS, ids=_UNSAFE_URL_IDS)
    def test_fails_with_mcp_url_invalid_when_the_url_is_not_safe(self, url: str) -> None:
        """Only a plain ``https`` URL without userinfo or query string is accepted."""
        mcp_servers = {"knowledge": McpServerConfig(url=Dsn(url))}

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers=mcp_servers)

        assert AgentErrorCode.MCP_URL_INVALID in _codes(excinfo.value)

    def test_the_message_does_not_contain_the_userinfo_when_the_url_is_rejected(self) -> None:
        """The rejection must not leak the credential embedded in the URL."""
        mcp_servers = {
            "knowledge": McpServerConfig(url=Dsn("https://user:s3cr3t@knowledge.example.com/mcp"))
        }

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers=mcp_servers)

        assert "s3cr3t" not in str(excinfo.value)

    def test_fails_with_credentials_inline_when_headers_ref_is_a_literal_secret(self) -> None:
        """``headers_ref`` references headers; it never carries them (FR-018)."""
        server = McpServerConfig(
            url=Dsn("https://knowledge.example.com/mcp"),
            headers_ref=Secret("sk-abc123def456ghi789"),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"knowledge": server})

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_the_message_does_not_contain_the_secret_when_headers_ref_is_rejected(self) -> None:
        """The rejection itself must not leak the very secret it rejects."""
        literal = "sk-abc123def456ghi789"
        server = McpServerConfig(
            url=Dsn("https://knowledge.example.com/mcp"), headers_ref=Secret(literal)
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"knowledge": server})

        assert literal not in str(excinfo.value)

    @pytest.mark.parametrize("timeout_ms", [0, -1, 600001], ids=["zero", "negative", "above_max"])
    def test_fails_with_policy_out_of_range_when_the_timeout_is_out_of_range(
        self,
        timeout_ms: int,
    ) -> None:
        """A per-call deadline outside ``1..600000`` is a configuration fault."""
        server = McpServerConfig(
            url=Dsn("https://knowledge.example.com/mcp"), timeout_ms=timeout_ms
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"knowledge": server})

        assert AgentErrorCode.POLICY_OUT_OF_RANGE in _codes(excinfo.value)

    def test_accepts_the_server_when_the_url_and_the_reference_are_valid(self) -> None:
        """The happy case: an https URL, a reference-shaped secret and a sane deadline."""
        server = McpServerConfig(
            url=Dsn("https://knowledge.example.com/mcp"),
            headers_ref=Secret("ai/knowledge/headers"),
            timeout_ms=15000,
        )

        config = _config_with(mcp_servers={"knowledge": server})

        assert config.mcp_servers["knowledge"] == server

    def test_timeout_ms_defaults_when_not_declared(self) -> None:
        """The documented per-call deadline applies when the deployment stays silent."""
        assert McpServerConfig(url=Dsn("https://knowledge.example.com/mcp")).timeout_ms == 20000

    def test_mcp_servers_is_empty_when_not_declared(self) -> None:
        """No server is reachable by default; every name must be declared."""
        assert dict(_config_with().mcp_servers) == {}


def _mcp_fields(fields: dict[str, object]) -> dict[str, object]:
    """Wrap address, header reference and free mappings the struct now stores."""
    prepared = dict(fields)
    url = prepared.get("url")
    if isinstance(url, str):
        prepared["url"] = Dsn(url)
    headers_ref = prepared.get("headers_ref")
    if isinstance(headers_ref, str):
        prepared["headers_ref"] = Secret(headers_ref)
    for name in ("auth", "env"):
        value = prepared.get(name)
        if isinstance(value, dict):
            prepared[name] = RedactedMap(value)
    return prepared


def _stdio_server(**overrides: object) -> McpServerConfig:
    """Build a valid ``transport: stdio`` server with the field under test substituted in."""
    fields: dict[str, object] = {
        "transport": "stdio",
        "command": "uvx",
        "args": ("mcp-server-fetch",),
    }
    fields.update(overrides)
    return McpServerConfig(**_mcp_fields(fields))  # type: ignore[arg-type]


def _issues_with(error: AgentCompilationError, code: AgentErrorCode) -> list[str]:
    """Extract the messages of the issues carrying ``code``."""
    return [issue.message for issue in error.issues if issue.code is code]


class TestMcpStdioTransport:
    """``transport: stdio`` runs the server as a subprocess of the worker (US1, US2)."""

    def test_accepts_stdio_with_command_and_args_and_no_url(self) -> None:
        """A stdio server is located by its command, never by a URL (FR-001, FR-004)."""
        server = _stdio_server()

        config = _config_with(mcp_servers={"search": server})

        assert config.mcp_servers["search"].transport == "stdio"
        assert config.mcp_servers["search"].url is None

    def test_fails_with_transport_invalid_and_a_security_note_when_stdio_carries_headers_ref(
        self,
    ) -> None:
        """No connection exists to authenticate; the message says what stdio implies (FR-006)."""
        server = _stdio_server(headers_ref="ai/search/headers")

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"search": server})

        messages = _issues_with(excinfo.value, AgentErrorCode.MCP_TRANSPORT_INVALID)
        assert len(messages) == 1
        assert "subprocess of this worker" in messages[0]
        assert "no connection to authenticate" in messages[0]

    def test_fails_with_transport_invalid_when_stdio_carries_auth(self) -> None:
        """An ``auth`` strategy authenticates a connection; stdio opens none (FR-005)."""
        server = _stdio_server(auth={"kind": "bearer", "token_ref": "ai/search/token"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"search": server})

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_TRANSPORT_INVALID]

    @pytest.mark.parametrize(
        "overrides",
        [
            {"command": None},
            {"command": ""},
            {"url": "https://search.example.com/mcp"},
        ],
        ids=["no_command", "empty_command", "with_url"],
    )
    def test_fails_with_transport_invalid_when_stdio_is_incoherent(
        self, overrides: dict[str, object]
    ) -> None:
        """stdio requires a non-empty ``command`` and refuses ``url`` (FR-004)."""
        mcp_servers = {"search": _stdio_server(**overrides)}

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers=mcp_servers)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_TRANSPORT_INVALID]

    @pytest.mark.parametrize(
        "overrides",
        [
            {"command": "uvx"},
            {"args": ("mcp-server-fetch",)},
            {"env": {"HOME": "/tmp"}},
            {"url": None},
        ],
        ids=["with_command", "with_args", "with_env", "no_url"],
    )
    def test_fails_with_transport_invalid_when_http_is_incoherent(
        self, overrides: dict[str, object]
    ) -> None:
        """http requires ``url`` and refuses the subprocess fields (FR-003)."""
        fields: dict[str, object] = {"url": "https://search.example.com/mcp"}
        fields.update(overrides)
        mcp_servers = {"search": McpServerConfig(**_mcp_fields(fields))}  # type: ignore[arg-type]

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers=mcp_servers)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_TRANSPORT_INVALID]

    def test_fails_with_transport_invalid_listing_the_accepted_ones_for_an_unknown_transport(
        self,
    ) -> None:
        """An unknown transport is refused and the message names the accepted ones (FR-002)."""
        server = McpServerConfig(transport="ws", url=Dsn("wss://search.example.com/mcp"))

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"search": server})

        messages = _issues_with(excinfo.value, AgentErrorCode.MCP_TRANSPORT_INVALID)
        assert len(messages) == 1
        assert "http" in messages[0]
        assert "stdio" in messages[0]

    @pytest.mark.parametrize("literal", ["sk abc", "sk{x}"], ids=["with_space", "with_brace"])
    def test_fails_with_credentials_inline_in_env_without_repeating_the_value(
        self, literal: str
    ) -> None:
        """A value shaped like a broken interpolation is refused; the message omits it (FR-007)."""
        server = _stdio_server(env={"API_KEY": literal})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"search": server})

        issues = [
            issue
            for issue in excinfo.value.issues
            if issue.code is AgentErrorCode.MCP_CREDENTIALS_INLINE
        ]
        assert [issue.field for issue in issues] == ["env.API_KEY"]
        assert literal not in str(excinfo.value)

    def test_accepts_a_token_already_resolved_in_env(self) -> None:
        """``env`` carries values already resolved by the secrets resolver (FR-007)."""
        server = _stdio_server(env={"GITHUB_TOKEN": "ghp_abc"})

        config = _config_with(mcp_servers={"search": server})

        assert config.mcp_servers["search"].env == {"GITHUB_TOKEN": "ghp_abc"}

    def test_fails_with_transport_invalid_when_the_env_name_is_not_valid(self) -> None:
        """An environment variable name is an identifier; anything else is a typo (FR-007)."""
        server = _stdio_server(env={"BAD KEY": "value"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"search": server})

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_TRANSPORT_INVALID]
        assert [issue.field for issue in excinfo.value.issues] == ["env.BAD KEY"]

    def test_fails_with_policy_out_of_range_when_the_timeout_is_zero_under_stdio(self) -> None:
        """The per-call deadline is bounded whatever the transport (FR-003, FR-004)."""
        server = _stdio_server(timeout_ms=0)

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"search": server})

        assert _codes(excinfo.value) == [AgentErrorCode.POLICY_OUT_OF_RANGE]

    def test_transport_defaults_to_http_when_not_declared(self) -> None:
        """A server declared today keeps its meaning: an HTTP endpoint (FR-001)."""
        assert McpServerConfig(url=Dsn("https://search.example.com/mcp")).transport == "http"


class TestA2AAgentRegistry:
    """``ai.a2a_agents`` is where an artifact's ``agent:`` name is located."""

    @pytest.mark.parametrize("url", _UNSAFE_URLS, ids=_UNSAFE_URL_IDS)
    def test_fails_with_a2a_url_invalid_when_the_url_is_not_safe(self, url: str) -> None:
        """Remote agents are held to the same URL rules as remote tool servers."""
        a2a_agents = {"translations": A2AAgentConfig(url=Dsn(url))}

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(a2a_agents=a2a_agents)

        assert AgentErrorCode.A2A_URL_INVALID in _codes(excinfo.value)

    def test_fails_with_credentials_inline_when_headers_ref_is_a_literal_secret(self) -> None:
        """``headers_ref`` references headers; it never carries them (FR-018)."""
        agent = A2AAgentConfig(
            url=Dsn("https://translations.example.com/a2a"),
            headers_ref=Secret("AKIAIOSFODNN7EXAMPLE"),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(a2a_agents={"translations": agent})

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_accepts_the_agent_when_the_url_and_the_reference_are_valid(self) -> None:
        """The happy case: an https URL and a reference-shaped secret."""
        agent = A2AAgentConfig(
            url=Dsn("https://translations.example.com/a2a"),
            headers_ref=Secret("ai/translations/headers"),
        )

        config = _config_with(a2a_agents={"translations": agent})

        assert config.a2a_agents["translations"] == agent

    def test_a2a_agents_is_empty_when_not_declared(self) -> None:
        """No remote agent is reachable by default; every name must be declared."""
        assert dict(_config_with().a2a_agents) == {}


class TestSkillsRoot:
    def test_skills_root_is_none_when_not_declared(self) -> None:
        """A bare library name needs a root; without one the compiler must complain."""
        assert _config_with().skills_root is None

    def test_keeps_the_skills_root_when_declared(self) -> None:
        """The root is deployment-owned; the artifact never carries a path."""
        assert _config_with(skills_root="/srv/app/skills").skills_root == "/srv/app/skills"


class TestAggregatedRegistryIssues:
    def test_accumulates_one_issue_per_invalid_registry_entry_when_both_fail(self) -> None:
        """One raise reports every faulty entry, never the first one only (FR-011)."""
        mcp_servers = {"knowledge": McpServerConfig(url=Dsn("http://knowledge.example.com/mcp"))}
        a2a_agents = {
            "translations": A2AAgentConfig(url=Dsn("http://translations.example.com/a2a"))
        }

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers=mcp_servers, a2a_agents=a2a_agents)

        assert set(_codes(excinfo.value)) == {
            AgentErrorCode.MCP_URL_INVALID,
            AgentErrorCode.A2A_URL_INVALID,
        }


class TestRemoteClients:
    """``ai.remote_clients`` decides whether a remote that will not open is fatal."""

    @pytest.mark.parametrize("mode", ["required", "optional"])
    def test_keeps_the_mode_when_it_is_one_of_the_valid_ones(self, mode: str) -> None:
        assert _config_with(remote_clients=mode).remote_clients == mode

    def test_fails_with_remote_clients_unknown_when_the_mode_does_not_exist(self) -> None:
        """An unknown mode must fail the config load, not silently mean ``required``."""
        document = b"""
engine: pydantic-ai
specs:
  - ai/agents/*/agent.yaml
models:
  default:
    provider: openai
    model: gpt-test
remote_clients: maybe
"""

        with pytest.raises(AgentCompilationError) as excinfo:
            msgspec.yaml.decode(document, type=AiConfig)

        assert _codes(excinfo.value) == [AgentErrorCode.REMOTE_CLIENTS_UNKNOWN]

    def test_the_message_names_the_key_and_both_modes_when_rejecting_the_value(self) -> None:
        """The reader must learn what to write without opening the source."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(remote_clients="maybe")

        message = excinfo.value.issues[0].message
        assert "ai.remote_clients" in message
        assert "maybe" in message
        assert "required" in message
        assert "optional" in message


class TestMaxConcurrentRuns:
    """``ai.max_concurrent_runs`` below 1 would admit no run at all."""

    def test_keeps_the_default_value(self) -> None:
        assert _config_with().max_concurrent_runs == 8

    @pytest.mark.parametrize("runs", [1, 2, 64])
    def test_keeps_a_value_of_at_least_one(self, runs: int) -> None:
        assert _config_with(max_concurrent_runs=runs).max_concurrent_runs == runs

    @pytest.mark.parametrize("runs", [0, -1])
    def test_fails_with_max_concurrent_runs_invalid_below_one(self, runs: int) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(max_concurrent_runs=runs)

        assert _codes(excinfo.value) == [AgentErrorCode.MAX_CONCURRENT_RUNS_INVALID]

    def test_the_message_names_the_key_and_the_minimum(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(max_concurrent_runs=0)

        message = excinfo.value.issues[0].message
        assert "ai.max_concurrent_runs" in message
        assert "minimum of 1" in message


class TestMaxAgentDepth:
    """``ai.max_agent_depth`` below 1 would refuse every run, top-level included."""

    def test_keeps_the_default_value(self) -> None:
        assert _config_with().max_agent_depth == 1

    @pytest.mark.parametrize("depth", [1, 2, 5])
    def test_keeps_a_value_of_at_least_one(self, depth: int) -> None:
        assert _config_with(max_agent_depth=depth).max_agent_depth == depth

    @pytest.mark.parametrize("depth", [0, -1])
    def test_fails_with_max_agent_depth_invalid_below_one(self, depth: int) -> None:
        """Zero would silently refuse the top-level run too, not only nesting."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(max_agent_depth=depth)

        assert _codes(excinfo.value) == [AgentErrorCode.MAX_AGENT_DEPTH_INVALID]

    def test_the_message_names_the_key_and_the_minimum(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(max_agent_depth=0)

        message = excinfo.value.issues[0].message
        assert "ai.max_agent_depth" in message
        assert "0" in message
