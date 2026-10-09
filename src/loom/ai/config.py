"""Typed configuration for the ``ai:`` deployment section.

Parsed via ``ctx.section(ConfigKey.AI, AiConfig)`` through the existing
:mod:`loom.core.config` loader, so ``${oc.env:...}`` interpolations and the
secret resolver apply before the decode, unchanged.  Validation is fail-fast
per struct and every issue carries a stable
:class:`~loom.ai.errors.AgentErrorCode`; :class:`AiConfig` aggregates the
issues found across every model role and endpoint and raises once.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from ipaddress import ip_address
from types import MappingProxyType
from urllib.parse import SplitResult, urlsplit

from msgspec import field

from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    a2a_base_url_invalid,
    a2a_expose_empty,
    a2a_url_invalid,
    endpoint_auth_missing,
    inference_target_incomplete,
    max_agent_depth_invalid,
    mcp_auth_conflict,
    mcp_auth_strategy_unknown,
    mcp_credentials_inline,
    mcp_transport_invalid,
    mcp_url_invalid,
    output_mode_unknown,
    policy_out_of_range,
    remote_clients_unknown,
)
from loom.ai.inference import OUTPUT_MODES, InferenceTarget
from loom.ai.pricing import ModelPrice
from loom.ai.remote_auth import is_strategy_registered, registered_strategy_names
from loom.core.model import LoomFrozenStruct

# Conservative allowlist for a secret *reference*: a name, path, ARN or key id
# the secrets resolver can look up.  It excludes whitespace, newlines, quotes,
# braces (so JSON blobs and PEM blocks never pass) — anything outside the
# pattern is rejected, fail-closed.
_REFERENCE_PATTERN = re.compile(r"^[A-Za-z0-9_/.:@=+-]+$")

# Known shapes of literal secret material that would otherwise satisfy the
# reference pattern: AWS access key ids, OpenAI-style keys, GitHub tokens.
_SECRET_MATERIAL_PREFIXES = ("AKIA", "sk-", "ghp_")

# A URL carrying userinfo (``scheme://user:pass@host``) embeds a credential.
_URL_USERINFO_RE = re.compile(r"://[^/]*@")

# Accepted values of ``ai.mcp_servers.<name>.transport``: a remote endpoint
# reached over HTTPS, or a subprocess of this worker spoken to over stdio.
_MCP_TRANSPORTS: tuple[str, ...] = ("http", "stdio")

# A valid environment variable name, as every POSIX shell and libc define it.
# ``re.ASCII`` keeps ``\w`` restricted to ``[A-Za-z0-9_]`` — POSIX names are
# ASCII-only, so the concise class must not admit Unicode word characters.
_ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_]\w*$", re.ASCII)

# Why ``headers_ref`` and ``auth`` are refused under ``transport: stdio``.
_STDIO_CREDENTIALS_REASON = (
    "'headers_ref' and 'auth' do not apply to transport 'stdio': the server runs "
    "as a subprocess of this worker, in the same container, with the identity, "
    "filesystem, network and instance credentials of the process itself; there "
    "is no connection to authenticate"
)

# The one reserved key of an ``auth`` block: it names the strategy, every other
# key is that strategy's own setting.
_AUTH_KIND_KEY = "kind"

# Accepted values of ``ai.remote_clients``: whether a remote client that will
# not open aborts start-up (``required``) or is dropped with a warning
# (``optional``).
_REMOTE_CLIENTS_MODES: tuple[str, ...] = ("required", "optional")

# Bounds of a single remote call, mirroring ``policies.tool_timeout_ms``.
_TIMEOUT_MS_MIN = 1
_TIMEOUT_MS_MAX = 600000

# Settings a provider requires directly in its ``InferenceTarget`` binding.
# Providers absent from this map need nothing beyond ``provider``/``model``
# at config time; their SDK-level settings are validated by the engine via
# ``loom.ai.registry.require_provider_setting``.  ``gateway`` designates a
# custom OpenAI-compatible gateway and therefore requires its ``endpoint``.
_REQUIRED_SETTINGS_BY_PROVIDER: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "bedrock": ("region",),
        "gateway": ("endpoint",),
    }
)


def _has_reference_shape(value: str) -> bool:
    """Report whether ``value`` has the structure of a single opaque token.

    Fail-closed: the value must match the conservative reference allowlist
    (which excludes spaces, newlines, quotes and ``{``, and therefore JSON
    blobs, ``BEGIN PRIVATE KEY`` blocks and unresolved interpolations) and must
    not be a URL carrying userinfo.
    """
    if _REFERENCE_PATTERN.fullmatch(value) is None:
        return False
    return _URL_USERINFO_RE.search(value) is None


def _is_credentials_reference(value: str) -> bool:
    """Report whether ``value`` looks like a secret reference, not a secret.

    Fail-closed heuristic (FR-018): the value must have the shape of a
    reference and must not match known literal-secret prefixes.
    """
    return _has_reference_shape(value) and not value.startswith(_SECRET_MATERIAL_PREFIXES)


_logger = logging.getLogger(__name__)


def _is_loopback(hostname: str | None) -> bool:
    """Whether *hostname* names this machine and nothing else.

    Args:
        hostname: Host component of a URL, or ``None`` when it declares none.

    Returns:
        ``True`` for ``localhost`` and for any address in a loopback range.
    """
    if hostname is None:
        return False
    if hostname == "localhost":
        return True
    try:
        return ip_address(hostname.strip("[]")).is_loopback
    except ValueError:
        return False


def _warn_plaintext_loopback(component: str, url: str) -> None:
    """Announce a plaintext loopback URL, which is allowed only because it is one.

    The exception is deliberate and narrow, so it must not be silent: a
    configuration that works on a laptop and is refused in staging is worth
    hearing about at start-up rather than at deploy time.

    Args:
        component: Configuration path the URL came from.
        url: The accepted URL, already redacted.
    """
    parts = urlsplit(url)
    if parts.scheme != "http" or not _is_loopback(parts.hostname):
        return
    _logger.warning(
        "%s uses plaintext http at %s. Allowed because the traffic cannot leave "
        "this machine; the same URL on any other host is refused, so this "
        "configuration is local-only.",
        component,
        parts.hostname,
    )


def _url_fault(url: str) -> str | None:
    """Return why a remote URL is unsafe, or ``None`` when it is acceptable."""
    try:
        parts: SplitResult = urlsplit(url)
    except ValueError:
        return "the URL is malformed"
    if parts.scheme != "https":
        if parts.scheme == "http" and _is_loopback(parts.hostname):
            # Plaintext is acceptable only when the traffic cannot leave the
            # machine. Requiring TLS from a developer's own MCP server buys
            # nothing — there is no network to intercept — and costs a
            # self-signed certificate before anything can be tried locally.
            # Anywhere else the refusal stands: see _warn_plaintext_loopback,
            # which makes the exception audible rather than silent.
            return None
        return "the scheme must be https"
    if not parts.hostname:
        return "the URL declares no host"
    if parts.username is not None or parts.password is not None:
        return "the URL carries credentials in its userinfo"
    if parts.query:
        return "the URL carries a query string, which may embed credentials"
    return None


def _redact_url(url: str) -> str:
    """Strip userinfo and query so an invalid-URL message cannot leak a secret."""
    return _URL_USERINFO_RE.sub("://***@", url).split("?", 1)[0]


def _validate_model_binding(role: str, target: InferenceTarget) -> list[AgentCompilationIssue]:
    """Collect the issues of one ``ai.models.<role>`` binding."""
    issues: list[AgentCompilationIssue] = []
    required = _REQUIRED_SETTINGS_BY_PROVIDER.get(target.provider, ())
    for setting in required:
        if not getattr(target, setting):
            issues.append(inference_target_incomplete(role, setting))
    if target.output_mode is not None and target.output_mode not in OUTPUT_MODES:
        issues.append(output_mode_unknown(role, target.output_mode, OUTPUT_MODES))
    ref = target.credentials_ref
    if ref is not None and not _is_credentials_reference(ref):
        # The rejected value is deliberately absent from the issue: the error
        # message must not leak the very secret it rejects.
        issues.append(mcp_credentials_inline(f"model role '{role}'", "credentials_ref"))
    return issues


class McpServerConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """One named MCP server (``ai.mcp_servers.<name>``).

    Artifacts name a server; this is where the server lives.  Keeping the
    transport, the address or command, the credential reference and the
    deadline here is what lets the same artifact move between environments
    unchanged.

    Attributes:
        transport: ``http`` for a remote endpoint, ``stdio`` for a subprocess
            of this worker.  Each transport accepts its own fields and refuses
            the other's.
        url: ``https://`` server URL, free of userinfo and query string.
            Required under ``http``; refused under ``stdio``.
        headers_ref: Reference to headers resolved by the secrets resolver.
            Never a literal secret.  The resolved payload must be a single
            ``Name=value`` header pair, a shape checked at start-up rather than
            here; a bearer token belongs in ``auth: {kind: bearer}``.
            Mutually exclusive with ``auth``.
        auth: Named authentication strategy and its settings, flattened:
            ``kind`` selects an entry point registered in ``loom.ai.remote_auth``
            and every other key is passed to it as a keyword argument.
            Mutually exclusive with ``headers_ref``.
        timeout_ms: Deadline of a single call to this server.
        command: Executable that speaks MCP over its stdin/stdout.  Required
            under ``stdio``; refused under ``http``.
        args: Arguments passed to ``command``.
        env: Environment variables handed to the subprocess, on top of the
            SDK's safe default subset; the worker's own environment is not
            inherited.  Values arrive already resolved by the secrets
            resolver, so they must have the shape of a single token.
    """

    transport: str = "http"
    url: str | None = None
    headers_ref: str | None = None
    auth: dict[str, str] | None = None
    timeout_ms: int = 20000
    command: str | None = None
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = None


class A2AAgentConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """One named remote A2A agent (``ai.a2a_agents.<name>``).

    The credential is declared exactly as an MCP server declares it, and for
    the same reason: the artifact names the agent, the deployment says how to
    authenticate to it.

    Attributes:
        url: ``https://`` agent URL, free of userinfo and query string.
        headers_ref: Reference to headers resolved by the secrets resolver.
            Never a literal secret.  The resolved payload must be a single
            ``Name=value`` header pair, a shape checked at start-up rather than
            here; a bearer token belongs in ``auth: {kind: bearer}``.
            Mutually exclusive with ``auth``.
        auth: Named authentication strategy and its settings, flattened:
            ``kind`` selects an entry point registered in ``loom.ai.remote_auth``
            and every other key is passed to it as a keyword argument.
            Mutually exclusive with ``headers_ref``.
    """

    url: str
    headers_ref: str | None = None
    auth: dict[str, str] | None = None


class AgentEndpointConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Per-agent HTTP exposure opt-in (FR-029a, FR-045a).

    An agent absent from ``ai.endpoints`` is never mounted; presence requires
    naming the authentication explicitly — there is no default.

    Attributes:
        enabled: Whether to mount the agent's HTTP endpoints.
        auth: Named authentication the mount requires.  Mandatory.
        allow_anonymous: Whether an empty-subject identity is accepted for
            this agent (FR-045a).
    """

    enabled: bool
    auth: str
    allow_anonymous: bool = False


class A2AConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """A2A exposure settings (FR-041).

    Attributes:
        base_url: Public URL the agent card advertises.
        expose: Agent names to publish.  Must be non-empty: empty means none,
            never all (FR-041a).

    Raises:
        AgentCompilationError: With :data:`~loom.ai.errors.AgentErrorCode.A2A_EXPOSE_EMPTY`
            when ``expose`` names no agent.
    """

    base_url: str
    expose: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.expose:
            raise AgentCompilationError([a2a_expose_empty()])
        # base_url is published verbatim in the capability card, which is the
        # one unauthenticated surface of the pillar: an http:// address would
        # advertise a plaintext channel to every discovering client, and
        # userinfo would publish a credential outright. Same rule the compiler
        # already applies to the remote URLs it validates, applied where this
        # one actually lives (FR-038).
        fault = _url_fault(self.base_url)
        if fault is None:
            _warn_plaintext_loopback("ai.a2a.base_url", self.base_url)
        if fault is not None:
            raise AgentCompilationError([a2a_base_url_invalid(_redact_url(self.base_url), fault)])


class AiConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Deployment configuration of the AI pillar (``ai:`` section).

    Attributes:
        engine: Entry-point name in group ``loom.ai.engines``.
        specs: Glob patterns of agent artifacts, relative to the app root.
            Mutually exclusive with the manifest ``AGENTS`` attribute: exactly
            one of the two declares the artifacts of an application, and
            declaring both is a compilation error.
        models: Model-role bindings; must contain every role an agent declares.
        prices: Prices keyed by vendor model id (``InferenceTarget.model``).
            A plan bound to a listed model is priced with these rates rather
            than the engine's own, which is what keeps ``policies.max_usd``
            enforceable for a model the engine cannot price.
        skills_root: Filesystem root bare skill library names resolve against.
        mcp_servers: Named remote MCP servers artifacts refer to by name.
        a2a_agents: Named remote A2A agents artifacts refer to by name.
        a2a: A2A exposure; absent means no card and no A2A endpoints (FR-041).
        endpoints: Per-agent HTTP opt-in (FR-029a).
        startup_timeout_ms: Total budget of start-up: opening every live
            client concurrently and validating the declared tool filters
            share one deadline, whatever the number of servers.
        remote_clients: Start-up tolerance for the MCP servers and A2A agents
            the deployment connects to, ``required`` (the default) or
            ``optional``.  Under ``optional`` a client that fails to *connect*
            is logged and dropped instead of aborting start-up, so an
            application can boot with no network.  It tolerates nothing else:
            a missing client factory is a wiring bug and stays fatal, a server
            that did open and whose tool listing times out still fails
            start-up, and no client becomes lazy — one that never opened is
            not reconnected later.
        max_concurrent_runs: Per-worker run limit (FR-033a).
        max_prompt_bytes: Enforced while reading the request body, against
            the ``prompt`` field alone (FR-015).
        max_state_bytes: Enforced while reading the request body, against the
            raw ``state`` bytes alone and before they are decoded — measuring
            the wire size is what keeps the check cheap. Joins
            ``max_prompt_bytes`` in the body's own cap (FR-015); a caller's
            ``state`` over this limit is refused with a ``413`` naming
            ``state``, independently of the prompt's own cap.
        health_cache_ttl_ms: Refresh period of the health probe.
        max_agent_depth: Longest chain of nested agent runs one task may
            open, counting the top-level run itself. A use case reaches a
            named agent through an ``Agent()`` marker parameter, and that
            handle's ``run`` counts as one more entry in the same chain the
            run that reached the use case already opened. Defaults to ``1``,
            under which the top-level run alone already consumes the whole
            budget: an output hook, or any other use case an agent's run
            invokes, that itself declares an ``Agent()`` marker finds no
            depth left, and the whole class of agent-calls-agent cycles is
            unreachable without deliberately raising this value.

    Raises:
        AgentCompilationError: Aggregating one issue per invalid model binding
            (incomplete provider settings, literal secret in
            ``credentials_ref``), per unsafe remote server or agent (bad URL,
            inline credentials, out-of-range timeout), per endpoint without
            a named ``auth``, for an unknown ``remote_clients`` mode, and for
            a ``max_agent_depth`` below 1 — which would refuse every run,
            including the top-level one.
    """

    engine: str
    models: dict[str, InferenceTarget]
    specs: tuple[str, ...] = ()
    prices: dict[str, ModelPrice] = field(default_factory=dict)
    skills_root: str | None = None
    mcp_servers: dict[str, McpServerConfig] = field(default_factory=dict)
    a2a_agents: dict[str, A2AAgentConfig] = field(default_factory=dict)
    a2a: A2AConfig | None = None
    endpoints: dict[str, AgentEndpointConfig] = field(default_factory=dict)
    startup_timeout_ms: int = 10000
    remote_clients: str = "required"
    max_concurrent_runs: int = 8
    max_prompt_bytes: int = 65536
    max_state_bytes: int = 65536
    health_cache_ttl_ms: int = 5000
    max_agent_depth: int = 1

    def __post_init__(self) -> None:
        issues: list[AgentCompilationIssue] = []
        for role, target in self.models.items():
            issues.extend(_validate_model_binding(role, target))
        issues.extend(_validate_mcp_servers(self.mcp_servers))
        issues.extend(_validate_a2a_agents(self.a2a_agents))
        for name, endpoint in self.endpoints.items():
            if not endpoint.auth.strip():
                issues.append(endpoint_auth_missing(name))
        if self.remote_clients not in _REMOTE_CLIENTS_MODES:
            issues.append(remote_clients_unknown(self.remote_clients, _REMOTE_CLIENTS_MODES))
        if self.max_agent_depth < 1:
            issues.append(max_agent_depth_invalid(self.max_agent_depth))
        if issues:
            raise AgentCompilationError(issues)


_UrlIssue = Callable[[str, str, str], AgentCompilationIssue]


def _validate_remote_url(component: str, url: str, build: _UrlIssue) -> list[AgentCompilationIssue]:
    """Collect the fault of one remote URL, redacted so it cannot leak a secret."""
    fault = _url_fault(url)
    if fault is None:
        _warn_plaintext_loopback(component, url)
        return []
    return [build(component, _redact_url(url), fault)]


def _validate_headers_ref(component: str, headers_ref: str | None) -> list[AgentCompilationIssue]:
    if headers_ref is None or _is_credentials_reference(headers_ref):
        return []
    # The rejected value is deliberately absent: the message must not leak the
    # very secret it rejects.
    return [mcp_credentials_inline(component, "headers_ref")]


def _validate_auth(
    component: str, headers_ref: str | None, auth: Mapping[str, str] | None
) -> list[AgentCompilationIssue]:
    """Collect the issues of one endpoint's ``auth`` block.

    Applied to MCP servers and A2A agents alike, since both declare the same
    block and resolve it through the same registry.  Two credentials on one
    connection are ambiguous, so ``headers_ref`` and ``auth`` are refused
    together; every setting in the block is held to the same fail-closed
    reference test as ``headers_ref``, so no literal secret reaches loom by the
    back door; and an unregistered strategy is refused here rather than at the
    first message in production.
    """
    if auth is None:
        return []
    if headers_ref is not None:
        return [mcp_auth_conflict(component)]
    issues = [
        # The rejected value is deliberately absent from the issue, as for
        # ``headers_ref``: the message must not leak what it rejects.
        mcp_credentials_inline(component, f"auth.{key}")
        for key, value in auth.items()
        if key != _AUTH_KIND_KEY and not _is_credentials_reference(value)
    ]
    kind = auth.get(_AUTH_KIND_KEY, "")
    if not is_strategy_registered(kind):
        issues.append(mcp_auth_strategy_unknown(component, kind, registered_strategy_names()))
    return issues


def _validate_mcp_transport(component: str, server: McpServerConfig) -> list[AgentCompilationIssue]:
    """Collect the fields of one server that its ``transport`` does not accept."""
    if server.transport not in _MCP_TRANSPORTS:
        accepted = ", ".join(_MCP_TRANSPORTS)
        return [
            mcp_transport_invalid(
                component, f"transport '{server.transport}' is not supported; accepted: {accepted}"
            )
        ]
    if server.transport == "stdio":
        return _validate_stdio_server(component, server)
    return _validate_http_server(component, server)


def _validate_http_server(component: str, server: McpServerConfig) -> list[AgentCompilationIssue]:
    """Collect the coherence, URL and credential issues of a ``transport: http`` server."""
    issues: list[AgentCompilationIssue] = []
    if server.url is None:
        issues.append(mcp_transport_invalid(component, "transport 'http' requires 'url'"))
    declared = (
        ("command", server.command is not None),
        ("args", bool(server.args)),
        ("env", server.env is not None),
    )
    stdio_only = ", ".join(f"'{name}'" for name, present in declared if present)
    if stdio_only:
        issues.append(
            mcp_transport_invalid(component, f"{stdio_only} do not apply to transport 'http'")
        )
    if server.url is not None:
        issues.extend(_validate_remote_url(component, server.url, mcp_url_invalid))
    issues.extend(_validate_headers_ref(component, server.headers_ref))
    issues.extend(_validate_auth(component, server.headers_ref, server.auth))
    return issues


def _validate_stdio_server(component: str, server: McpServerConfig) -> list[AgentCompilationIssue]:
    """Collect the coherence and environment issues of a ``transport: stdio`` server."""
    issues: list[AgentCompilationIssue] = []
    if not server.command:
        issues.append(
            mcp_transport_invalid(component, "transport 'stdio' requires a non-empty 'command'")
        )
    if server.url is not None:
        issues.append(mcp_transport_invalid(component, "'url' only applies to transport 'http'"))
    if server.headers_ref is not None or server.auth is not None:
        issues.append(mcp_transport_invalid(component, _STDIO_CREDENTIALS_REASON))
    issues.extend(_validate_mcp_env(component, server.env))
    return issues


def _validate_mcp_env(component: str, env: Mapping[str, str] | None) -> list[AgentCompilationIssue]:
    """Collect the issues of a subprocess ``env`` block: valid names, single-token values."""
    if env is None:
        return []
    issues: list[AgentCompilationIssue] = []
    for name, value in env.items():
        if _ENV_NAME_PATTERN.fullmatch(name) is None:
            issues.append(
                mcp_transport_invalid(
                    component,
                    f"'{name}' is not a valid environment variable name",
                    field=f"env.{name}",
                )
            )
        if not _has_reference_shape(value):
            # The rejected value is deliberately absent: a broken interpolation
            # may still contain part of the secret it failed to resolve.
            issues.append(mcp_credentials_inline(component, f"env.{name}"))
    return issues


def _validate_mcp_servers(
    servers: Mapping[str, McpServerConfig],
) -> list[AgentCompilationIssue]:
    """Collect the issues of every ``ai.mcp_servers`` entry."""
    issues: list[AgentCompilationIssue] = []
    for name, server in servers.items():
        component = f"ai.mcp_servers.{name}"
        issues.extend(_validate_mcp_transport(component, server))
        if not _TIMEOUT_MS_MIN <= server.timeout_ms <= _TIMEOUT_MS_MAX:
            issues.append(
                policy_out_of_range(
                    component,
                    "timeout_ms",
                    server.timeout_ms,
                    _TIMEOUT_MS_MIN,
                    _TIMEOUT_MS_MAX,
                )
            )
    return issues


def _validate_a2a_agents(
    agents: Mapping[str, A2AAgentConfig],
) -> list[AgentCompilationIssue]:
    """Collect the issues of every ``ai.a2a_agents`` entry."""
    issues: list[AgentCompilationIssue] = []
    for name, agent in agents.items():
        component = f"ai.a2a_agents.{name}"
        issues.extend(_validate_remote_url(component, agent.url, a2a_url_invalid))
        issues.extend(_validate_headers_ref(component, agent.headers_ref))
        issues.extend(_validate_auth(component, agent.headers_ref, agent.auth))
    return issues
