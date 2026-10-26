"""Pluggable authentication for the remote endpoints an agent calls out to.

One registry serves both outbound transports — the MCP servers of
``ai.mcp_servers`` and the remote agents of ``ai.a2a_agents``.  Loom ships
**no login flow of its own**.  A deployment names a strategy in that endpoint's
``auth.kind``; loom resolves the name in the entry-point group
:data:`REMOTE_AUTH_ENTRY_POINT_GROUP`, calls the registered object with the rest
of the block as keyword arguments, and hands the result to the client.  The
artifact keeps saying only ``server: <name>`` or ``agent: <name>``, so it moves
between environments unchanged whether that endpoint needs no credential, a
fixed header, or a token exchange.

**One group, named for what it authenticates, not for who called first.**  The
contract for a strategy is whatever the HTTP client accepts — not an
abstraction of ours — and nothing about ``Authorization: Bearer <token>`` is
MCP-specific, so a deployment that authenticates to one internal service
registers its strategy once and grants it to either transport.  The group was introduced in 1.7.0 as
``loom.ai.mcp_auth``, when MCP was its only consumer; keeping that name for
A2A strategies too would make it a lie for half its contents and would leave a
third party unable to tell which transports its registration is offered to, so
the group was renamed the release after it appeared, with no deprecation cycle
because it had no consumers yet.

Any existing ``httpx.Auth`` class in the ecosystem is registered as it stands,
with no adapter::

    [project.entry-points."loom.ai.remote_auth"]
    agent-session = "my_package.auth:AgentSessionAuth"

Loom registers three strategies, all thin delegations to what the libraries
already provide:

``oauth``
    Returns the sentinel the MCP client understands, so the client's own
    standard OAuth flow runs.  Loom implements none of it.  It is the one
    strategy that returns no auth object at all, and therefore the one an A2A
    agent cannot use: :func:`shared_a2a_auth` refuses it by name rather than
    connecting without it.
``bearer``
    ``Authorization: Bearer <token>`` from a token the deployment resolves.
``static``
    Fixed headers, resolved by the deployment's secret resolver.

``headers_ref`` and ``auth`` are mutually exclusive on one endpoint, and
``auth: {kind: static, ...}`` is the long spelling of the shorthand: both read
the same ``Name=value`` payload through :func:`headers_from_ref`.  Reach for
``bearer`` whenever the credential is a token, since the composed header cannot
be written in configuration at all.

**Two HTTP libraries, one callable.**  The MCP toolset comes from
``pydantic-ai``, whose HTTP client is ``httpx2``, and it reaches that client
through the MCP transport, which passes any auth object it does not recognise
straight through; loom builds the A2A client itself, with ``httpx``.  Each of
the two libraries accepts an auth object only when it is an instance of *its
own* ``Auth`` class, a two-tuple, or a **callable**, so a class written against
one flavour is refused by the other and loom adapts neither.  The two
strategies loom ships therefore return a plain callable — the one shape both
clients accept, and the supported answer for a deployment's own fixed-header
strategy.  A strategy that needs a real flow registers a class, written against
the flavour of the transport that will use it.

A callable is a single-shot flow: it sets headers on the request it is handed,
returns it, and never sees the response.  A class that does need the response
body must know that ``requires_response_body`` is honoured by ``Auth``'s own
base flow, not by the client, so a strategy that overrides ``async_auth_flow``
replaces the very code that reads the flag and has to ``await response.aread()``
itself.

Nothing here imports an HTTP library at all, at module load or later: ``httpx``
reaches a deployment with the A2A client and ``httpx2`` with the MCP one, both
optional, while the strategy-name check runs during configuration decode in
every deployment.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from threading import Lock
from typing import TYPE_CHECKING, Any, Final, cast, get_type_hints

import msgspec

from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.model.redaction import Secret
from loom.core.plugins.entrypoints import (
    DuplicateEntryPointError,
    EntryPointNotFoundError,
    list_entry_points,
    load_entry_point,
    select_entry_point,
)

if TYPE_CHECKING:  # annotations only: importing the plan at runtime would
    # close a cycle, since the configuration this module validates is what the
    # compiler reads to build that plan.
    from typing import TypeAlias

    import httpx

    from loom.ai.compiler import CompiledRemoteAuth

    AuthObject: TypeAlias = "httpx.Auth | Callable[[Any], Any]"
    """What an HTTP client presents on a request.

    The two shapes of ``httpx._client._build_auth`` and its ``httpx2`` twin
    that a strategy has reason to return — an ``Auth`` instance of that
    client's own flavour, or a callable the client wraps in its own
    ``FunctionAuth`` — leaving out the two-tuple, which no strategy returns.
    """

    ClientAuth: TypeAlias = "AuthObject | str"
    """What a strategy may resolve to: an auth object, or the MCP sentinel.

    The sentinel never reaches a client's ``auth=``; the MCP client reads it and
    runs its own OAuth flow, so :func:`shared_a2a_auth` refuses it.  Both
    aliases are named only while type-checking: this module imports no HTTP
    library at runtime.
    """

REMOTE_AUTH_ENTRY_POINT_GROUP = "loom.ai.remote_auth"
"""Entry-point group every outbound authentication strategy registers under."""

_HEADER_SEPARATOR = "="

_MCP_SCOPE = "mcp"
_A2A_SCOPE = "a2a"


def registered_strategy_names() -> list[str]:
    """List the strategy names installed distributions register.

    Returns:
        Every name registered in :data:`REMOTE_AUTH_ENTRY_POINT_GROUP`, sorted,
        as the "not registered, available: ..." message reports them.
    """
    return sorted({ep.name for ep in list_entry_points(REMOTE_AUTH_ENTRY_POINT_GROUP)})


def is_strategy_registered(kind: str) -> bool:
    """Report whether ``kind`` resolves to a registered strategy.

    Called during configuration decode so an unknown strategy fails at compile
    time, naming what is installed, rather than at the first message in
    production.

    Args:
        kind: Strategy name from an endpoint's ``auth.kind``.

    Returns:
        ``True`` when a distribution registers ``kind``.
    """
    if not kind:
        return False
    return select_entry_point(REMOTE_AUTH_ENTRY_POINT_GROUP, kind, on_duplicate="error") is not None


def headers_from_ref(component: str, headers_ref: Secret[str] | str | None) -> dict[str, str]:
    """Read the header a resolved ``headers_ref`` carries.

    The deployment's secret resolver has already run — ``${secrets:/path}`` is
    an OmegaConf resolver, so the value reaching loom is the payload, not the
    path.  That payload must be **one ``Name=value`` header pair**, and the
    shape is checked here, at start-up, rather than while the artifact compiles:
    configuration accepts any reference-shaped string, so a malformed payload is
    a start-up failure by name, not a compile-time one.  Several headers, a value
    carrying spaces, or a bearer token belong in a strategy — see
    :func:`bearer_token` — rather than in this shorthand.

    Args:
        component: Configuration path the failure is attributed to.
        headers_ref: Resolved payload, or ``None`` when the server declares none.

    Returns:
        The headers to send, empty when ``headers_ref`` is ``None``.

    Raises:
        AgentCompilationError: With ``MCP_HEADERS_REF_INVALID`` when the payload
            is not one ``Name=value`` pair.  The rejected value never appears in
            the message.

    Example::

        headers_from_ref("mcp server 'kb'", "X-API-Key=abc123")
    """
    if headers_ref is None:
        return {}
    payload = headers_ref.reveal() if isinstance(headers_ref, Secret) else headers_ref
    name, separator, value = payload.partition(_HEADER_SEPARATOR)
    if not separator or not name or not value:
        raise AgentCompilationError(
            [AgentErrorCode.MCP_HEADERS_REF_INVALID.issue(component=component)]
        )
    return {name: value}


def shared_mcp_auth(server: str, auth: CompiledRemoteAuth | None) -> ClientAuth | None:
    """Return the authentication object of an MCP server, built once per server.

    **One instance per server, shared by every agent granted it.**  The
    credential belongs to the deployment, not to the agent: a renewing strategy
    holds a token, and one instance means one renewal rather than one per agent,
    and no burst of simultaneous logins when several agents start together.

    Args:
        server: Configured server name, the sharing key.
        auth: Compiled strategy name and settings, or ``None`` when the server
            declares none.

    Returns:
        What the MCP client's ``auth`` parameter accepts — a callable, an
        ``Auth`` of the client's own flavour, the sentinel the client's own
        OAuth flow answers to, or ``None`` when the server declares no
        strategy.

    Raises:
        AgentCompilationError: With ``MCP_AUTH_STRATEGY_INVALID`` when the
            strategy cannot be loaded, rejects its settings, or returns
            something the MCP client cannot use.

    Example::

        auth = shared_mcp_auth("orders", capability.auth)
    """
    if auth is None:
        return None
    return _STRATEGIES.get(f"{_MCP_SCOPE}:{server}", auth)


def shared_a2a_auth(agent: str, auth: CompiledRemoteAuth | None) -> AuthObject | None:
    """Return the authentication object of a remote agent, built once per agent.

    Shares per configured agent exactly as :func:`shared_mcp_auth` shares per
    server, and from the same registry: the sharing keys are scoped by transport
    so that an MCP server and an A2A agent registered under the same name remain
    two credentials.

    Args:
        agent: Configured agent name, the sharing key.
        auth: Compiled strategy name and settings, or ``None`` when the agent
            declares none.

    Returns:
        What the A2A HTTP client's ``auth`` parameter accepts — a callable or
        an :class:`httpx.Auth` — or ``None`` when the agent declares no
        strategy.

    Raises:
        AgentCompilationError: With ``MCP_AUTH_STRATEGY_INVALID`` when the
            strategy cannot be loaded, rejects its settings, or resolves to the
            MCP client's OAuth sentinel, which no HTTP client can present.

    Example::

        auth = shared_a2a_auth("market", capability.auth)
    """
    if auth is None:
        return None
    built = _STRATEGIES.get(f"{_A2A_SCOPE}:{agent}", auth)
    if isinstance(built, str):
        # ``oauth`` delegates to the MCP client's own flow, which the A2A
        # transport does not have. Refusing by name is the point: connecting
        # without the credential the deployment declared is the failure this
        # module exists to prevent.
        raise AgentCompilationError(
            [
                AgentErrorCode.MCP_AUTH_STRATEGY_INVALID.issue(
                    kind=auth.kind,
                    reason="it delegates to the MCP client's own flow, which the A2A transport "
                    "cannot run; an A2A agent needs an httpx.Auth or a callable",
                )
            ]
        )
    return built


def standard_oauth() -> str:
    """Delegate to the MCP client's own standard OAuth flow.

    Registered as the ``oauth`` strategy.  Loom implements no part of the flow:
    it returns the sentinel the client library answers to, so the OAuth support
    the MCP specification standardises is the client's, unchanged.

    Returns:
        The sentinel the MCP client recognises as "run your OAuth flow".
    """
    return "oauth"


def bearer_token(*, token_ref: str) -> Callable[[Any], Any]:
    """Send ``Authorization: Bearer <token>``, the most common MCP credential.

    Registered as the ``bearer`` strategy.  The strategy composes the header
    itself, which is the whole point of it: the composed value carries a space,
    and configuration refuses a space so that no literal credential can hide in
    it.  A token on its own — a JWT is base64url with dots — passes that test,
    so the deployment stores the token and loom writes the header.

    Args:
        token_ref: Resolved bearer token, never the composed header.

    Returns:
        A callable that presents the token on every request, which is the one
        shape both HTTP flavours accept — see the module docstring.
    """
    return _header_auth({"Authorization": f"Bearer {token_ref}"})


def static_headers(*, headers_ref: str) -> Callable[[Any], Any]:
    """Attach fixed headers, resolved by the deployment's secret resolver.

    Registered as the ``static`` strategy.  It is the ``auth`` block's spelling
    of the ``headers_ref`` shorthand, for deployments that prefer every server
    to name a strategy.

    Args:
        headers_ref: Resolved ``Name=value`` payload, as ``headers_ref`` carries.

    Returns:
        A callable that adds those headers to every request, which is the one
        shape both HTTP flavours accept — see the module docstring.

    Raises:
        AgentCompilationError: With ``MCP_HEADERS_REF_INVALID`` when the payload
            is not one ``Name=value`` pair.
    """
    headers = headers_from_ref("auth strategy 'static'", headers_ref)
    return _header_auth(headers)


class _SharedStrategies:
    """The one authentication instance of each remote endpoint, built on demand.

    Construction happens while the deployment starts up, from however many
    agents were granted the same endpoint; the lock makes that first build
    happen once, and every later caller receives the same object.  Keys are
    transport-scoped, so an MCP server and an A2A agent sharing a configured
    name do not share a credential.
    """

    def __init__(self) -> None:
        self._by_endpoint: dict[str, ClientAuth] = {}
        self._lock = Lock()

    def get(self, endpoint: str, auth: CompiledRemoteAuth) -> ClientAuth:
        with self._lock:
            existing = self._by_endpoint.get(endpoint)
            if existing is not None:
                return existing
            built = _build(auth)
            self._by_endpoint[endpoint] = built
            return built


_COERCIBLE: Final = (str, int, float, bool)
"""Annotations a setting is converted to.

Deliberately only these.  A strategy declaring ``cfg: MyObject`` receives the
string it receives today and is unaffected, so widening the rule later stays
additive while narrowing it never has to happen.
"""


def _coerce_settings(strategy: Any, settings: dict[str, str]) -> dict[str, object]:
    """Convert each setting whose parameter declares a primitive type.

    Configuration carries every setting as a string, because each one passes the
    inline-credential refusal, which admits no spaces.  That is loom's own
    constraint, so loom converts back rather than handing a strategy a string
    where its signature asked for an ``int`` -- or, worse, a ``bool``, since the
    string ``"false"`` is truthy and would silently invert what a deployment
    asked for.

    ``inspect.signature(strategy)`` reads the callable itself, not
    ``__init__``: two of the three strategies loom registers are plain
    functions, and inspecting a function's ``__init__`` yields ``object``'s
    ``(*args, **kwargs)`` -- no parameters, and therefore no conversion, in
    silence.

    Args:
        strategy: The loaded strategy, a class or a function.
        settings: The endpoint's settings, every value a string.

    Returns:
        The settings, with primitive-annotated values converted and everything
        else -- custom types, unannotated parameters, names the strategy does
        not declare -- passed through unchanged.

    Raises:
        ValidationError: When a value cannot become the primitive its parameter
            declares.  The caller names the setting.
    """
    try:
        params = inspect.signature(strategy).parameters
        # signature() reads a class through its __init__, but get_type_hints()
        # on a class reads its *attribute* annotations, so the two must be
        # pointed at different objects to describe the same parameters.
        annotated: Any = getattr(strategy, "__init__") if isinstance(strategy, type) else strategy  # noqa: B009
        hints = get_type_hints(annotated)
    except Exception:  # noqa: BLE001
        # A callable whose signature or annotations cannot be resolved -- a
        # C builtin, a partial, an annotation naming a TYPE_CHECKING-only
        # import -- behaves exactly as it did before this function existed.
        # Introspection must never be the thing that breaks a working strategy.
        return dict(settings)
    coerced: dict[str, object] = {}
    for name, raw in settings.items():
        annotation = hints.get(name)
        if name not in params or annotation not in _COERCIBLE:
            coerced[name] = raw
        else:
            coerced[name] = msgspec.convert(raw, type=annotation, strict=False)
    return coerced


def _build(auth: CompiledRemoteAuth) -> ClientAuth:
    """Load the named strategy and construct it from its settings."""
    strategy = _load_strategy(auth.kind)
    try:
        plain = {
            key: value.reveal() if isinstance(value, Secret) else value
            for key, value in auth.settings
        }
        settings = _coerce_settings(strategy, plain)
    except msgspec.ValidationError as exc:
        raise AgentCompilationError(
            [
                AgentErrorCode.MCP_AUTH_STRATEGY_INVALID.issue(
                    kind=auth.kind, reason=f"a setting has the wrong type: {exc}"
                )
            ]
        ) from exc
    try:
        built = strategy(**settings)
    except TypeError as exc:
        raise AgentCompilationError(
            [
                AgentErrorCode.MCP_AUTH_STRATEGY_INVALID.issue(
                    kind=auth.kind, reason=f"it rejected its settings: {exc}"
                )
            ]
        ) from exc
    return _checked(auth.kind, built)


def _load_strategy(kind: str) -> Any:
    """Load the registered strategy, reporting either failure as a coded issue."""
    try:
        return load_entry_point(REMOTE_AUTH_ENTRY_POINT_GROUP, kind, on_duplicate="error")
    except EntryPointNotFoundError as exc:
        # Unreachable through configuration, which refuses an unregistered
        # strategy at decode; reached only if a distribution is uninstalled
        # between decode and start-up.
        available = ", ".join(exc.available) if exc.available else "none"
        raise AgentCompilationError(
            [
                AgentErrorCode.MCP_AUTH_STRATEGY_INVALID.issue(
                    kind=kind, reason=f"it is not registered; registered: {available}"
                )
            ]
        ) from exc
    except DuplicateEntryPointError as exc:
        raise AgentCompilationError(
            [
                AgentErrorCode.MCP_AUTH_STRATEGY_INVALID.issue(
                    kind=kind,
                    reason="it is provided by more than one distribution: "
                    f"{', '.join(exc.distributions)}",
                )
            ]
        ) from exc


def _checked(kind: str, built: object) -> ClientAuth:
    """Accept what both clients accept, structurally, never by class.

    ``_build_auth`` — the same function in ``httpx`` and in ``httpx2`` — takes
    three shapes: an ``Auth``, a two-tuple, or a callable it wraps in its own
    ``FunctionAuth``.  Loom accepts the first structurally, by the ``auth_flow``
    attribute the client calls, so a strategy satisfying that contract is
    accepted whichever flavour it was written against; the third as it stands;
    and the MCP OAuth sentinel, which never reaches a client's ``auth=`` at all.
    The two-tuple is left out: no strategy has reason to return one, and
    accepting it would make every mistaken two-element result look valid.

    The ``auth_flow`` probe stays first, ahead of the callable one, so that a
    real auth object is recognised as the auth object it is: an ``Auth``
    instance is not callable, while a class object is, and probing the other way
    round would report a class as a callable.
    """
    if isinstance(built, str) or callable(getattr(built, "auth_flow", None)) or callable(built):
        return cast("ClientAuth", built)
    raise AgentCompilationError(
        [
            AgentErrorCode.MCP_AUTH_STRATEGY_INVALID.issue(
                kind=kind, reason="it returned no httpx.Auth, no callable and no client sentinel"
            )
        ]
    )


def _header_auth(headers: Mapping[str, str]) -> Callable[[Any], Any]:
    """Build the fixed-header callable both HTTP flavours wrap as their own auth.

    The callable must **return** the request: each client wraps it in its own
    ``FunctionAuth``, whose flow is ``yield self._func(request)``, so a callable
    returning ``None`` would send that instead of the request.  It receives a
    request of whichever flavour drove it, which is the whole point and the
    reason nothing here is typed against one library.

    Args:
        headers: Headers to add to every request, copied so that a later change
            to the caller's mapping cannot alter a live credential.

    Returns:
        The callable a strategy returns.
    """
    frozen = dict(headers)

    def add_fixed_headers(request: Any) -> Any:
        """Add this strategy's fixed headers to one outgoing request."""
        request.headers.update(frozen)
        return request

    return add_fixed_headers


_STRATEGIES = _SharedStrategies()
"""Process-wide sharing of one authentication instance per remote endpoint."""


__all__: Sequence[str] = [
    "REMOTE_AUTH_ENTRY_POINT_GROUP",
    "bearer_token",
    "headers_from_ref",
    "is_strategy_registered",
    "registered_strategy_names",
    "shared_a2a_auth",
    "shared_mcp_auth",
    "standard_oauth",
    "static_headers",
]
