"""Mounting policy of the A2A surface: what is published, and how safely.

Selection, the start-up refusals and the authentication-exclusion bookkeeping.
None of this is A2A-specific — it is the policy any surface publishing an agent
to the public internet has to satisfy — which is why it is stated once here
rather than inline in the transport.

**Only the card is anonymous.** Binding reads the exclusions mounted on the
application's authentication middleware and refuses to start when any of them
covers a path under the A2A or the agents prefix other than a card path
(FR-041b); it then registers the card paths as the sole exclusion. Publishing
an agent no caller could ever authenticate against is refused too.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final, cast

from fastapi import FastAPI
from starlette.middleware import Middleware

from loom.ai.a2a.card import build_agent_card, card_path
from loom.ai.compiler import AgentPlan
from loom.ai.config import AgentEndpointConfig, AiConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.fastapi.response import ENCODER
from loom.ai.runtime import AgentRuntime
from loom.core.config.errors import ConfigError
from loom.rest.auth.abc import Authenticator
from loom.rest.auth.middleware import AuthenticationMiddleware

_logger = logging.getLogger(__name__)

# Default prefix :func:`loom.ai.fastapi.endpoints.bind_agent_endpoints` mounts
# under. Guarded here because an exclusion written against it would open the
# HTTP invocation surface just as widely as one written against the A2A prefix.
# A deployment mounting the agents elsewhere passes its own ``agents_prefix``.
DEFAULT_AGENTS_PREFIX: Final[str] = "/agents"


@dataclass(frozen=True, slots=True)
class PublishedAgent:
    """One agent published over A2A, with everything the routes need precomputed."""

    name: str
    card: bytes
    max_steps: int
    endpoint: AgentEndpointConfig | None
    conversational: bool


def published_agents(
    *,
    config: AiConfig,
    plans: Mapping[str, AgentPlan],
    runtime: AgentRuntime,
    mechanism: str | None,
    prefix: str,
) -> tuple[PublishedAgent, ...]:
    """Select the exposed agents that really exist, warning about the ones that do not.

    ``expose`` is authoritative and never empty (``A2A_EXPOSE_EMPTY``): an empty
    list means no agent, never all of them (FR-041a).

    A plan declaring ``deps_type`` or ``deps_schema`` (``plan.state is not
    None``) is refused rather than published: the A2A handler runs an agent
    with no state (``a2a/_handlers.py:235``), and the message shape it reads
    carries a prompt and a ``contextId``, with no field for one
    (``a2a/_handlers.py:175-186``). Rendering a declared state that never
    arrives degrades silently rather than failing — measured,
    ``render({})`` returns a sentence with its placeholders empty. Choosing a
    place in the A2A envelope to carry state would publish a protocol
    convention, which is a decision for whoever owns loom's A2A surface, not
    one this pass makes on its behalf.

    Raises:
        AgentCompilationError: With ``STATE_SURFACE_UNSUPPORTED``, naming the
            first stateful agent found in ``expose`` and the ``a2a`` surface
            it conflicts with.
    """
    a2a = config.a2a
    assert a2a is not None  # noqa: S101 - guarded by the caller's early return
    published: list[PublishedAgent] = []
    for name in a2a.expose:
        plan = plans.get(name)
        if plan is None or not runtime.has_agent(name):
            _logger.warning(
                "Agent %s is listed in 'ai.a2a.expose' but no compiled agent has that name: "
                "it is not published over A2A",
                name,
            )
            continue
        if plan.state is not None:
            raise AgentCompilationError(
                [AgentErrorCode.STATE_SURFACE_UNSUPPORTED.issue(component=name, surface="a2a")]
            )
        card = ENCODER.encode(build_agent_card(plan, a2a, mechanism=mechanism, prefix=prefix))
        published.append(
            PublishedAgent(
                name=name,
                card=card,
                max_steps=plan.policies.max_iterations,
                endpoint=_active_endpoint(config.endpoints.get(name)),
                conversational=plan.conversation is not None,
            )
        )
    return tuple(published)


def _active_endpoint(endpoint: AgentEndpointConfig | None) -> AgentEndpointConfig | None:
    """Return the HTTP opt-in only when it is actually in force.

    ``allow_anonymous`` lives in the HTTP stanza, whose mount is a double
    opt-in — ``enabled`` **and** a named ``auth`` (FR-029a). A stanza failing
    either grants nothing, so A2A must not inherit its anonymity opt-out:
    otherwise switching the HTTP surface off while leaving
    ``allow_anonymous: true`` behind would keep anonymous invocation alive on
    the one surface published to the internet.
    """
    if endpoint is None or not endpoint.enabled or not endpoint.auth.strip():
        return None
    return endpoint


def _allows_anonymous(agent: PublishedAgent) -> bool:
    """Report whether this agent accepts an unverified external caller."""
    return agent.endpoint is not None and agent.endpoint.allow_anonymous


def require_usable_authenticator(
    agent: PublishedAgent, authenticator: Authenticator | None
) -> None:
    """Refuse to publish an agent whose invocation route could never be used.

    Mirrors :func:`loom.ai.fastapi.endpoints._require_usable_authenticator`:
    without an authenticator every invocation answers 401 forever, which is a
    dead route advertised by a live card, not a security posture.

    Raises:
        ConfigError: When no authenticator can verify callers and the agent
            declares no explicit ``allow_anonymous`` opt-out.
    """
    if authenticator is not None or _allows_anonymous(agent):
        return
    raise ConfigError(
        f"Agent {agent.name!r} is published in 'ai.a2a.expose' but the application configures "
        "no authentication, so every A2A invocation would be refused. Add the "
        "'app.rest.auth.jwt' section, pass create_app(authenticator=...), or opt into "
        f"anonymous callers with 'ai.endpoints.{agent.name}' enabled, its 'auth' named and "
        "'allow_anonymous: true'."
    )


def verify_exclusions(
    exclude_paths: Sequence[str], card_paths: frozenset[str], *, prefix: str, agents_prefix: str
) -> None:
    """Refuse to start when an exclusion opens an invocation path (FR-041b).

    Exclusions are matched as exact strings by
    :class:`~loom.rest.auth.middleware.AuthenticationMiddleware`, so a
    hand-written ``/a2a`` would not merely be useless — combined with a router
    that serves ``/a2a`` itself it opens the whole invocation surface.

    Raises:
        AgentCompilationError: With ``AUTH_EXCLUSION_OVERLAPS_AGENTS``, naming
            every offending exclusion at once.
    """
    guarded = (prefix, agents_prefix)
    offending = [
        path
        # Deduplicated in order: the mounted list and the argument usually
        # repeat each other, and the failure must name each path once.
        for path in dict.fromkeys(exclude_paths)
        if path not in card_paths
        and any(path == root or path.startswith(f"{root}/") for root in guarded)
    ]
    if offending:
        raise AgentCompilationError(
            [AgentErrorCode.AUTH_EXCLUSION_OVERLAPS_AGENTS.issue(paths=offending)]
        )


def _is_auth_middleware(entry: Middleware) -> bool:
    """Report whether one recorded middleware entry is the authentication one.

    ``Middleware.cls`` is typed as a generic ASGI factory protocol, so the
    identity test needs the cast to be expressible at all.
    """
    return cast(object, entry.cls) is AuthenticationMiddleware


def _auth_middleware_entries(app: FastAPI) -> list[Middleware]:
    return [entry for entry in app.user_middleware if _is_auth_middleware(entry)]


def _declared_exclusions(entry: Middleware) -> tuple[str, ...]:
    """Read one middleware entry's exclusion list.

    Raises:
        ConfigError: When the recorded ``exclude_paths`` argument is not a list
            of paths, since neither the guard nor the card registration could
            then be trusted.
    """
    declared = entry.kwargs.get("exclude_paths", ())
    if not isinstance(declared, Sequence) or isinstance(declared, str):
        raise ConfigError(
            "The mounted 'AuthenticationMiddleware' declares an unrecognisable "
            "'exclude_paths' argument, so the A2A agent card paths cannot be excluded "
            "from authentication."
        )
    return tuple(str(path) for path in declared)


def mounted_exclusions(app: FastAPI) -> tuple[str, ...]:
    """Return the authentication exclusions the deployment actually runs with.

    The authoritative list is the one mounted on the middleware, not the one
    the caller of :func:`bind_a2a_endpoints` chose to repeat in its argument —
    which defaults to empty. Reading it here is what makes the FR-041b guard
    fail closed.
    """
    return tuple(
        path for entry in _auth_middleware_entries(app) for path in _declared_exclusions(entry)
    )


def register_card_exclusions(app: FastAPI, card_paths: Sequence[str]) -> None:
    """Add the card paths to the authentication middleware's exclusion list.

    The middleware is mounted by ``create_app`` *before* the routes exist, so
    the exclusion cannot be passed at construction time; the ASGI stack is not
    built yet, which is what makes extending the recorded keyword argument
    sound. Doing this here — rather than in ``create_app`` — keeps the card the
    *only* path this feature can ever open.

    Args:
        app: Application whose authentication middleware is amended.
        card_paths: Paths to serve without credentials.

    Raises:
        ConfigError: When the ASGI stack is already built, or when no
            recognisable :class:`AuthenticationMiddleware` entry is mounted
            even though the deployment configured an authenticator.
    """
    if app.middleware_stack is not None:
        raise ConfigError(
            "The A2A surface must be bound before the ASGI middleware stack is built: "
            "the agent card paths can no longer be excluded from authentication."
        )
    entries = _auth_middleware_entries(app)
    if not entries:
        raise ConfigError(
            "The application configures an authenticator but no 'AuthenticationMiddleware' is "
            "mounted, so the A2A agent card paths cannot be excluded from authentication. "
            "Mount the middleware through create_app() before binding the A2A endpoints."
        )
    for entry in entries:
        entry.kwargs["exclude_paths"] = (*_declared_exclusions(entry), *card_paths)


def announce_publication(
    agent: PublishedAgent, *, base_url: str, mechanism: str | None, prefix: str
) -> None:
    """Emit the startup WARNING naming exactly what is now reachable from outside."""
    allow_anonymous = _allows_anonymous(agent)
    _logger.warning(
        "A2A agent published to the public internet: url=%s%s/%s card=%s agent=%s auth=%s "
        "allow_anonymous=%s. %s",
        base_url.rstrip("/"),
        prefix,
        agent.name,
        card_path(agent.name, prefix=prefix),
        agent.name,
        mechanism or "none",
        allow_anonymous,
        _identity_notice(allow_anonymous, conversational=agent.conversational),
    )


def _identity_notice(allow_anonymous: bool, *, conversational: bool = False) -> str:
    """State plainly which identity an external A2A caller's run executes as.

    Anonymous callers share one subject, so when the agent declares a
    ``conversation`` loader nothing but the A2A ``contextId`` separates their
    threads: whoever presents an id reads that thread.
    """
    if allow_anonymous:
        notice = (
            "allow_anonymous is set, so external callers are NOT authenticated: every run "
            "spends model tokens on behalf of an unidentified stranger, and only "
            "'max_concurrent_runs' and 'run_timeout_ms' bound that cost"
        )
        if conversational:
            notice += (
                "; 'conversation' is declared, and every anonymous caller shares one "
                "subject, so threads are separated by the A2A 'contextId' alone: the id is "
                "the credential"
            )
        return notice
    return (
        "the card is served anonymously; every invocation requires a verified caller, and "
        "each capability call then runs as that identity"
    )
