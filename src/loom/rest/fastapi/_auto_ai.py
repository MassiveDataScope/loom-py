"""Optional AI wiring for the FastAPI composition root."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI

from loom.composition._ai import AiWiring
from loom.core.observability.runtime import ObservabilityRuntime
from loom.rest.fastapi._auto_auth import _AuthWiring, _require_jwt_audience

if TYPE_CHECKING:
    from loom.ai.config import AiConfig


_AGENTS_PREFIX = "/agents"


def _bind_agent_surface(
    app: FastAPI,
    ai: AiWiring,
    auth: _AuthWiring,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Mount the agent surfaces: HTTP for every opt-in, A2A for every exposure.

    Both mounts read the same ``_AGENTS_PREFIX``. That single source is what
    makes the FR-041b exclusion guard meaningful: it measures the agents prefix
    an exclusion could open, so a prefix nobody mounts would leave the real one
    unguarded.

    Raises:
        ConfigError: When an agent opts in without a usable authenticator, or
            when its callers are verified by a JWT without a validated ``aud``.
    """
    if ai.config is None or ai.runtime is None:
        return
    _validate_agent_endpoint_auth(ai.config, auth)
    # Local import: same containment rule as 'resolve_ai'.
    from loom.ai.fastapi.endpoints import bind_agent_endpoints

    bind_agent_endpoints(
        app,
        runtime=ai.runtime,
        config=ai.config,
        authenticator=auth.authenticator,
        observability_runtime=observability_runtime,
        prefix=_AGENTS_PREFIX,
    )
    _bind_a2a_surface(app, ai, auth, observability_runtime)


def _bind_a2a_surface(
    app: FastAPI,
    ai: AiWiring,
    auth: _AuthWiring,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Publish the agents named in ``ai.a2a.expose`` over inbound A2A (FR-041).

    Guarded on the section rather than delegated to ``bind_a2a_endpoints``'s own
    early return: the import below pulls the ``ai-a2a`` extra, which a
    deployment without an ``ai.a2a`` section is not required to have installed.
    """
    if ai.config is None or ai.runtime is None or ai.config.a2a is None:
        return
    # Local import: the A2A transport lives behind the 'ai-a2a' extra.
    from loom.ai.a2a.server import bind_a2a_endpoints

    bind_a2a_endpoints(
        app,
        runtime=ai.runtime,
        config=ai.config,
        plans=ai.plans,
        authenticator=auth.authenticator,
        exclude_paths=auth.exclude_paths,
        observability_runtime=observability_runtime,
        agents_prefix=_AGENTS_PREFIX,
    )


def _validate_agent_endpoint_auth(ai_cfg: AiConfig, auth: _AuthWiring) -> None:
    """Apply the JWT start-up gates to every agent bound to a verified caller (§4).

    An agent surface is at least as privileged as a SQL one: the verified
    caller drives every capability the agent holds, as that caller. So it must
    not boot in a configuration the SQL surface already refuses.
    """
    if auth.jwt_config is None:
        return
    for name in _agents_bound_to_a_verified_caller(ai_cfg):
        _require_jwt_audience(f"Agent {name!r}", auth.jwt_config)


def _agents_bound_to_a_verified_caller(ai_cfg: AiConfig) -> tuple[str, ...]:
    """Name every agent whose callers are authenticated, over either surface.

    An A2A-published agent counts too, and is the stricter case of the two: the
    HTTP mount is opt-in per agent, while ``ai.a2a.expose`` puts the agent on
    the public internet, where an unvalidated ``aud`` accepts a token minted
    for another service as the caller driving every capability the agent holds.
    """
    http = {
        # The double opt-in 'bind_agent_endpoints' mounts on.
        name
        for name, endpoint in ai_cfg.endpoints.items()
        if endpoint.enabled and endpoint.auth.strip() and not endpoint.allow_anonymous
    }
    a2a = {name for name in _a2a_exposed(ai_cfg) if not _a2a_allows_anonymous(ai_cfg, name)}
    return tuple(sorted(http | a2a))


def _a2a_exposed(ai_cfg: AiConfig) -> tuple[str, ...]:
    return ai_cfg.a2a.expose if ai_cfg.a2a is not None else ()


def _a2a_allows_anonymous(ai_cfg: AiConfig, name: str) -> bool:
    """Mirror ``loom.ai.a2a._binding``'s rule for an unverified A2A caller.

    Anonymity is only in force when the HTTP stanza's own double opt-in holds:
    a disabled or unnamed-``auth`` stanza grants nothing, so its
    ``allow_anonymous`` does not travel to the A2A surface either.
    """
    endpoint = ai_cfg.endpoints.get(name)
    if endpoint is None or not endpoint.enabled or not endpoint.auth.strip():
        return False
    return endpoint.allow_anonymous
