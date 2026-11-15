"""Optional AI wiring for the FastAPI composition root."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI

from loom.composition import AiWiring
from loom.core.observability.runtime import ObservabilityRuntime
from loom.rest.fastapi._auto_auth import _AuthWiring

if TYPE_CHECKING:
    from loom.ai.config import AiConfig


_AGENTS_PREFIX = "/agents"


def _bind_agent_surfaces(
    app: FastAPI,
    ai: AiWiring,
    auth: _AuthWiring,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Mount the HTTP surface of every opted-in agent and the A2A surface of every exposed one.

    Both surfaces are mounted under ``_AGENTS_PREFIX``. The A2A server is
    imported only when the ``ai.a2a`` section is present.

    Raises:
        ConfigError: When an agent opts in without a usable authenticator, or
            when its callers are verified by a JWT without a validated ``aud``.
    """
    config, runtime = ai.config, ai.runtime
    if config is None or runtime is None:
        return
    _validate_agent_endpoint_auth(config, auth)
    from loom.ai.fastapi.endpoints import bind_agent_endpoints

    bind_agent_endpoints(
        app,
        runtime=runtime,
        config=config,
        authenticator=auth.authenticator,
        observability_runtime=observability_runtime,
        prefix=_AGENTS_PREFIX,
    )
    if config.a2a is None:
        return
    from loom.ai.a2a.server import bind_a2a_endpoints

    bind_a2a_endpoints(
        app,
        runtime=runtime,
        config=config,
        plans=ai.plans,
        authenticator=auth.authenticator,
        exclude_paths=auth.exclude_paths,
        observability_runtime=observability_runtime,
        agents_prefix=_AGENTS_PREFIX,
    )


def _validate_agent_endpoint_auth(ai_cfg: AiConfig, auth: _AuthWiring) -> None:
    """Apply the JWT start-up gates to every agent bound to a verified caller.

    Raises:
        ConfigError: When the JWT mechanism sets no audience.
    """
    for name in _agents_bound_to_a_verified_caller(ai_cfg):
        auth.require_jwt_audience(f"Agent {name!r}")


def _agents_bound_to_a_verified_caller(ai_cfg: AiConfig) -> tuple[str, ...]:
    """Name every agent whose callers are authenticated, over HTTP or A2A."""
    http = {
        name
        for name, endpoint in ai_cfg.endpoints.items()
        if endpoint.active and not endpoint.allow_anonymous
    }
    a2a = {name for name in _a2a_exposed(ai_cfg) if not _a2a_allows_anonymous(ai_cfg, name)}
    return tuple(sorted(http | a2a))


def _a2a_exposed(ai_cfg: AiConfig) -> tuple[str, ...]:
    return ai_cfg.a2a.expose if ai_cfg.a2a is not None else ()


def _a2a_allows_anonymous(ai_cfg: AiConfig, name: str) -> bool:
    """Whether agent ``name`` serves unverified A2A callers: its active HTTP stanza allows them."""
    endpoint = ai_cfg.active_endpoint(name)
    return endpoint is not None and endpoint.allow_anonymous
