"""Inbound A2A transport: one JSON-RPC endpoint and one card per published agent.

Serves the A2A protocol on top of
:class:`~loom.ai.execution.AgentHost`, reusing the pure projections of
:mod:`loom.ai.a2a.card` and :mod:`loom.ai.a2a.events`. Nothing here re-derives
the card or the event mapping: this module is transport only, and the rules
every agent transport shares — the body cap, the caller check, the span that
survives a disconnect and the heartbeat race — come from
:mod:`loom.ai._transport` rather than from the HTTP surface's private names.

``fasta2a.schema`` supplies the wire contract and the error taxonomy; the
ready-made ``fasta2a`` application is not mounted. Every run goes through
:class:`~loom.ai.execution.AgentHost`, under the caller's ``Identity``,
``max_concurrent_runs``, ``run_timeout_ms``, the run span and the outward
redaction.

This module is composition only. The three layers it assembles each state
their own rules: :mod:`loom.ai.a2a._rpc` owns the JSON-RPC envelope and the
error taxonomy, :mod:`loom.ai.a2a._handlers` owns the methods, and
:mod:`loom.ai.a2a._binding` owns what may be published and how safely — the
start-up refusals and the authentication exclusions included.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Final

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response

from loom.ai._transport import (
    BODY_OVERHEAD_BYTES,
    TransportError,
    read_body_capped,
    require_caller,
)
from loom.ai.a2a._binding import (
    DEFAULT_AGENTS_PREFIX,
    PublishedAgent,
    announce_publication,
    mounted_exclusions,
    published_agents,
    register_card_exclusions,
    require_usable_authenticator,
    verify_exclusions,
)
from loom.ai.a2a._handlers import Call, Handler, make_handlers
from loom.ai.a2a._rpc import (
    RpcFault,
    decode_envelope,
    error_response,
    invalid_params_error,
    method_not_found_error,
    require_method,
    unexpected_error,
)
from loom.ai.a2a.card import DEFAULT_A2A_PREFIX, card_path
from loom.ai.compiler import AgentPlan
from loom.ai.config import AiConfig
from loom.ai.event import AiEvent, AiKey
from loom.ai.execution import AgentHost
from loom.ai.fastapi.response import error_response as flat_error_response
from loom.core.identity import Identity
from loom.core.observability.observability import Observability
from loom.rest.auth.abc import Authenticator

_logger = logging.getLogger(__name__)

_MEDIA_TYPE_JSON: Final[str] = "application/json"


def _make_rpc_handler(
    agent: PublishedAgent, handlers: Mapping[str, Handler], *, max_prompt_bytes: int
) -> Callable[[Request], Awaitable[Response]]:
    """Build the JSON-RPC endpoint of one published agent.

    Authentication is the one failure answered outside the protocol: it
    precedes JSON-RPC, so an unverified caller gets a flat HTTP 401 and never
    reaches the dispatcher. Everything from the body read onwards answers in
    JSON-RPC — an oversized body included. Answering that one with a flat 413
    gave the same logical failure two shapes, since a prompt over the cap
    detected one layer later (``_extract_prompt``) already answers ``-32602``.
    """
    body_cap = max_prompt_bytes + BODY_OVERHEAD_BYTES

    async def dispatch(request: Request, identity: Identity) -> Response:
        request_id: int | str | None = None
        try:
            body = await read_body_capped(request, max_bytes=body_cap)
            envelope = decode_envelope(body)
            request_id = envelope.id
            method = require_method(envelope)
            handler = handlers.get(method)
            if handler is None:
                return error_response(request_id, method_not_found_error(method))
            return await handler(Call(agent, request_id, envelope.params, identity))
        except TransportError as exc:
            return error_response(request_id, invalid_params_error(exc.message))
        except RpcFault as exc:
            return error_response(request_id, exc.error)
        except Exception:
            _logger.exception(AiEvent.ENDPOINT_FAILED, extra={AiKey.AGENT: agent.name})
            return error_response(request_id, unexpected_error())

    async def serve_rpc(request: Request) -> Response:
        try:
            identity = require_caller(agent.name, agent.endpoint)
        except TransportError as exc:
            return flat_error_response(exc.status_code, exc.code, exc.message)
        return await dispatch(request, identity)

    return serve_rpc


def _make_card_handler(card: bytes) -> Callable[[], Response]:
    """Build the handler serving one agent's card, encoded once at bind time."""

    def serve_card() -> Response:
        return Response(content=card, media_type=_MEDIA_TYPE_JSON)

    return serve_card


def mount_a2a_endpoints(
    app: FastAPI,
    *,
    host: AgentHost,
    config: AiConfig,
    plans: Sequence[AgentPlan],
    authenticator: Authenticator | None = None,
    exclude_paths: Sequence[str] = (),
    observability: Observability | None = None,
    prefix: str = DEFAULT_A2A_PREFIX,
    agents_prefix: str = DEFAULT_AGENTS_PREFIX,
) -> None:
    """Publish the agents named in ``ai.a2a.expose`` over A2A.

    With no ``ai.a2a`` section nothing is mounted and nothing is logged. Each published
    agent gets one anonymous card route and one authenticated JSON-RPC route serving
    ``message/send`` and ``message/stream``; every other A2A method answers a named
    JSON-RPC error, as its card says.

    Args:
        app: FastAPI application to mount the routes on.
        host: Entered host serving the agents.
        config: Parsed ``ai:`` section.
        plans: Compiled plans the host was built from; the card is projected
            from them, which is why they are passed explicitly instead of read
            back out of the host.
        authenticator: Mechanism authenticating callers; its name selects the
            security scheme the card advertises.
        exclude_paths: Extra authentication exclusions to validate against the
            invocation surface. The exclusions mounted on the
            application's :class:`AuthenticationMiddleware` are read directly
            and always validated; this argument only adds to them.
        observability: Runtime emitting one span per run.
        prefix: Path prefix the A2A surface is mounted under.
        agents_prefix: Path prefix the HTTP agent surface is mounted under,
            guarded by the same exclusion check.

    Raises:
        AgentCompilationError: With ``AUTH_EXCLUSION_OVERLAPS_AGENTS`` when an
            exclusion covers an invocation path.
        ConfigError: When an agent is published without a usable authenticator
            and without an explicit anonymous opt-out, or when the card paths
            cannot be excluded from authentication.

    Example::

        mount_a2a_endpoints(app, host=host, config=ai_config, plans=plans,
                           authenticator=authenticator, exclude_paths=("/health",))
    """
    if config.a2a is None:
        return
    mechanism = authenticator.name if authenticator is not None else None
    agents = published_agents(
        config=config,
        plans={plan.name: plan for plan in plans},
        host=host,
        mechanism=mechanism,
        prefix=prefix,
    )
    if not agents:
        return
    for agent in agents:
        require_usable_authenticator(agent, authenticator)
    cards = [card_path(agent.name, prefix=prefix) for agent in agents]
    verify_exclusions(
        (*exclude_paths, *mounted_exclusions(app)),
        frozenset(cards),
        prefix=prefix,
        agents_prefix=agents_prefix,
    )
    if authenticator is not None:
        register_card_exclusions(app, cards)
    handlers = make_handlers(
        host,
        config,
        prefix=prefix,
        observability=observability or Observability.noop(),
    )
    for agent, path in zip(agents, cards, strict=True):
        app.add_api_route(path, _make_card_handler(agent.card), methods=["GET"])
        app.add_api_route(
            f"{prefix}/{agent.name}",
            _make_rpc_handler(agent, handlers, max_prompt_bytes=config.max_prompt_bytes),
            methods=["POST"],
        )
        announce_publication(agent, prefix=prefix)
