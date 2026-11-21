"""HTTP surface of the agent host: ``/run``, ``/stream`` and ``/health``.

Mirrors :func:`loom.rest.fastapi.sql.mount_sql_endpoints`. Mounting is a double
opt-in — an agent needs ``enabled`` **and** a named ``auth`` in ``ai.endpoints``
— every mount is announced with a WARNING carrying its security state, and an
agent that opts in without a usable authenticator aborts start-up instead of
serving anonymously by accident.

Two request-path rules:

* Authentication is evaluated **before** existence, so an anonymous probe for
  an unknown agent gets ``401`` and the surface cannot be used to enumerate the
  agents an application runs.
* The body is capped **while it is read**. The declared ``Content-Length`` is a
  fast path for honest clients, never the cap.

Both are enforced by :mod:`loom.ai._transport`, which the A2A surface shares:
they are properties of an agent transport, not of this wire protocol.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from types import MappingProxyType
from typing import Annotated, Any, Final

import msgspec
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from loom.ai._transport import (
    BODY_OVERHEAD_BYTES,
    HEARTBEAT_MS,
    TransportError,
    always_closed,
    annotate_usage,
    project_output,
    read_body_capped,
    require_caller,
    with_projected_output,
)
from loom.ai.abc import (
    CONVERSATION_ID_MAX_LENGTH,
    AgentEvent,
    AgentResult,
    ErrorEvent,
    FinalEvent,
    HealthState,
    StateShape,
)
from loom.ai.config import AgentEndpointConfig, AiConfig
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.execution import AgentHost
from loom.ai.fastapi.response import AgentJSONResponse, error_response, result_payload
from loom.ai.fastapi.streaming import encode_sse_event, stream_sse
from loom.ai.log_events import AiLogField
from loom.core.config.errors import ConfigError
from loom.core.errors import BoundaryValidationError
from loom.core.identity import Identity, current_identity
from loom.core.logger import LogField
from loom.core.model import LoomFrozenStruct
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.core.observability.span import LoomSpan
from loom.core.tracing import get_trace_id
from loom.rest.auth.abc import Authenticator
from loom.rest.errors import _INTERNAL_ERROR_MESSAGE, _public_message

_logger = logging.getLogger(__name__)

_MEDIA_TYPE_SSE: Final[str] = "text/event-stream"

_STATUS_BY_CODE: Mapping[AgentRunErrorCode, int] = MappingProxyType(
    {
        AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION: 422,
        AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED: 422,
        AgentRunErrorCode.USAGE_LIMIT_EXCEEDED: 422,
        AgentRunErrorCode.STATE_UNDECLARED: 422,
        AgentRunErrorCode.STATE_REQUIRED: 422,
        AgentRunErrorCode.PROVIDER_UNAVAILABLE: 503,
        AgentRunErrorCode.PROVIDER_RATE_LIMITED: 503,
        AgentRunErrorCode.TOOL_UNAVAILABLE: 503,
        AgentRunErrorCode.TOOL_TIMEOUT: 504,
        AgentRunErrorCode.RUN_TIMEOUT: 504,
        AgentRunErrorCode.TOO_MANY_RUNS: 429,
        AgentRunErrorCode.UNAUTHORIZED: 403,
        AgentRunErrorCode.HOOK_FAILED: 500,
        AgentRunErrorCode.CONVERSATION_LOAD_FAILED: 500,
        AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT: 504,
    }
)


class _AgentRunRequest(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Body accepted by ``/run`` and ``/stream``: a prompt, an optional thread and state.

    ``conversation_id`` is opaque to the host: it selects the conversation the
    loader use case receives, when the artifact declares one, and is copied
    verbatim into the output hook's command. Its bounds are enforced here, at decode, so an
    out-of-range value is a ``422`` and never reaches the host.

    ``state`` is ``msgspec.Raw`` rather than a decoded value: this struct is
    decoded by the one module-level :data:`_REQUEST_DECODER` shared by every
    agent, which cannot know any one artefact's declared state shape.
    ``Raw`` stops decoding at this field, so :func:`_decode_state` can parse
    those bytes exactly once, against the artefact's own shape.

    The default is an empty ``Raw``; :func:`_decode_state` treats it and an
    explicit JSON ``null`` alike, as "no state given".
    """

    prompt: str
    conversation_id: (
        Annotated[str, msgspec.Meta(min_length=1, max_length=CONVERSATION_ID_MAX_LENGTH)] | None
    ) = None
    state: msgspec.Raw = msgspec.field(default_factory=msgspec.Raw)


_REQUEST_DECODER = msgspec.json.Decoder(_AgentRunRequest)


def _run_error_response(error: AgentRunError) -> Response:
    """Map a run-error code onto its published HTTP status; a ``5xx`` hides the message."""
    status = _STATUS_BY_CODE.get(error.code, 500)
    if status >= 500:
        _logger.error(
            "Agent run failed (code=%s interaction_id=%s trace_id=%s)",
            error.code,
            error.interaction_id,
            get_trace_id(),
            exc_info=error,
        )
    return error_response(
        status,
        str(error.code),
        _public_message(status, str(error)),
        interaction_id=error.interaction_id,
    )


_ABSENT_STATE: Final[frozenset[bytes]] = frozenset({b"", b"null"})
"""Raw bytes meaning "the caller sent no ``state``": the field's own default
(an empty ``Raw``) and an explicit JSON ``null`` decode to these, and
:class:`_AgentRunRequest` cannot tell them apart (see its own docstring)."""


def _decode_state(
    name: str, state_shape: StateShape | None, raw: msgspec.Raw
) -> Mapping[str, Any] | None:
    """Decode one request's ``state`` bytes exactly once, against its declared shape.

    Args:
        name: Agent this request targets, named in a raised error.
        state_shape: The agent's declared state shape
            (:meth:`~loom.ai.execution.AgentHost.state_shape`), or ``None``
            when it declares neither ``deps_type`` nor ``deps_schema``.
        raw: Undecoded ``state`` bytes from the request body; :data:`_ABSENT_STATE`
            when the caller sent none.

    Returns:
        ``None`` when *raw* is absent. Otherwise the normalised mapping the
        bundle carries: *state_shape*'s own
        :class:`~loom.core.model.LoomType` decoded and turned to builtins
        when it has one, or a plain JSON value under the open
        ``deps_type: dict`` form.

    Raises:
        TransportError: 422 ``STATE_NOT_DECLARED`` when *raw* is given and
            *state_shape* is ``None``; 422 ``INVALID_STATE`` when *raw* does
            not fit the declared shape.
    """
    if bytes(raw) in _ABSENT_STATE:
        return None
    if state_shape is None:
        raise TransportError(
            422,
            "STATE_NOT_DECLARED",
            f"agent {name!r} declares no state (no 'deps_type' or 'deps_schema'); "
            "remove 'state' from the request or declare a state shape on the artefact",
        )
    try:
        return state_shape.decode(bytes(raw))
    except (BoundaryValidationError, msgspec.DecodeError) as exc:
        raise TransportError(422, "INVALID_STATE", str(exc)) from exc


async def _read_request(
    request: Request,
    *,
    name: str,
    max_prompt_bytes: int,
    max_state_bytes: int,
    state_shape: StateShape | None,
) -> tuple[_AgentRunRequest, Mapping[str, Any] | None]:
    """Read and validate the body of one invocation, decoding ``state`` once.

    Args:
        request: Incoming HTTP request.
        name: Agent this request targets.
        max_prompt_bytes: Cap on the ``prompt`` field alone.
        max_state_bytes: Cap on the raw ``state`` bytes alone, measured
            before decode — cheap, and independent of the prompt's own cap.
        state_shape: The agent's declared state shape, read once by the
            caller through :meth:`~loom.ai.execution.AgentHost.state_shape`.

    Returns:
        The decoded request, and its ``state`` normalised against
        *state_shape* — the mapping the dependency bundle carries.

    Raises:
        TransportError: 413 when the body, the prompt or ``state`` exceeds
            its own cap; 422 when the body is not the documented shape, or
            ``state`` does not fit the artefact's declared shape.
    """
    body = await read_body_capped(
        request, max_bytes=max_prompt_bytes + max_state_bytes + BODY_OVERHEAD_BYTES
    )
    try:
        parsed = _REQUEST_DECODER.decode(body)
    except msgspec.DecodeError as exc:
        raise TransportError(422, "INVALID_REQUEST", str(exc)) from exc
    if len(parsed.prompt.encode("utf-8")) > max_prompt_bytes:
        raise TransportError(
            413,
            "PROMPT_TOO_LARGE",
            f"Request body exceeds the maximum accepted size ({max_prompt_bytes} bytes)",
        )
    if len(bytes(parsed.state)) > max_state_bytes:
        raise TransportError(
            413,
            "STATE_TOO_LARGE",
            f"'state' exceeds the maximum accepted size ({max_state_bytes} bytes)",
        )
    state = _decode_state(name, state_shape, parsed.state)
    return parsed, state


def _require_agent(name: str, exposed: Mapping[str, AgentEndpointConfig], host: AgentHost) -> None:
    """Refuse an agent that is not compiled or not exposed over HTTP.

    Raises:
        TransportError: 404 ``AGENT_NOT_FOUND``.
    """
    if name in exposed and host.has_agent(name):
        return
    raise TransportError(404, "AGENT_NOT_FOUND", f"no agent named {name!r} is exposed")


async def _annotated_run(
    host: AgentHost,
    span: LoomSpan,
    name: str,
    body: _AgentRunRequest,
    identity: Identity,
    state: Mapping[str, Any] | None,
) -> AgentResult:
    """Run one agent, publishing what it spent however it ends.

    A run that made three model round trips and then failed its output schema
    cost exactly as much as one that succeeded, so the failure publishes its
    counters too — otherwise a model that fails often would rank better on
    cost than one that answers.

    Args:
        host: Host serving the agent.
        span: Open span of this run.
        name: Agent to run.
        body: Decoded request.
        identity: Verified caller.
        state: This run's state, already normalised by :func:`_decode_state`.

    Returns:
        The completed run's result. Never echoes ``state`` back: it carries
        only what the artefact's own run produced.

    Raises:
        AgentRunError: Whatever the run failed with, unchanged.
    """
    try:
        result = await host.run(
            name,
            body.prompt,
            identity=identity,
            conversation_id=body.conversation_id,
            state=state,
        )
    except AgentRunError as exc:
        annotate_usage(span, exc.usage)
        raise
    annotate_usage(span, result.usage)
    return result


def _make_run_handler(
    host: AgentHost,
    config: AiConfig,
    exposed: Mapping[str, AgentEndpointConfig],
    *,
    path: str,
    observability: Observability,
) -> Callable[[Request, str], object]:
    """Build the handler serving one complete run."""

    async def run_agent(request: Request, name: str) -> Response:
        try:
            identity = require_caller(name, exposed.get(name))
            _require_agent(name, exposed, host)
            body, state = await _read_request(
                request,
                name=name,
                max_prompt_bytes=config.max_prompt_bytes,
                max_state_bytes=config.max_state_bytes,
                state_shape=host.state_shape(name),
            )
            span = observability.open_span(
                Scope.AGENT,
                "agent_run",
                trace_id=get_trace_id(),
                attributes={
                    LogField.HTTP_ROUTE: path,
                    LogField.HTTP_REQUEST_METHOD: "POST",
                    LogField.HTTP_RESPONSE_STATUS_CODE: 200,
                    AiLogField.AGENT: name,
                    LogField.AUTH_MECHANISM: identity.mechanism,
                },
            )
            with always_closed(span), span.as_current():
                result = await _annotated_run(host, span, name, body, identity, state)
            result = project_output(result, host.output_type(name))
            return AgentJSONResponse(content=result_payload(result))
        except TransportError as exc:
            return error_response(exc.status_code, exc.code, exc.message)
        except AgentRunError as exc:
            return _run_error_response(exc)
        except Exception:
            _logger.exception("Unhandled error in the run endpoint of agent %r", name)
            return error_response(500, "INTERNAL_ERROR", _INTERNAL_ERROR_MESSAGE)

    return run_agent


async def _annotating_usage(
    events: AsyncIterator[AgentEvent], span: LoomSpan
) -> AsyncIterator[AgentEvent]:
    """Relay *events*, annotating *span* with the usage the final one carries.

    A stream reports its usage in its terminal event, which is already encoded
    by the time a frame exists, so the annotation is taken here — where the
    event is still typed — and lands on the same closing attributes the
    non-streaming run publishes. A failed run publishes what it burned before
    it failed; a stream that never reaches a terminal event annotates nothing.

    Args:
        events: Run events, terminal event last.
        span: Open span of the run, closed by its owner.

    Yields:
        Every event, unchanged, in order.
    """
    async for event in events:
        if isinstance(event, FinalEvent | ErrorEvent):
            annotate_usage(span, event.usage)
        yield event


def _stream_frames(
    host: AgentHost,
    name: str,
    body: _AgentRunRequest,
    identity: Identity,
    state: Mapping[str, Any] | None,
    *,
    path: str,
    observability: Observability,
) -> AsyncIterator[bytes]:
    """Drive one run inside the response's own task and encode it as SSE.

    The run's single span is opened inside the generator, not around the
    handler: the handler returns as soon as the response exists, while the run
    lasts for as long as the frames are pulled. Opening it here makes the span
    open on the first frame and close on generator exit — including the
    cancellation of a disconnected client, which
    :func:`~loom.ai._transport.always_closed` turns into a terminal event.

    The span carries no ``status_code``: the status line of a stream is
    committed to 200 before the run produces anything, so the field would be a
    constant that reads as "the run succeeded" even when the terminal frame is
    an ``error`` one or the client left mid-run.
    """

    async def _frames() -> AsyncIterator[bytes]:
        span = observability.open_span(
            Scope.AGENT,
            "agent_run",
            trace_id=get_trace_id(),
            attributes={
                LogField.HTTP_ROUTE: path,
                LogField.HTTP_REQUEST_METHOD: "POST",
                AiLogField.AGENT: name,
                LogField.AUTH_MECHANISM: identity.mechanism,
            },
        )
        with always_closed(span):
            try:
                async with host.run_stream(
                    name,
                    body.prompt,
                    identity=identity,
                    conversation_id=body.conversation_id,
                    state=state,
                ) as events:
                    projected_events = with_projected_output(events, host.output_type(name))
                    async for frame in stream_sse(
                        _annotating_usage(projected_events, span),
                        heartbeat_ms=HEARTBEAT_MS,
                    ):
                        yield frame
            except AgentRunError as exc:
                annotate_usage(span, exc.usage)
                yield encode_sse_event(
                    ErrorEvent(code=exc.code, message=str(exc), interaction_id=exc.interaction_id)
                )

    return _frames()


def _make_stream_handler(
    host: AgentHost,
    config: AiConfig,
    exposed: Mapping[str, AgentEndpointConfig],
    *,
    path: str,
    observability: Observability,
) -> Callable[[Request, str], object]:
    """Build the handler serving one run as server-sent events."""

    async def stream_agent(request: Request, name: str) -> Response:
        try:
            identity = require_caller(name, exposed.get(name))
            _require_agent(name, exposed, host)
            body, state = await _read_request(
                request,
                name=name,
                max_prompt_bytes=config.max_prompt_bytes,
                max_state_bytes=config.max_state_bytes,
                state_shape=host.state_shape(name),
            )
        except TransportError as exc:
            return error_response(exc.status_code, exc.code, exc.message)
        return StreamingResponse(
            _stream_frames(
                host,
                name,
                body,
                identity,
                state,
                path=path,
                observability=observability,
            ),
            media_type=_MEDIA_TYPE_SSE,
        )

    return stream_agent


def _make_health_handler(
    host: AgentHost,
    exposed: Mapping[str, AgentEndpointConfig],
) -> Callable[[str], object]:
    """Build the handler serving the cached health of one agent."""

    async def agent_health(name: str) -> Response:
        try:
            _require_agent(name, exposed, host)
        except TransportError as exc:
            return error_response(exc.status_code, exc.code, exc.message)
        health = await host.health(name)
        payload: dict[str, object] = {"status": health.status}
        if health.detail is not None:
            payload["detail"] = health.detail
        if current_identity().is_authenticated:
            payload["checks"] = dict(health.checks)
        status_code = 503 if health.status == HealthState.UNAVAILABLE else 200
        return AgentJSONResponse(content=payload, status_code=status_code)

    return agent_health


def _exposed_agents(
    config: AiConfig, authenticator: Authenticator | None
) -> Mapping[str, AgentEndpointConfig]:
    """Select the agents that opted into HTTP, refusing unusable opt-ins.

    Raises:
        ConfigError: When an agent opts in but no authentication mechanism can
            verify its callers and it does not declare ``allow_anonymous``.
    """
    exposed: dict[str, AgentEndpointConfig] = {}
    for name, endpoint in config.endpoints.items():
        if not endpoint.active:
            continue
        _require_usable_authenticator(name, endpoint, authenticator)
        exposed[name] = endpoint
    return exposed


def _require_usable_authenticator(
    name: str, endpoint: AgentEndpointConfig, authenticator: Authenticator | None
) -> None:
    if authenticator is not None or endpoint.allow_anonymous:
        return
    raise ConfigError(
        f"Agent {name!r}: 'ai.endpoints.{name}.auth' requires a verified caller but the "
        "application configures no authentication. Add the 'app.rest.auth.jwt' section, "
        "pass create_app(authenticator=...), or set 'allow_anonymous: true' explicitly."
    )


def _announce_mount(
    host: AgentHost,
    name: str,
    endpoint: AgentEndpointConfig,
    *,
    prefix: str,
) -> None:
    """Emit the startup WARNING carrying the security state of one mount."""
    if not host.has_agent(name):
        _logger.warning(
            "Agent endpoint configured for agent=%s but no compiled agent has that name: "
            "its routes answer 404",
            name,
        )
        return
    kinds = host.capability_kinds(name)
    _logger.warning(
        "Agent endpoints mounted: path=%s/%s/{run,stream,health} agent=%s auth=%s "
        "allow_anonymous=%s capabilities=%s. %s",
        prefix,
        name,
        name,
        endpoint.auth,
        endpoint.allow_anonymous,
        ",".join(kinds) or "none",
        _identity_notice(endpoint, kinds, conversational=host.has_conversation(name)),
    )


_DEPLOYMENT_CREDENTIAL_KINDS = frozenset({"mcp", "a2a"})
"""Capability kinds reached with the deployment's credential, not the caller's."""


def _identity_notice(
    endpoint: AgentEndpointConfig, kinds: Sequence[str], *, conversational: bool = False
) -> str:
    """State plainly which identity the capability calls of this mount run as.

    ``allow_anonymous`` is not a relaxation of the caller check on top of an
    otherwise verified identity: it removes the identity altogether, so the
    reassuring sentence of an authenticated mount would be false next to it.

    Remote kinds are named separately because their authorisation does not
    depend on who calls: a remote server sees the credential the deployment
    configured for it, shared by every caller of every agent granted it.
    """
    if endpoint.allow_anonymous:
        notice = (
            "allow_anonymous is set, so callers are NOT authenticated: every capability "
            "call runs with no verified identity, and every run spends model tokens on "
            "behalf of an unidentified caller — only 'max_concurrent_runs' and "
            "'run_timeout_ms' bound that cost, there is no rate limit"
        )
        if conversational:
            notice += (
                "; 'conversation' is declared, and every anonymous caller shares one "
                "subject, so threads are separated by 'conversation_id' alone: the id is "
                "the credential"
            )
        return notice
    remote = sorted(_DEPLOYMENT_CREDENTIAL_KINDS.intersection(kinds))
    if not remote:
        return (
            "'auth' only authenticates the caller; every capability call then runs as that "
            "verified identity"
        )
    return (
        "'auth' only authenticates the caller; local capability calls then run as that "
        f"verified identity, but {', '.join(remote)} reach their remote endpoint with the "
        "credential this deployment configured for it, shared by every caller: who calls "
        "does not bound what the remote side allows"
    )


def mount_agent_endpoints(
    app: FastAPI,
    *,
    host: AgentHost,
    config: AiConfig,
    authenticator: Authenticator | None = None,
    observability: Observability | None = None,
    prefix: str = "/agents",
) -> None:
    """Mount ``/run``, ``/stream`` and ``/health`` for every opted-in agent.

    Only agents present in ``ai.endpoints`` with ``enabled`` **and** a named
    ``auth`` are reachable; every other compiled agent exposes no HTTP surface
    at all. Each mount is announced with a WARNING carrying its security state.

    Args:
        app: FastAPI application to mount the routes on.
        host: Entered host serving the agents.
        config: Parsed ``ai:`` section.
        authenticator: Mechanism authenticating callers of the application.
            ``None`` is only acceptable for agents declaring
            ``allow_anonymous``.
        observability: Emits one span per run, over both
            ``/run`` and ``/stream``. This surface is the single owner of that
            span: :class:`~loom.ai.execution.AgentHost` emits none.
        prefix: Path prefix the routes are mounted under.

    Raises:
        ConfigError: When an agent opts into HTTP without a usable
            authenticator and without ``allow_anonymous``.

    Example::

        mount_agent_endpoints(app, host=host, config=ai_config,
                             authenticator=authenticator)
    """
    exposed = _exposed_agents(config, authenticator)
    if not exposed:
        return
    if observability is None:
        observability = Observability.noop()
    run_path = f"{prefix}/{{name}}/run"
    app.add_api_route(
        run_path,
        _make_run_handler(host, config, exposed, path=run_path, observability=observability),
        methods=["POST"],
    )
    stream_path = f"{prefix}/{{name}}/stream"
    app.add_api_route(
        stream_path,
        _make_stream_handler(host, config, exposed, path=stream_path, observability=observability),
        methods=["POST"],
    )
    app.add_api_route(
        f"{prefix}/{{name}}/health",
        _make_health_handler(host, exposed),
        methods=["GET"],
    )
    for name, endpoint in exposed.items():
        _announce_mount(host, name, endpoint, prefix=prefix)
