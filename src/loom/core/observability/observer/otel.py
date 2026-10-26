"""OpenTelemetry wiring — tracer construction, log correlation, log export.

Only ``opentelemetry-api`` is imported at module scope. The SDK and the OTLP
exporters are extras-only, so every import that touches them is deferred to the
call that needs them and guarded with an actionable error.

Spans are not opened here: they are opened by
:class:`~loom.core.observability.runtime.ObservabilityRuntime`, the only place
that owns one lexical scope covering both ends of a unit of work.
"""

from __future__ import annotations

import atexit
import logging
import os
from collections.abc import Callable, MutableMapping
from typing import TYPE_CHECKING, Any, cast

from opentelemetry import _logs as otel_logs
from opentelemetry import trace
from opentelemetry.trace import Tracer

from loom.core.config.observability import OtelConfig
from loom.core.model.redaction import reveal_for_driver
from loom.core.observability.protocol import SpanFlusher

if TYPE_CHECKING:  # SDK types are extras-only: never import them at runtime here.
    from opentelemetry.sdk._logs import LoggingHandler
    from opentelemetry.sdk._logs.export import LogRecordExporter

_ERR_MISSING_GRPC = "OTel protocol='grpc' requires 'opentelemetry-exporter-otlp-proto-grpc'."
_ERR_MISSING_HTTP = (
    "OTel protocol='http/protobuf' requires 'opentelemetry-exporter-otlp-proto-http'."
)
_ERR_MISSING_SDK = (
    "OTEL span and log export requires 'opentelemetry-sdk'. "
    "Install it with: pip install 'loom-py[etl-otel]'"
)
_V1_LOGS_SUFFIX = "/v1/logs"
_log = logging.getLogger(__name__)


def _load_span_exporter_cls(protocol: str) -> type[Any]:
    """Import the OTLP span exporter class for a protocol, on demand.

    The exporters are extras-only, so the import is deferred until an
    exporter is actually requested.

    Args:
        protocol: Configured OTLP protocol.

    Returns:
        The OTLP span exporter class for the protocol.

    Raises:
        ValueError: If the matching exporter package is not installed.
    """
    if protocol == "grpc":
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        except ImportError as exc:
            raise ValueError(_ERR_MISSING_GRPC) from exc
        return OTLPSpanExporter
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as HttpSpanExporter,
        )
    except ImportError as exc:
        raise ValueError(_ERR_MISSING_HTTP) from exc
    return HttpSpanExporter


def _load_log_exporter_cls(protocol: str) -> type[Any]:
    """Import the OTLP log exporter class for a protocol, on demand.

    Args:
        protocol: Resolved OTLP logs protocol.

    Returns:
        The OTLP log exporter class for the protocol.

    Raises:
        ValueError: If the protocol is unsupported or its exporter package
            is not installed.
    """
    if protocol == "grpc":
        try:
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
        except ImportError as exc:
            raise ValueError(_ERR_MISSING_GRPC) from exc
        return OTLPLogExporter
    if protocol != "http/protobuf":
        raise ValueError(_ERR_MISSING_HTTP)
    try:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter as HttpLogExporter,
        )
    except ImportError as exc:
        raise ValueError(_ERR_MISSING_HTTP) from exc
    return HttpLogExporter


def build_tracer(config: OtelConfig) -> tuple[Tracer, SpanFlusher | None]:
    """Build the tracer Loom opens its spans on, and the exporter to flush.

    Where spans go and what trace ids they get are orthogonal. ``endpoint``
    chooses the first: a configured endpoint builds a ``TracerProvider``
    private to Loom, never installed globally, so Loom exports its own spans
    without taking ownership of the process-wide provider; an empty endpoint
    shares whatever provider the host installed — a proxy that resolves lazily
    when there is none yet.

    ``adopt_host_id_generator`` chooses the second when the provider is not
    Loom's. Loom's own provider always gets the generator, which is what makes
    a message's trace id the OTEL trace id from ingestion to death.

    Args:
        config: OTLP exporter configuration.

    Returns:
        The tracer, and the provider to force-flush when Loom owns one.

    Raises:
        ValueError: If the protocol is unsupported or its OTLP exporter
            package is not installed.
        ImportError: If ``opentelemetry-sdk`` is not installed.
    """
    config.validate()
    if config.endpoint is None:
        return _host_tracer(config), None

    exporter = _build_exporter(config)
    try:
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:
        raise ImportError(_ERR_MISSING_SDK) from exc

    # Imported here rather than at module scope: the whole module reaches the
    # SDK, which is extras-only (see module docstring).
    from loom.core.observability.otel_ids import LoomMessageIdGenerator, build_sampler

    resource_attrs = {SERVICE_NAME: config.service_name, **config.resource_attributes}
    provider = TracerProvider(
        resource=Resource.create(resource_attrs),
        id_generator=LoomMessageIdGenerator(),
        sampler=build_sampler(config),
    )
    provider.add_span_processor(
        BatchSpanProcessor(exporter, **reveal_for_driver(config.span_processor_kwargs))
    )
    # After the generator is in place: ``get_tracer`` snapshots it, so a tracer
    # taken before this point would silently keep random trace ids.
    tracer = provider.get_tracer(config.tracer_name, config.tracer_version or None)
    return tracer, provider


def _host_tracer(config: OtelConfig) -> Tracer:
    """Return a tracer from the host-installed provider, adopting ids on request.

    Without adoption the host's random trace ids stand, so a message cannot be
    followed end to end. That is a legitimate choice — Loom does not mutate
    another library's provider behind its owner's back — but it is silent, so
    it is announced once at startup.
    """
    provider = trace.get_tracer_provider()
    if not config.adopt_host_id_generator:
        _log.info(
            "otel_host_provider_random_trace_ids",
            extra={
                "setting": "observability.otel.config.adopt_host_id_generator",
                "provider": type(provider).__name__,
                "consequence": (
                    "spans are emitted with random trace ids; a streaming message "
                    "cannot be followed from ingestion to death under one trace"
                ),
            },
        )
        return trace.get_tracer(config.tracer_name, config.tracer_version or None)

    from loom.core.observability.otel_ids import adopt_host_id_generator

    adopt_host_id_generator(provider)
    # Taken after the install either way: on failure the provider is unchanged,
    # so this is the same tracer the non-adopting branch would have returned.
    return provider.get_tracer(config.tracer_name, config.tracer_version or None)


def _build_exporter(config: OtelConfig) -> Any:
    endpoint = "" if config.endpoint is None else config.endpoint.reveal()
    kwargs: dict[str, Any] = {
        "endpoint": endpoint,
        **reveal_for_driver(config.exporter_kwargs),
    }
    if config.headers:
        kwargs["headers"] = reveal_for_driver(config.headers)
    exporter_cls = _load_span_exporter_cls(config.protocol)
    if config.protocol == "grpc":
        kwargs["insecure"] = config.insecure
    return exporter_cls(**kwargs)


def build_log_correlation_processor() -> Callable[
    [Any, str, MutableMapping[str, Any]],
    MutableMapping[str, Any],
]:
    """Build a structlog processor that adds the active OTEL span IDs.

    Returns:
        A processor function compatible with ``structlog``.
    """

    def _processor(
        logger: Any,
        method: str,
        event_dict: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        span = trace.get_current_span()
        if span.is_recording():
            ctx = span.get_span_context()
            event_dict.setdefault("otel_trace_id", format(ctx.trace_id, "032x"))
            event_dict.setdefault("otel_span_id", format(ctx.span_id, "016x"))
        return event_dict

    return _processor


_LOG_EXPORT_STATE: tuple[Any, LoggingHandler] | None = None


def install_otel_log_export(
    config: OtelConfig,
    *,
    exporter: LogRecordExporter | None = None,
) -> LoggingHandler | None:
    """Install a stdlib log handler that exports OTEL logs.

    The helper is idempotent for the current process. Subsequent calls return
    the already-installed handler.

    Args:
        config: OTLP exporter configuration.
        exporter: Optional pre-built log record exporter. When omitted, one is
            built from ``config``.

    Returns:
        The installed logging handler.

    Raises:
        ImportError: If ``opentelemetry-sdk`` is not installed.
        ValueError: If the configured protocol has no installed OTLP exporter.
    """
    global _LOG_EXPORT_STATE
    config.validate()
    if _LOG_EXPORT_STATE is not None:
        return _LOG_EXPORT_STATE[1]

    try:
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    except ImportError as exc:
        raise ImportError(_ERR_MISSING_SDK) from exc

    log_exporter = exporter or _build_log_exporter(config)
    provider = LoggerProvider(
        resource=Resource.create(
            {
                SERVICE_NAME: config.service_name,
                **config.resource_attributes,
            }
        )
    )
    provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    otel_logs.set_logger_provider(provider)

    handler = LoggingHandler(level=logging.NOTSET, logger_provider=provider)
    logging.getLogger().addHandler(handler)
    atexit.register(provider.shutdown)
    _LOG_EXPORT_STATE = (provider, handler)
    return handler


def _build_log_exporter(config: OtelConfig) -> LogRecordExporter:
    kwargs: dict[str, Any] = {**reveal_for_driver(config.exporter_kwargs)}
    endpoint = _resolve_log_endpoint(config)
    if endpoint is not None:
        kwargs["endpoint"] = endpoint
    if config.headers:
        kwargs["headers"] = reveal_for_driver(config.headers)

    protocol = _resolve_log_protocol(config)
    exporter_cls = _load_log_exporter_cls(protocol)
    if protocol == "grpc":
        kwargs["insecure"] = config.insecure
    return cast("LogRecordExporter", exporter_cls(**kwargs))


def _resolve_log_endpoint(config: OtelConfig) -> str | None:
    env_endpoint = os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "").strip()
    if env_endpoint:
        return env_endpoint

    configured = "" if config.endpoint is None else config.endpoint.reveal()
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", configured).strip()
    if not endpoint:
        return None
    if endpoint.endswith("/v1/traces"):
        return endpoint[: -len("/v1/traces")] + _V1_LOGS_SUFFIX
    if endpoint.endswith(_V1_LOGS_SUFFIX):
        return endpoint
    return endpoint.rstrip("/") + _V1_LOGS_SUFFIX


def _resolve_log_protocol(config: OtelConfig) -> str:
    env_protocol = os.getenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "").strip()
    if env_protocol:
        return env_protocol
    return os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", config.protocol).strip()


__all__ = [
    "build_log_correlation_processor",
    "build_tracer",
    "install_otel_log_export",
]
