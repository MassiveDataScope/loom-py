"""OpenTelemetry wiring — tracer construction, log correlation, log export.

Only ``opentelemetry-api`` is imported at module scope. The SDK and the OTLP
exporters are extras-only, so every import that touches them is deferred to the
call that needs them and guarded with an actionable error.

Spans are not opened here: they are opened by
:class:`~loom.core.observability.observability.Observability`, the only place
that owns one lexical scope covering both ends of a unit of work.
"""

from __future__ import annotations

import atexit
import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar, cast

from opentelemetry import _logs as otel_logs
from opentelemetry import trace
from opentelemetry.trace import Tracer

from loom.core.config.observability import OtelConfig
from loom.core.logger import LogField
from loom.core.logger.record import loom_trace_ids
from loom.core.model.redaction import reveal_for_driver
from loom.core.observability.log_events import ObservabilityLogEvent, ObservabilityLogField
from loom.core.observability.protocol import SpanFlusher

if TYPE_CHECKING:
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
    tracer = provider.get_tracer(config.tracer_name, config.tracer_version or None)
    return tracer, provider


def _host_tracer(config: OtelConfig) -> Tracer:
    """Return a tracer from the host-installed provider, adopting ids on request.

    Without adoption the host's random trace ids stand, so a message cannot be
    followed end to end. That is a legitimate choice — Loom does not mutate
    another library's provider behind its owner's back — but it is silent, so
    it is announced once at startup: spans then carry random trace ids, and a
    streaming message cannot be followed from ingestion to death under one trace.
    """
    provider = trace.get_tracer_provider()
    if not config.adopt_host_id_generator:
        _log.info(
            ObservabilityLogEvent.HOST_TRACE_IDS_RANDOM,
            extra={
                LogField.SETTING: "observability.otel.config.adopt_host_id_generator",
                ObservabilityLogField.OTEL_PROVIDER: type(provider).__name__,
            },
        )
        return trace.get_tracer(config.tracer_name, config.tracer_version or None)

    from loom.core.observability.otel_ids import adopt_host_id_generator

    adopt_host_id_generator(provider)
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


def otel_trace_ids() -> tuple[str | None, str | None]:
    """The ids every record carries: the recording OTEL span's, else loom's trace id.

    The one trace id of a record. Loom's own spans adopt loom's trace id as
    their OTEL trace id, so the two agree whenever a loom span is recording.
    """
    span = trace.get_current_span()
    if not span.is_recording():
        return loom_trace_ids()
    ctx = span.get_span_context()
    return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")


class OtelLogExport:
    """The process's one OTEL log export: its provider and the handler that feeds it.

    :func:`install_otel_log_export` builds it once per process; :meth:`installed`
    returns it, and :meth:`forget` drops it so the next install builds another,
    which a test does to start from a fresh export.

    Args:
        provider: The OTEL ``LoggerProvider`` that exports the records.
        handler: The stdlib handler writing into *provider*.
    """

    _installed: ClassVar[OtelLogExport | None] = None

    __slots__ = ("handler", "_provider")

    def __init__(self, provider: Any, handler: LoggingHandler) -> None:
        self._provider = provider
        self.handler = handler

    def flush(self) -> None:
        """Export every record the handler has buffered."""
        self._provider.force_flush()

    @classmethod
    def installed(cls) -> OtelLogExport | None:
        """The export installed in this process, or ``None``."""
        return cls._installed

    @classmethod
    def forget(cls) -> None:
        """Drop the installed export: the next install builds a new one."""
        cls._installed = None


def install_otel_log_export(
    config: OtelConfig,
    *,
    exporter: LogRecordExporter | None = None,
) -> LoggingHandler:
    """Build the stdlib log handler that exports OTEL logs.

    The handler is not attached to the root logger: pass it to
    :func:`~loom.core.logger.install.configure_logging`, which attaches it and
    puts the value filter on it like on every root handler. The helper is
    idempotent for the current process: later calls return the same handler.

    Args:
        config: OTLP exporter configuration.
        exporter: Optional pre-built log record exporter. When omitted, one is
            built from ``config``.

    Returns:
        The export handler.

    Raises:
        ImportError: If ``opentelemetry-sdk`` is not installed.
        ValueError: If the configured protocol has no installed OTLP exporter.
    """
    config.validate()
    installed = OtelLogExport.installed()
    if installed is not None:
        return installed.handler

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
    atexit.register(provider.shutdown)
    OtelLogExport._installed = OtelLogExport(provider, handler)
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
    "build_tracer",
    "install_otel_log_export",
    "otel_trace_ids",
]
