"""OpenTelemetry wiring — tracer construction, log correlation, log export.

Only ``opentelemetry-api`` is imported at module scope. The SDK and the OTLP
exporters are extras-only, so every import that touches them is deferred to the
call that needs them and guarded with an actionable error.

Spans are not opened here: they are opened by
:class:`~loom.core.observability.observability.Observability`, the only place
that owns one lexical scope covering both ends of a unit of work.
"""

from __future__ import annotations

import logging
import os
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, cast

from opentelemetry import trace
from opentelemetry._logs import SeverityNumber
from opentelemetry.trace import Tracer
from opentelemetry.util.types import Attributes

from loom.core.config.observability import DEFAULT_SERVICE_NAME, OtelConfig
from loom.core.errors import InvalidValue
from loom.core.log_names import LogField
from loom.core.logger.formats import RecordFields
from loom.core.logger.identity import LogIdentity
from loom.core.logger.install import EXPORT_HANDLER_NAME
from loom.core.model.redaction import reveal_for_driver
from loom.core.observability.diagnostic import ObservabilityEvent, ObservabilityKey
from loom.core.observability.protocol import SpanFlusher
from loom.core.plugins.optional import MissingExtraError

if TYPE_CHECKING:
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import LogRecordExporter

_OTEL_EXTRA: Final = "etl-otel"
_ERR_MISSING_SDK = (
    "OTEL span and log export requires 'opentelemetry-sdk'. "
    "Install it with: pip install 'loom-py[etl-otel]'"
)
_V1_LOGS_SUFFIX = "/v1/logs"
_log = logging.getLogger(__name__)

_INSTRUMENTATION_SCOPE: Final = "loom"
_NANOSECONDS_PER_SECOND: Final = 1_000_000_000

_SEVERITIES: Final = MappingProxyType(
    {
        logging.NOTSET: SeverityNumber.UNSPECIFIED,
        logging.DEBUG: SeverityNumber.DEBUG,
        logging.INFO: SeverityNumber.INFO,
        logging.WARNING: SeverityNumber.WARN,
        logging.ERROR: SeverityNumber.ERROR,
        logging.CRITICAL: SeverityNumber.FATAL,
    }
)
"""The severity of each standard level; its name is the severity text."""


def _load_span_exporter_cls(protocol: str) -> type[Any]:
    """Import the OTLP span exporter class for a protocol, on demand.

    The exporters are extras-only, so the import is deferred until an
    exporter is actually requested.

    Args:
        protocol: Configured OTLP protocol.

    Returns:
        The OTLP span exporter class for the protocol.

    Raises:
        MissingExtraError: If the matching exporter package is not installed.
    """
    if protocol == "grpc":
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        except ImportError as exc:
            raise MissingExtraError(__name__, _OTEL_EXTRA, exc.name) from exc
        return OTLPSpanExporter
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as HttpSpanExporter,
        )
    except ImportError as exc:
        raise MissingExtraError(__name__, _OTEL_EXTRA, exc.name) from exc
    return HttpSpanExporter


def _load_log_exporter_cls(protocol: str) -> type[Any]:
    """Import the OTLP log exporter class for a protocol, on demand.

    Args:
        protocol: Resolved OTLP logs protocol.

    Returns:
        The OTLP log exporter class for the protocol.

    Raises:
        InvalidValue: If the protocol is unsupported.
        MissingExtraError: If its exporter package is not installed.
    """
    if protocol == "grpc":
        try:
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
        except ImportError as exc:
            raise MissingExtraError(__name__, _OTEL_EXTRA, exc.name) from exc
        return OTLPLogExporter
    if protocol != "http/protobuf":
        raise InvalidValue("OTLP protocol must be 'http/protobuf' or 'grpc'.")
    try:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter as HttpLogExporter,
        )
    except ImportError as exc:
        raise MissingExtraError(__name__, _OTEL_EXTRA, exc.name) from exc
    return HttpLogExporter


def build_tracer(
    config: OtelConfig, *, service_name: str = DEFAULT_SERVICE_NAME
) -> tuple[Tracer, SpanFlusher | None]:
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
        service_name: The Resource's ``service.name``, resolved by the caller.

    Returns:
        The tracer, and the provider to force-flush when Loom owns one.

    Raises:
        ConfigError: If the protocol is unsupported.
        MissingExtraError: If its OTLP exporter package is not installed.
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

    resource_attrs = {
        SERVICE_NAME: service_name,
        **config.resource_attributes,
    }
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
    it is announced once at startup.
    """
    provider = trace.get_tracer_provider()
    if not config.adopt_host_id_generator:
        _log.info(
            ObservabilityEvent.HOST_ID_GENERATOR_KEPT,
            extra={ObservabilityKey.PROVIDER: type(provider).__name__},
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


class OtelLogHandler(logging.Handler):
    """Export each record through the OpenTelemetry Logs API, with no exception.

    The body is the record's message and the attributes are its fields as loom's formats
    write them, under their own names: a ``Secret`` is masked, an object is its type name
    and no private attribute of the record is exported. The provider's ``Resource`` carries
    the service, its version and the environment; the process's runtime, which is known
    only after the provider is built, is an attribute of each record. Inside a recording
    span neither of its ids is an attribute: the Logs API takes them from the context. The
    exception is never handed over, so the SDK adds no ``exception.message``: the
    attributes hold its type, its code and its stack.

    Closing the handler shuts the provider down, which the standard library does for every
    handler when the process exits.

    Args:
        provider: The provider the records are emitted through.
        identity: The process the records come from, for its runtime.
    """

    def __init__(self, provider: LoggerProvider, identity: LogIdentity) -> None:
        super().__init__()
        self.set_name(EXPORT_HANDLER_NAME)
        self._provider = provider
        self._logger = provider.get_logger(_INSTRUMENTATION_SCOPE)
        self._identity = identity

    def emit(self, record: logging.LogRecord) -> None:
        """Emit *record* with its fields rendered and no exception; a failure is handled."""
        try:
            view = RecordFields(record)
            attributes = view.fields()
            attributes[LogField.RUNTIME.value] = self._identity.runtime.value
            if LogField.SPAN_ID in attributes:
                del attributes[LogField.TRACE_ID], attributes[LogField.SPAN_ID]
            stack = view.stack
            if stack is not None:
                attributes[LogField.EXCEPTION_STACKTRACE.value] = stack
            severity = _SEVERITIES[view.standard_level]
            self._logger.emit(
                timestamp=int(record.created * _NANOSECONDS_PER_SECOND),
                severity_number=severity,
                severity_text=severity.name,
                body=view.message,
                attributes=cast("Attributes", attributes),
            )
        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Flush the provider's processors."""
        self._provider.force_flush()

    def close(self) -> None:
        """Shut the provider down."""
        self._provider.shutdown()
        super().close()


def build_log_export(
    config: OtelConfig,
    identity: LogIdentity,
    *,
    service_name: str = DEFAULT_SERVICE_NAME,
    exporter: LogRecordExporter | None = None,
) -> OtelLogHandler:
    """Build the stdlib log handler that exports OTEL logs, with a provider of its own.

    Like Loom's tracer provider, the logger provider is private to the handler and never
    installed globally. The handler is not attached to the root logger: pass it to
    :func:`~loom.core.logger.install.configure_logging`, which attaches it.

    Args:
        config: OTLP exporter configuration.
        identity: The process the records come from; its service, version and environment
            are the Resource's.
        service_name: The Resource's ``service.name``, resolved by the caller.
        exporter: Optional pre-built log record exporter. When omitted, one is
            built from ``config``.

    Returns:
        The export handler.

    Raises:
        ImportError: If ``opentelemetry-sdk`` is not installed.
        ConfigError: If the configured protocol is unsupported.
        MissingExtraError: If its OTLP exporter package is not installed.
    """
    config.validate()
    try:
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    except ImportError as exc:
        raise ImportError(_ERR_MISSING_SDK) from exc

    log_exporter = exporter or _build_log_exporter(config)
    process = {key: value for key, value in identity.fields().items() if key != LogField.RUNTIME}
    provider = LoggerProvider(
        resource=Resource.create(
            {**process, SERVICE_NAME: service_name, **config.resource_attributes}
        )
    )
    provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    return OtelLogHandler(provider, identity)


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
    "OtelLogHandler",
    "build_log_export",
    "build_tracer",
]
