"""Observability — shared fan-out engine for all Loom modules.

The tracer factory is imported lazily from ``from_config``: it reaches the
OpenTelemetry SDK and OTLP exporters, which ship as extras only. Keeping that
import out of module scope is what lets ``loom.core.observability`` be imported
with nothing but ``opentelemetry-api`` installed.

Loom never calls ``set_tracer_provider``. The OTEL *context* is global, the
*provider* is not: a span from Loom's own provider therefore nests correctly
under a host span (logfire, an operator agent) and correctly parents the host
spans opened inside it, with each provider exporting only its own spans. That
is what lets Loom coexist with a host SDK without owning anything global.
"""

from __future__ import annotations

import logging
from collections.abc import Generator, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from functools import partial
from time import perf_counter
from typing import Self

from opentelemetry.trace import INVALID_SPAN, Link, NoOpTracer, Span, Tracer
from prometheus_client import start_http_server

from loom.core.environment import Environment, resolve_environment
from loom.core.logger import NO_ATTRIBUTES, LogField, LogRuntime, SpanAttributes
from loom.core.logger.install import LogInstallation, configure_logging
from loom.core.observability.config import (
    ObservabilityConfig,
    OtelObservabilityConfig,
    PrometheusObservabilityConfig,
)
from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.log_events import ObservabilityLogEvent
from loom.core.observability.observer.log import LogLifecycleObserver
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.protocol import LifecycleObserver, SpanFlusher
from loom.core.observability.span import (
    LoomSpan,
    SpanIdentity,
    apply_terminal_state,
    elapsed_ms,
)
from loom.core.observability.topology import ROOT_SCOPES
from loom.core.tracing.context import active_trace_id, get_trace_id
from loom.prometheus.lifecycle import PrometheusLifecycleObserver


def _resolve_scrape_port(cfg: PrometheusObservabilityConfig) -> int | None:
    """Return the scrape server port for streaming processes, or None.

    Returns None when Prometheus is disabled, a Pushgateway is configured
    (batch/ETL mode), or no port is declared in the config.
    """
    if not cfg.enabled:
        return None
    if cfg.pushgateway_url is not None:
        return None
    if cfg.config is None:
        return None
    return cfg.config.port


def _resolve_scrape_addr(cfg: PrometheusObservabilityConfig) -> str:
    """Return the bind address for the standalone scrape server."""
    if cfg.config is None:
        return "127.0.0.1"
    return cfg.config.bind_address


def _configure_logging(
    config: ObservabilityConfig,
    environment: Environment,
    service_name: str | None,
    service_version: str | None,
) -> LogInstallation:
    """Make the process's records safe and install the log format.

    With OTEL, the records carry the ids of the recording span and, when
    ``export_logs`` is set, the OTEL log export is installed beside the format.
    """
    otel = config.otel
    if not otel.enabled or otel.config is None:
        return configure_logging(
            config.log,
            environment=environment,
            service_name=service_name,
            service_version=service_version,
        )
    from loom.core.observability.observer.otel import install_otel_log_export, otel_trace_ids

    extra_handlers = (install_otel_log_export(otel.config),) if otel.export_logs else ()
    return configure_logging(
        config.log,
        environment=environment,
        service_name=service_name,
        service_version=service_version,
        extra_handlers=extra_handlers,
        trace_ids=otel_trace_ids,
    )


def _resolve_tracer(config: OtelObservabilityConfig) -> tuple[Tracer, SpanFlusher | None]:
    """Resolve the tracer spans are opened on, and the exporter to flush.

    Three cases, in order: OTEL disabled yields an injected no-op tracer with
    no ambient lookup at all; a configured endpoint yields Loom's own private
    ``TracerProvider``; an empty endpoint yields whatever provider the host
    process installed — or a proxy that resolves to it later.
    """
    if not config.enabled or config.config is None:
        return NoOpTracer(), None
    from loom.core.observability.observer.otel import build_tracer

    return build_tracer(config.config)


def _build_observers(config: ObservabilityConfig) -> list[LifecycleObserver]:
    """Build the observer chain declared by the observability config."""
    observers: list[LifecycleObserver] = []
    if config.log.enabled:
        observers.append(LogLifecycleObserver())
    if config.prometheus.enabled:
        observers.append(
            PrometheusLifecycleObserver(
                pushgateway_url=config.prometheus.pushgateway_url,
            )
        )
    return observers


class Observability:
    """Shared observability for all Loom modules.

    Emits :class:`~loom.core.observability.event.LifecycleEvent` to every
    registered observer. Observer failures are logged and discarded — they
    never interrupt the main execution path.

    Spans are opened on the injected tracer. No tracer means no tracing at
    all: an injected no-op tracer, with no lookup of the ambient provider.

    Use :meth:`from_config` to build an instance from YAML-parsed config.
    Use :meth:`noop` in tests and environments without observability.

    Args:
        observers: Sequence of lifecycle observers to fan events out to.
        tracer: Tracer every span is opened on. Defaults to a no-op tracer.

    Example::

        runtime = Observability.from_config(config.observability)

        with runtime.span(Scope.USE_CASE, "CreateOrder", trace_id=tid):
            result = use_case.execute(command)
    """

    def __init__(
        self,
        observers: Sequence[LifecycleObserver],
        *,
        tracer: Tracer | None = None,
        _scrape_port: int | None = None,
        _scrape_addr: str = "127.0.0.1",
        _span_flusher: SpanFlusher | None = None,
        _max_span_links: int = 128,
        _logging: LogInstallation | None = None,
    ) -> None:
        self._observers = tuple(observers)
        self._logging = _logging
        self._tracer: Tracer = tracer if tracer is not None else NoOpTracer()
        self._records_spans = not isinstance(self._tracer, NoOpTracer)
        self._scrape_port = _scrape_port
        self._scrape_addr = _scrape_addr
        self._scrape_server_started = False
        self._span_flusher = _span_flusher
        self._max_span_links = _max_span_links
        self._log = logging.getLogger(__name__)

    def running_as(self, runtime: LogRuntime) -> None:
        """Stamp the log records of this process as emitted by *runtime*."""
        if self._logging is not None:
            self._logging.running_as(runtime)

    @property
    def observers(self) -> tuple[LifecycleObserver, ...]:
        """Return the configured observer chain."""
        return self._observers

    @property
    def tracer(self) -> Tracer:
        """Return the tracer every span of this runtime is opened on."""
        return self._tracer

    @property
    def max_span_links(self) -> int:
        """Return the upper bound on the links one batch span may carry."""
        return self._max_span_links

    def start_scrape_server(self) -> None:
        """Start a standalone Prometheus HTTP scrape server on the configured port.

        No-op when no port is configured or the server is already running.
        Intended for long-running streaming processes that have no existing
        HTTP server to mount ``/metrics`` on. Safe to call multiple times.

        Raises:
            OSError: If the port is already in use by another process.
        """
        if self._scrape_port is None or self._scrape_server_started:
            return
        start_http_server(self._scrape_port, addr=self._scrape_addr)
        self._scrape_server_started = True

    def emit(self, event: LifecycleEvent) -> None:
        """Emit one lifecycle event to all registered observers.

        Observer failures are caught, logged at WARNING, and discarded.

        Use this for genuinely unpaired events only: anything with a start and
        an end belongs in :meth:`span` or :meth:`open_span`, which also open
        the matching OTEL span.

        Args:
            event: Lifecycle event to dispatch.
        """
        self._dispatch(event)

    def _flush_root(self, scope: Scope) -> None:
        """Drain Loom's own span exporter once a root-scope span has ended.

        A batch processor would otherwise still be holding the spans of a
        short-lived process — an ETL run, a job — when it exits. Only Loom's
        own provider is drained: a host-owned provider is not Loom's to drive.
        """
        if self._span_flusher is not None and scope in ROOT_SCOPES:
            self._span_flusher.force_flush()

    def _dispatch(self, event: LifecycleEvent) -> None:
        """Forward one event to each observer with isolated failures, then drop its exception."""
        for obs in self._observers:
            try:
                obs.on_event(event)
            except Exception:
                self._log.warning(
                    ObservabilityLogEvent.OBSERVER_FAILED,
                    extra={
                        LogField.OBSERVER: type(obs).__name__,
                        LogField.SCOPE: event.scope.value,
                    },
                    exc_info=True,
                )
        event.drop_failure()

    def use_case(self, name: str, attributes: SpanAttributes) -> AbstractContextManager[None]:
        """The ``use_case`` scope of one execution of *name*, in the running trace.

        The engine opens it for every execution, whatever runtime asked for it.
        """
        return self.span(Scope.USE_CASE, name, trace_id=get_trace_id(), attributes=attributes)

    @contextmanager
    def span(
        self,
        scope: Scope,
        name: str,
        *,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        id: str | None = None,
        attributes: SpanAttributes = NO_ATTRIBUTES,
    ) -> Generator[None, None, None]:
        """Open one span, emitting ``START`` on entry and ``END``/``ERROR`` on exit.

        The OTEL span is made current for the whole body, so anything that
        opens a span inside — a Loom span, a third-party instrumentation, the
        host SDK — nests under it. Duration is measured with ``perf_counter``
        and attached to the closing event. If the body raises an ``Exception``,
        the failure is recorded on the span, ``ERROR`` is emitted, and the
        exception is re-raised.

        Precondition: the ``with`` block is entered and exited in the same
        context. Driving it by hand across ``asend`` boundaries of an async
        generator detaches a context token in a context that never attached
        it. Use :meth:`open_span` for that shape.

        Args:
            scope: Logical unit of work — one of the values in
                :class:`~loom.core.observability.event.Scope`.
            name: Operation name within the scope.
            trace_id: Trace identifier propagated to both events.
            correlation_id: Business lineage identifier propagated to both events.
            id: Stable identifier of this operation instance, such as a task id.
            attributes: Fields of the span and of both events, under vocabulary keys.

        Example::

            with runtime.span(Scope.NODE, "transform", attributes={LogField.COUNT: 3}):
                result = transform(message)
        """
        identity = SpanIdentity(scope, name, trace_id, correlation_id, id, attributes)
        start_event = identity.start_event()
        started = perf_counter()
        try:
            with active_trace_id(trace_id), self._current_otel_span(start_event) as otel_span:
                self.emit(start_event)
                try:
                    yield
                except Exception as exc:
                    self._close_span(otel_span, identity.error_event(exc, elapsed_ms(started)))
                    raise
                else:
                    self._close_span(otel_span, identity.end_event(elapsed_ms(started)))
        finally:
            self._flush_root(scope)

    def _current_otel_span(self, start_event: LifecycleEvent) -> AbstractContextManager[Span]:
        """The OTEL span of *start_event*, made current; with no tracer, an invalid one.

        A no-op tracer would make a non-recording span current and hide the
        caller's span from whatever annotates it, so none is opened and the
        caller's span stays current, untouched by this scope's outcome.
        """
        if not self._records_spans:
            return nullcontext(INVALID_SPAN)
        return self._tracer.start_as_current_span(
            start_event.otel_span_name(),
            attributes=start_event.otel_attributes(),
            record_exception=False,
            set_status_on_exception=False,
        )

    def open_span(
        self,
        scope: Scope,
        name: str,
        *,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        id: str | None = None,
        links: Sequence[Link] | None = None,
        start_time_ns: int | None = None,
        root: bool = False,
        attributes: SpanAttributes = NO_ATTRIBUTES,
    ) -> LoomSpan:
        """Open a span whose end does not share a lexical scope with its start.

        The returned handle is never made current, so it can be closed from a
        different context than the one that opened it — the shape a streaming
        response has, where the span opens on one ``asend`` and closes on
        another. Its parent is captured now, from the current context.

        Prefer :meth:`span` whenever the span opens and closes together.

        Args:
            scope: Logical unit of work — one of the values in
                :class:`~loom.core.observability.event.Scope`.
            name: Operation name within the scope.
            trace_id: Trace identifier propagated to every event of the span.
                When it is a 32-character hex id and the span turns out to be a
                root, it also becomes the OTEL trace id.
            correlation_id: Business lineage identifier propagated likewise.
            id: Stable identifier of this operation instance.
            links: Spans this one fans in from — a batch operation has N
                parents, which a trace tree cannot express.
            start_time_ns: Epoch nanoseconds the work really started at, for a
                span opened after the fact.
            root: Open the span with no parent, whatever the ambient context
                is. A message's own span must land in the message's trace, not
                in the trace of the batch or node that happens to enclose it.
            attributes: Fields of the span and of its events, under vocabulary keys.

        Returns:
            The open span handle. The caller owns closing it exactly once with
            :meth:`~loom.core.observability.span.LoomSpan.end` or
            :meth:`~loom.core.observability.span.LoomSpan.fail`.

        Example::

            handle = runtime.open_span(Scope.AGENT, "agent_run", attributes=agent_fields)
            with always_closed(handle):
                async for frame in frames:
                    yield frame
        """
        identity = SpanIdentity(scope, name, trace_id, correlation_id, id, attributes)
        with active_trace_id(trace_id):
            return LoomSpan.open(
                tracer=self._tracer,
                identity=identity,
                emit=self.emit,
                on_closed=partial(self._flush_root, scope),
                links=links,
                start_time_ns=start_time_ns,
                root=root,
            )

    def _close_span(self, otel_span: Span, event: LifecycleEvent) -> None:
        """Stamp the outcome on the span and emit its closing event."""
        apply_terminal_state(otel_span, event)
        self.emit(event)

    @classmethod
    def from_config(
        cls,
        config: ObservabilityConfig,
        *,
        environment: Environment | str | None = None,
        service_name: str | None = None,
        service_version: str | None = None,
    ) -> Self:
        """Build an ``Observability`` from an ``ObservabilityConfig``.

        Observers are instantiated in order: log → Prometheus. When no
        backend is enabled, a
        :class:`~loom.core.observability.observer.noop.NoopObserver` is used so
        the runtime is always safe to call.

        The tracer is resolved from the OTEL section: none when OTEL is off,
        Loom's own provider when an endpoint is configured, and the host's
        provider when the endpoint is empty.

        The record factory, the value filter and the log format are installed
        from inside this method, whether or not ``log`` is enabled, so every
        record is safe before any observer emits its first event. ``log.enabled``
        only turns the lifecycle records on.

        Args:
            config: Unified observability configuration.
            environment: Environment of the process; picks the default log
                format and is stamped on every record. Resolved like
                :func:`~loom.core.environment.resolve_environment` when ``None``.
            service_name: ``app.name``, stamped on every log record.
            service_version: ``app.version``, stamped on every log record.

        Returns:
            Configured ``Observability`` ready for use.

        Raises:
            ValueError: If OTEL config is invalid (wrong protocol, missing exporter).
            ConfigError: If ``observability.log.format`` is not registered.
        """
        if config.otel.enabled and config.otel.export_logs and config.otel.config is None:
            raise ValueError(
                "observability.otel.export_logs requires observability.otel.config to be provided."
            )

        installation = _configure_logging(
            config, resolve_environment(environment), service_name, service_version
        )
        observers = _build_observers(config)
        tracer, flusher = _resolve_tracer(config.otel)
        scrape_port = _resolve_scrape_port(config.prometheus)
        scrape_addr = _resolve_scrape_addr(config.prometheus)
        return cls(
            observers or [NoopObserver()],
            tracer=tracer,
            _scrape_port=scrape_port,
            _scrape_addr=scrape_addr,
            _span_flusher=flusher,
            _max_span_links=(
                config.otel.config.max_span_links if config.otel.config is not None else 128
            ),
            _logging=installation,
        )

    @classmethod
    def noop(cls) -> Self:
        """Build a no-op runtime for tests and environments without observability.

        Returns:
            ``Observability`` backed by a single ``NoopObserver`` and a
            no-op tracer — no observer call, no span, no ambient lookup.
        """
        return cls([NoopObserver()], tracer=NoOpTracer())


__all__ = ["Observability"]
