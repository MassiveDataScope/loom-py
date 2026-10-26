"""ObservabilityRuntime — shared fan-out engine for all Loom modules.

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
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from functools import partial
from time import perf_counter
from typing import Self

from opentelemetry.trace import Link, NoOpTracer, Span, Tracer
from prometheus_client import start_http_server

from loom.core.environment import Environment, resolve_environment
from loom.core.logger.config import configure_logging_from_values
from loom.core.observability.config import (
    ObservabilityConfig,
    OtelObservabilityConfig,
    PrometheusObservabilityConfig,
)
from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.observer.structlog import StructlogLifecycleObserver
from loom.core.observability.protocol import LifecycleObserver, SpanFlusher
from loom.core.observability.span import (
    LoomSpan,
    SpanIdentity,
    apply_terminal_state,
    elapsed_ms,
)
from loom.core.observability.topology import ROOT_SCOPES
from loom.core.tracing.context import active_trace_id
from loom.prometheus.lifecycle import PrometheusLifecycleAdapter


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


def _configure_structlog_logging(config: ObservabilityConfig, environment: Environment) -> None:
    """Configure structlog when the observability config owns logging setup."""
    logger_config = config.log.config
    if logger_config is None:
        return
    extra_processors: tuple[object, ...] = ()
    if config.otel.enabled and config.otel.export_logs:
        # Imported lazily: the OTEL observer module reaches the OpenTelemetry
        # SDK, which is an extras-only dependency (see module docstring).
        from loom.core.observability.observer.otel import build_log_correlation_processor

        extra_processors = (build_log_correlation_processor(),)
    configure_logging_from_values(
        name=logger_config.name,
        environment=environment,
        renderer=logger_config.renderer,
        colors=logger_config.colors,
        level=logger_config.level,
        named_levels=logger_config.named_levels,
        handlers=logger_config.handlers,
        fields=logger_config.fields,
        extra_processors=extra_processors,
    )
    if config.otel.enabled and config.otel.export_logs:
        if config.otel.config is None:
            raise ValueError(
                "observability.otel.export_logs requires observability.otel.config to be provided."
            )
        from loom.core.observability.observer.otel import install_otel_log_export

        install_otel_log_export(config.otel.config)


def _resolve_tracer(config: OtelObservabilityConfig) -> tuple[Tracer, SpanFlusher | None]:
    """Resolve the tracer spans are opened on, and the exporter to flush.

    Three cases, in order: OTEL disabled yields an injected no-op tracer with
    no ambient lookup at all; a configured endpoint yields Loom's own private
    ``TracerProvider``; an empty endpoint yields whatever provider the host
    process installed — or a proxy that resolves to it later.
    """
    if not config.enabled or config.config is None:
        return NoOpTracer(), None
    # Imported lazily: the tracer factory reaches the OpenTelemetry SDK and
    # OTLP exporters, which are extras-only (see module docstring).
    from loom.core.observability.observer.otel import build_tracer

    return build_tracer(config.config)


def _build_observers(
    config: ObservabilityConfig, environment: Environment
) -> list[LifecycleObserver]:
    """Build the observer chain declared by the observability config."""
    observers: list[LifecycleObserver] = []
    if config.log.enabled:
        _configure_structlog_logging(config, environment)
        observers.append(StructlogLifecycleObserver())
    if config.prometheus.enabled:
        observers.append(
            PrometheusLifecycleAdapter(
                pushgateway_url=config.prometheus.pushgateway_url,
            )
        )
    return observers


class ObservabilityRuntime:
    """Shared observability runtime for all Loom modules.

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

        runtime = ObservabilityRuntime.from_config(config.observability)

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
    ) -> None:
        self._observers = tuple(observers)
        self._tracer: Tracer = tracer if tracer is not None else NoOpTracer()
        self._scrape_port = _scrape_port
        self._scrape_addr = _scrape_addr
        self._scrape_server_started = False
        self._span_flusher = _span_flusher
        self._max_span_links = _max_span_links
        self._log = logging.getLogger(__name__)

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
        """Forward one event to each observer with isolated failures."""
        for obs in self._observers:
            try:
                obs.on_event(event)
            except Exception:
                self._log.warning(
                    "observer_error",
                    extra={"observer": type(obs).__name__, "scope": event.scope},
                    exc_info=True,
                )

    @contextmanager
    def span(
        self,
        scope: Scope,
        name: str,
        *,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        **meta: object,
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
            **meta: Domain-specific fields forwarded as top-level keys in ``event.meta``.

        Example::

            with runtime.span(Scope.NODE, "transform", trace_id=tid, flow="ingest"):
                result = transform(message)
        """
        identity = SpanIdentity.build(
            scope, name, trace_id=trace_id, correlation_id=correlation_id, meta=meta
        )
        start_event = identity.start_event()
        started = perf_counter()
        try:
            # Both exception flags default to True: leaving them implicit would
            # record the exception and set the status twice, once here and once
            # in the SDK's own exit handler.
            with (
                active_trace_id(trace_id),
                self._tracer.start_as_current_span(
                    start_event.otel_span_name(),
                    attributes=start_event.otel_attributes(),
                    record_exception=False,
                    set_status_on_exception=False,
                ) as otel_span,
            ):
                self.emit(start_event)
                try:
                    yield
                except Exception as exc:
                    otel_span.record_exception(exc)
                    self._close_span(otel_span, identity.error_event(exc, elapsed_ms(started)))
                    raise
                else:
                    self._close_span(otel_span, identity.end_event(elapsed_ms(started)))
        finally:
            # Outside the ``with``: the span of this run is already ended by
            # the time its exporter is drained.
            self._flush_root(scope)

    def open_span(
        self,
        scope: Scope,
        name: str,
        *,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        links: Sequence[Link] | None = None,
        start_time_ns: int | None = None,
        root: bool = False,
        attributes: Mapping[str, object] | None = None,
        **meta: object,
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
            links: Spans this one fans in from — a batch operation has N
                parents, which a trace tree cannot express.
            start_time_ns: Epoch nanoseconds the work really started at, for a
                span opened after the fact.
            root: Open the span with no parent, whatever the ambient context
                is. A message's own span must land in the message's trace, not
                in the trace of the batch or node that happens to enclose it.
            attributes: Extra fields merged into ``**meta``, for keys that are
                not valid Python identifiers such as ``terminal.reason``.
            **meta: Domain-specific fields forwarded as top-level keys in ``event.meta``.

        Returns:
            The open span handle. The caller owns closing it exactly once with
            :meth:`~loom.core.observability.span.LoomSpan.end` or
            :meth:`~loom.core.observability.span.LoomSpan.fail`.

        Example::

            handle = runtime.open_span(Scope.AGENT, "agent_run", agent=name)
            with always_closed(handle):
                async for frame in frames:
                    yield frame
        """
        identity = SpanIdentity.build(
            scope,
            name,
            trace_id=trace_id,
            correlation_id=correlation_id,
            meta={**meta, **(attributes or {})},
        )
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
    ) -> Self:
        """Build an ``ObservabilityRuntime`` from an ``ObservabilityConfig``.

        Observers are instantiated in order: structlog → Prometheus. When no
        backend is enabled, a
        :class:`~loom.core.observability.observer.noop.NoopObserver` is used so
        the runtime is always safe to call.

        The tracer is resolved from the OTEL section: none when OTEL is off,
        Loom's own provider when an endpoint is configured, and the host's
        provider when the endpoint is empty.

        Calling ``configure_logging_from_values`` from inside this method
        guarantees that the structlog pipeline is configured before any
        observer emits its first event.

        Args:
            config: Unified observability configuration.
            environment: Environment of the process; drives the logger's
                renderer and colour defaults. Resolved like
                :func:`~loom.core.environment.resolve_environment` when ``None``.

        Returns:
            Configured ``ObservabilityRuntime`` ready for use.

        Raises:
            ValueError: If OTEL config is invalid (wrong protocol, missing exporter).
            ValueError: If OTEL log export is enabled without a logger config.
        """
        if (
            config.otel.enabled
            and config.otel.export_logs
            and (not config.log.enabled or config.log.config is None)
        ):
            raise ValueError(
                "observability.otel.export_logs requires observability.log.enabled=True "
                "and observability.log.config to be provided."
            )
        if config.otel.enabled and config.otel.export_logs and config.otel.config is None:
            raise ValueError(
                "observability.otel.export_logs requires observability.otel.config to be provided."
            )

        observers = _build_observers(config, resolve_environment(environment))
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
        )

    @classmethod
    def noop(cls) -> Self:
        """Build a no-op runtime for tests and environments without observability.

        Returns:
            ``ObservabilityRuntime`` backed by a single ``NoopObserver`` and a
            no-op tracer — no observer call, no span, no ambient lookup.
        """
        return cls([NoopObserver()], tracer=NoOpTracer())


__all__ = ["ObservabilityRuntime"]
