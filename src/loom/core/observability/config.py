"""Unified observability configuration structs for all Loom modules."""

from __future__ import annotations

import msgspec

from loom.core.config.observability import OtelConfig
from loom.core.logger.config import LoggerConfig
from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import Dsn


class PrometheusConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Prometheus scrape endpoint configuration.

    Args:
        path: HTTP path where ``/metrics`` is served. Used by REST services
            that mount the scrape endpoint on an existing FastAPI app.
        port: Port for the standalone HTTP scrape server. Used by long-running
            streaming processes that have no existing HTTP server.
            ``None`` disables the standalone server (default).
        bind_address: Network interface the standalone scrape server binds to.
            Defaults to ``"127.0.0.1"`` (loopback only). Set to ``"0.0.0.0"``
            only when the Prometheus scraper cannot reach the pod via loopback.
    """

    path: str = "/metrics"
    port: int | None = None
    bind_address: str = "127.0.0.1"


class LogObservabilityConfig(
    LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True
):
    """Structured-log observability backend configuration.

    Args:
        enabled: Enable the structlog observer. Defaults to ``True``.
        config: Optional full logger configuration. When ``None``, logging
            must have been configured externally before
            ``ObservabilityRuntime.from_config`` is called.
    """

    enabled: bool = True
    config: LoggerConfig | None = None


class OtelObservabilityConfig(
    LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True
):
    """OpenTelemetry traces and logs backend configuration.

    Args:
        enabled: Enable the OTEL observer. Defaults to ``False``.
        export_logs: Bridge structlog entries to the OTEL Logs SDK so logs
            reach the same collector as traces. Requires the OTEL SDK to be
            installed. Defaults to ``False``.
        config: OTLP exporter configuration. Required when ``enabled`` is
            ``True`` and an endpoint is needed.
    """

    enabled: bool = False
    export_logs: bool = False
    config: OtelConfig | None = None


class PrometheusObservabilityConfig(
    LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True
):
    """Prometheus metrics backend configuration.

    Args:
        enabled: Enable the Prometheus lifecycle adapter. Defaults to ``False``.
        pushgateway_url: Pushgateway URL for batch jobs. When ``None``,
            metrics are served on ``/metrics`` (suitable for long-running
            services). When set, metrics are pushed at runner shutdown.
        config: Scrape endpoint configuration.
    """

    enabled: bool = False
    pushgateway_url: Dsn | None = None
    config: PrometheusConfig | None = None


class ObservabilityConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Top-level observability configuration shared by all Loom runners.

    Compose this struct inside any runner config under an ``observability``
    field to get unified log + OTEL + Prometheus support from YAML.

    Args:
        log: Structured-log backend settings.
        otel: OpenTelemetry traces and logs backend settings.
        prometheus: Prometheus metrics backend settings.

    Example::

        class MyRuntimeConfig(LoomFrozenStruct, frozen=True, kw_only=True):
            observability: ObservabilityConfig = msgspec.field(
                default_factory=ObservabilityConfig
            )
    """

    log: LogObservabilityConfig = msgspec.field(default_factory=LogObservabilityConfig)
    otel: OtelObservabilityConfig = msgspec.field(default_factory=OtelObservabilityConfig)
    prometheus: PrometheusObservabilityConfig = msgspec.field(
        default_factory=PrometheusObservabilityConfig
    )


__all__ = [
    "LogObservabilityConfig",
    "ObservabilityConfig",
    "OtelConfig",
    "OtelObservabilityConfig",
    "PrometheusConfig",
    "PrometheusObservabilityConfig",
]
