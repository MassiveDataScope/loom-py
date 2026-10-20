"""Shared OpenTelemetry configuration objects used by Loom runtimes."""

from __future__ import annotations

import warnings
from typing import Any
from urllib.parse import urlparse

from loom.core.model import LoomFrozenStruct

_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}

SAMPLERS: frozenset[str] = frozenset(
    {
        "always_on",
        "always_off",
        "traceidratio",
        "parentbased_always_on",
        "parentbased_always_off",
        "parentbased_traceidratio",
    }
)
"""Sampler names accepted by :attr:`OtelConfig.sampler`."""


def _is_local_endpoint(endpoint: str) -> bool:
    return (urlparse(endpoint).hostname or "") in _LOCAL_HOSTS


class OtelConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """OpenTelemetry SDK/exporter configuration.

    Args:
        service_name: Resource attribute ``service.name``.
        tracer_name: Tracer instrumentation name.
        tracer_version: Optional tracer instrumentation version.
        protocol: OTLP protocol (``http/protobuf`` or ``grpc``).
        endpoint: OTLP endpoint URI. When empty, uses global OTel runtime defaults.
        insecure: Exporter transport mode when supported by protocol/exporter.
        headers: Exporter request headers (vendor auth/tags).
        resource_attributes: Additional OTel resource attributes.
        span_attributes: Static span attributes added to all spans emitted by this observer.
        exporter_kwargs: Extra keyword args passed through to OTLP exporter.
        span_processor_kwargs: Extra keyword args passed through to BatchSpanProcessor.
        adopt_host_id_generator: Installs Loom's trace-id generator on the
            already-installed ``TracerProvider`` so streaming messages get
            end-to-end traces. Host spans are unaffected — the generator
            delegates to the default random generator whenever no Loom trace
            id is active. Only meaningful when ``endpoint`` is empty; Loom's
            own provider always gets the generator.
        sampler: Sampler name, one of :data:`SAMPLERS`. Applied to Loom's own
            provider only: with an empty ``endpoint`` the host sampler decides.
        sampler_ratio: Sampled fraction for the ratio-based samplers. Because
            the OTEL trace id *is* the message trace id, the decision is stable
            from ingestion to death and across services, so a sampled message
            yields a complete trace and an unsampled one yields none.
        max_span_links: Upper bound on the links a batch span carries. When the
            bound is hit the span is marked with ``loom.links_truncated``.
    """

    service_name: str = "loom"
    tracer_name: str = "loom"
    tracer_version: str = ""
    protocol: str = "http/protobuf"
    endpoint: str = ""
    insecure: bool = False
    headers: dict[str, str] = {}
    resource_attributes: dict[str, str] = {}
    span_attributes: dict[str, str] = {}
    exporter_kwargs: dict[str, Any] = {}
    span_processor_kwargs: dict[str, Any] = {}
    adopt_host_id_generator: bool = False
    sampler: str = "parentbased_always_on"
    sampler_ratio: float = 1.0
    max_span_links: int = 128

    def validate(self) -> None:
        """Validate the transport protocol, the sampler, and the link bound.

        Raises:
            ValueError: If protocol is unsupported.
            ValueError: If the sampler name is unknown, the sampled ratio is
                outside ``[0.0, 1.0]``, or the link bound is not positive.
        """
        if self.protocol not in {"http/protobuf", "grpc"}:
            raise ValueError(
                "observability.otel_config.protocol must be either 'http/protobuf' or 'grpc'."
            )
        if self.sampler not in SAMPLERS:
            raise ValueError(
                "observability.otel_config.sampler must be one of "
                f"{sorted(SAMPLERS)}, got {self.sampler!r}."
            )
        if not 0.0 <= self.sampler_ratio <= 1.0:
            raise ValueError(
                "observability.otel_config.sampler_ratio must be within [0.0, 1.0], "
                f"got {self.sampler_ratio!r}."
            )
        if self.max_span_links < 1:
            raise ValueError(
                "observability.otel_config.max_span_links must be >= 1, "
                f"got {self.max_span_links!r}."
            )
        if self.insecure and self.endpoint and not _is_local_endpoint(self.endpoint):
            warnings.warn(
                f"OtelConfig.insecure=True on non-local endpoint {self.endpoint!r}."
                " Set insecure=False for production deployments.",
                UserWarning,
                stacklevel=2,
            )


__all__ = ["SAMPLERS", "OtelConfig"]
