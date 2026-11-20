"""Prometheus observability for Loom.

Optional module — requires ``prometheus-client``::

    pip install "loom-py[prometheus]"

Provides:

- :class:`PrometheusMetricsAdapter` — wires into the bootstrap pipeline to
  record use-case level metrics (request counts, durations, errors).
- :class:`PrometheusMiddleware` — pure ASGI middleware that records
  HTTP-level metrics (request counts, durations by path template).

Example — :func:`~loom.composition.create_app` wires both, and serves the
scrape path, when ``observability.prometheus.enabled`` is set::

    # config/app.yaml
    #   observability:
    #     prometheus:
    #       enabled: true
    from loom.composition import create_app

    app = create_app("config/app.yaml")
"""

from loom.prometheus.kafka import KafkaMetricName, KafkaPrometheusMetrics
from loom.prometheus.lifecycle import PrometheusLifecycleAdapter
from loom.prometheus.middleware import PrometheusMiddleware
from loom.prometheus.recorder import PrometheusMetricsAdapter

__all__ = [
    "KafkaMetricName",
    "KafkaPrometheusMetrics",
    "PrometheusLifecycleAdapter",
    "PrometheusMetricsAdapter",
    "PrometheusMiddleware",
]
