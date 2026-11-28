"""Registered factories for the observability sections that need valid defaults.

Importing ``make`` or ``SampleFactory`` from :mod:`loom.testing` registers them, so
``make(OtelConfig, endpoint=None)`` keeps a protocol and a sampler that validate,
and ``make(ObservabilityConfig, log=log)`` keeps no execution observers, instead
of generated text.
"""

from __future__ import annotations

from loom.core.config.observability import OtelConfig
from loom.core.observability.config import ObservabilityConfig
from loom.testing.data import SampleFactory


class OtelConfigFactory(SampleFactory[OtelConfig]):
    """An exporter section with every key at its default."""

    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True


class ObservabilityConfigFactory(SampleFactory[ObservabilityConfig]):
    """Default logging, OTEL, Prometheus and no execution observers."""

    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True
