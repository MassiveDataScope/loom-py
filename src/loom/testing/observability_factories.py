"""Registered factory for the OpenTelemetry section, whose text fields must name a known option.

Importing ``make`` or ``SampleFactory`` from :mod:`loom.testing` registers it, so
``make(OtelConfig, endpoint=None)`` keeps a protocol and a sampler that validate
instead of generated text.
"""

from __future__ import annotations

from loom.core.config.observability import OtelConfig
from loom.testing.data import SampleFactory


class OtelConfigFactory(SampleFactory[OtelConfig]):
    """An exporter section with every key at its default."""

    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True
