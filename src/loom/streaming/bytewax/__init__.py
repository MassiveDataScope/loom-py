"""Bytewax runtime adapter for Loom streaming flows.

This package requires ``bytewax`` to be installed.

Usage::

    from loom.streaming.bytewax import build_dataflow

    flow = build_dataflow(compiled_plan)
"""

from __future__ import annotations

import sys

try:  # pragma: no cover
    import bytewax as _bytewax
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "The bytewax runtime is missing. The 'streaming' extra "
        "(loom-kernel[streaming]) installs bytewax only on Python < 3.13, because "
        "bytewax publishes no wheel for Python 3.13 or later; this interpreter is "
        f"Python {sys.version_info.major}.{sys.version_info.minor}. Install the "
        'extra on Python < 3.13 (pin requires-python to ">=3.11,<3.13") or '
        "install bytewax yourself."
    ) from exc
else:
    del _bytewax


from loom.streaming.bytewax._adapter import build_dataflow, build_dataflow_with_shutdown
from loom.streaming.bytewax._errors import RuntimeConfigurationError
from loom.streaming.bytewax._runtime_io import KafkaPartitionedSource
from loom.streaming.bytewax._sink_registry import RegisteredSink, RuntimeSinkBinding
from loom.streaming.bytewax.runner import (
    BytewaxRecoverySettings,
    BytewaxRuntimeConfig,
    StreamingRunner,
)
from loom.streaming.core._exceptions import DuplicateErrorSinkError

__all__ = [
    "RuntimeConfigurationError",
    "build_dataflow",
    "build_dataflow_with_shutdown",
    "BytewaxRecoverySettings",
    "BytewaxRuntimeConfig",
    "DuplicateErrorSinkError",
    "KafkaPartitionedSource",
    "RegisteredSink",
    "RuntimeSinkBinding",
    "StreamingRunner",
]
