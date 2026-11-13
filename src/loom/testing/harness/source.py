"""The in-memory configuration the HTTP test harness composes from: no YAML, no stores."""

from __future__ import annotations

from pathlib import Path

from loom.composition.source import AppConfig, CompositionSource
from loom.core.config import ConfigContext
from loom.core.environment import resolve_environment
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime


def harness_source() -> CompositionSource:
    """The ``test-harness`` application, its jobs inline, in the ``LOOM_ENV`` environment.

    Raises:
        ConfigError: When ``LOOM_ENV`` is not an environment.
    """
    app = AppConfig(name="test-harness", job_service="inline")
    return CompositionSource(
        config=ConfigContext.from_dict({"app": {"name": app.name, "job_service": app.job_service}}),
        environment=resolve_environment(),
        code_path=Path.cwd(),
        config_root=Path.cwd(),
        app=app,
        observability=ObservabilityRuntime.noop(),
        observability_config=ObservabilityConfig(),
    )
