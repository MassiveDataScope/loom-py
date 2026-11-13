"""Load a composition source from configuration."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from loom.composition.source import AppConfig, CompositionSource
from loom.core.config import ConfigError, ConfigKey, ConfigResolver, with_default_resolvers
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime


def load_source(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    caller: str = "load_source",
) -> CompositionSource:
    """Load the configuration, environment and code an application composes from.

    Args:
        *config_paths: See :func:`create_app`.
        config_dir: See :func:`create_app`.
        environment: See :func:`create_app`.
        code_path: See :func:`create_app`.
        resolvers: See :func:`create_app`.
        caller: Entry point named when the paths are invalid. Loom's own entry
            points pass their name; callers leave the default.

    Returns:
        The loaded source.

    Raises:
        ConfigError: When the configuration is incomplete or invalid.
    """
    from loom.composition import build

    resolved_environment = resolve_environment(environment)
    paths = select_config_paths(config_paths, config_dir, resolved_environment, caller=caller)
    ctx = build.load_application_config(
        *paths, resolvers=with_default_resolvers(resolvers), environment=resolved_environment
    )
    observability_config = ctx.section_or_default(
        ConfigKey.OBSERVABILITY, ObservabilityConfig, ObservabilityConfig()
    )
    observability = ObservabilityRuntime.from_config(
        observability_config, environment=resolved_environment
    )
    if not ctx.has(f"{ConfigKey.APP}.name"):
        raise ConfigError(
            f"{ConfigKey.APP}.name is required: name the application once in the YAML; "
            "it labels the API, the worker and their metrics"
        )
    app_cfg = ctx.section(ConfigKey.APP, AppConfig)
    config_root = Path(paths[0]).resolve().parent
    return CompositionSource(
        config=ctx,
        environment=resolved_environment,
        code_path=build.add_code_path(
            code_path if code_path is not None else app_cfg.code_path, config_root
        ),
        config_root=config_root,
        app=app_cfg,
        observability=observability,
        observability_config=observability_config,
    )
