"""What the composition has resolved before an adapter declares its recipe."""

from __future__ import annotations

import functools
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from loom.core.config import (
    ConfigContext,
    ConfigError,
    ConfigKey,
    ConfigResolver,
    with_default_resolvers,
)
from loom.core.config.overrides import load_application_config
from loom.core.discovery.config import DiscoveryConfig, discover
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import resolve_backend
from loom.core.persistence.stores import StoreCatalog, StoreKey, load_catalog

if TYPE_CHECKING:
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.persistence import PersistenceBackend


class AppConfig(msgspec.Struct, kw_only=True):
    """The keys of ``app`` every entry point reads; a transport reads its own."""

    name: str
    entry: str | None = None
    job_service: str | None = None
    code_path: str = DEFAULT_CODE_PATH
    discovery: DiscoveryConfig = msgspec.field(default_factory=DiscoveryConfig)


@dataclass(frozen=True, kw_only=True)
class CompositionSource:
    """The configuration, environment and code an adapter composes from.

    Args:
        config: Loaded configuration, overrides applied.
        environment: Environment resolved once for the whole application.
        code_path: Application code directory, already on ``sys.path``.
        config_root: Directory of the first configuration file.
        app: The ``app`` keys every entry point reads.
        observability: Logging, tracing and metrics, configured before
            anything else can fail.
        observability_config: The ``observability`` section *observability*
            was built from; an adapter reads its own keys there.
    """

    config: ConfigContext
    environment: Environment
    code_path: Path
    config_root: Path
    app: AppConfig
    observability: ObservabilityRuntime
    observability_config: ObservabilityConfig

    @classmethod
    def load(
        cls,
        *config_paths: str,
        config_dir: str | None = None,
        environment: Environment | str | None = None,
        code_path: str | None = None,
        resolvers: Sequence[ConfigResolver] = (),
        caller: str = "load_source",
    ) -> CompositionSource:
        """Load the configuration, environment and code an application composes from.

        Args:
            *config_paths: See :func:`~loom.composition.create_app`.
            config_dir: See :func:`~loom.composition.create_app`.
            environment: See :func:`~loom.composition.create_app`.
            code_path: See :func:`~loom.composition.create_app`.
            resolvers: See :func:`~loom.composition.create_app`.
            caller: Entry point named when the paths are invalid. Loom's own entry
                points pass their name; callers leave the default.

        Returns:
            The loaded source.

        Raises:
            ConfigError: When the configuration is incomplete or invalid.
        """
        resolved_environment = resolve_environment(environment)
        paths = select_config_paths(config_paths, config_dir, resolved_environment, caller=caller)
        ctx = load_application_config(
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
        return cls(
            config=ctx,
            environment=resolved_environment,
            code_path=add_code_path(
                code_path if code_path is not None else app_cfg.code_path, config_root
            ),
            config_root=config_root,
            app=app_cfg,
            observability=observability,
            observability_config=observability_config,
        )

    def discover(self) -> DiscoveryResult:
        """Discover the use cases, models, interfaces and agents ``app.discovery`` names."""
        return discover(self.app.discovery)

    @functools.cached_property
    def catalog(self) -> StoreCatalog:
        """The stores ``stores`` declares, loaded once for the whole composition.

        Raises:
            ConfigError: When the store catalog does not load, a retired
                persistence key or a connection-only ``stores.default`` included.
        """
        return load_catalog(self.config)

    def backend(self, key: StoreKey, *, default: str) -> PersistenceBackend:
        """Build the backend ``stores.<key>.backend`` names; *default* without that store.

        Raises:
            ConfigError: As :attr:`catalog`, or when no backend is
                registered under that name.
        """
        spec = self.catalog.specs.get(key)
        return resolve_backend(default if spec is None else spec.backend)
