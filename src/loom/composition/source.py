"""What the composition has resolved before an adapter declares its recipe."""

from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from loom.core.config import ConfigContext
from loom.core.discovery.config import DiscoveryConfig, discover
from loom.core.locator import DEFAULT_CODE_PATH
from loom.core.persistence import resolve_backend
from loom.core.persistence.stores import DEFAULT_STORE, StoreCatalog, load_catalog

if TYPE_CHECKING:
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.environment import Environment
    from loom.core.observability.config import ObservabilityConfig
    from loom.core.observability.runtime import ObservabilityRuntime
    from loom.core.persistence import PersistenceBackend


class AppConfig(msgspec.Struct, kw_only=True):
    """The keys of ``app`` every entry point reads; a transport reads its own."""

    name: str
    entry: str | None = None
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

    def backend(self, default: str) -> PersistenceBackend:
        """Build the backend ``stores.default.backend`` names; *default* without that store.

        Raises:
            ConfigError: As :attr:`catalog`, or when no backend is
                registered under that name.
        """
        spec = self.catalog.specs.get(DEFAULT_STORE)
        return resolve_backend(default if spec is None else spec.backend)
