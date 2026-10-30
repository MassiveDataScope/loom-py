"""What the composition has resolved before an adapter declares its recipe."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from loom.core.config import ConfigContext, ConfigKey
from loom.core.discovery import (
    InterfacesDiscoveryEngine,
    ManifestDiscoveryEngine,
    ModulesDiscoveryEngine,
)
from loom.core.locator import DEFAULT_CODE_PATH
from loom.core.persistence import resolve_backend

if TYPE_CHECKING:
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.environment import Environment
    from loom.core.observability.runtime import ObservabilityRuntime
    from loom.core.persistence import PersistenceBackend


class DiscoveryInterfaces(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    modules: list[str] = msgspec.field(default_factory=list)
    warn_recommended: bool = True


class DiscoveryModules(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    include: list[str] = msgspec.field(default_factory=list)


class DiscoveryManifest(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    module: str = ""


class DiscoveryConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """The ``app.discovery`` section."""

    mode: str = "interfaces"
    interfaces: DiscoveryInterfaces = msgspec.field(default_factory=DiscoveryInterfaces)
    modules: DiscoveryModules = msgspec.field(default_factory=DiscoveryModules)
    manifest: DiscoveryManifest = msgspec.field(default_factory=DiscoveryManifest)


class AppConfig(msgspec.Struct, kw_only=True):
    """The keys of ``app`` every entry point reads; a transport reads its own."""

    name: str
    code_path: str = DEFAULT_CODE_PATH
    discovery: DiscoveryConfig = msgspec.field(default_factory=DiscoveryConfig)


class _PersistenceConfig(msgspec.Struct, kw_only=True):
    backend: str | None = None


_DISCOVERY_ENGINES: dict[str, Callable[[DiscoveryConfig], DiscoveryResult]] = {
    "interfaces": lambda cfg: InterfacesDiscoveryEngine(
        cfg.interfaces.modules,
        warn_recommended=cfg.interfaces.warn_recommended,
    ).discover(),
    "modules": lambda cfg: ModulesDiscoveryEngine(cfg.modules.include).discover(),
    "manifest": lambda cfg: ManifestDiscoveryEngine(cfg.manifest.module).discover(),
}


def discover(discovery: DiscoveryConfig) -> DiscoveryResult:
    """Run the discovery engine *discovery* names.

    Raises:
        ValueError: When the mode is not one of the engines.
    """
    engine = _DISCOVERY_ENGINES.get(discovery.mode)
    if engine is None:
        raise ValueError(f"Unsupported discovery mode: {discovery.mode!r}")
    return engine(discovery)


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
    """

    config: ConfigContext
    environment: Environment
    code_path: Path
    config_root: Path
    app: AppConfig
    observability: ObservabilityRuntime

    def discover(self) -> DiscoveryResult:
        """Discover the use cases, models, interfaces and agents ``app.discovery`` names."""
        return discover(self.app.discovery)

    def backend_name(self, default: str) -> str:
        """Return ``persistence.backend``, or *default* when the key is absent."""
        persistence = self.config.section_or_default(
            ConfigKey.PERSISTENCE, _PersistenceConfig, _PersistenceConfig()
        )
        return persistence.backend or default

    def backend(self, default: str) -> PersistenceBackend:
        """Build the backend :meth:`backend_name` names.

        Raises:
            ConfigError: When no backend is registered under that name.
        """
        return resolve_backend(self.backend_name(default))
