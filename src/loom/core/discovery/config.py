"""The ``app.discovery`` section and the engine each of its modes runs.

Every reader of the section decodes it with these structs: the composition
root embeds :class:`DiscoveryConfig` in its ``app`` keys, while the schema
tools and the Celery worker call :func:`decode_discovery`, which keeps their
own default mode and tolerates keys the section does not declare.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Final

import msgspec

from loom.core.config.loader import convert_section
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.interfaces import InterfacesDiscoveryEngine
from loom.core.discovery.manifest import ManifestDiscoveryEngine
from loom.core.discovery.modules import ModulesDiscoveryEngine

DISCOVERY_PATH: Final = "app.discovery"
"""Dotted path of the section, for error messages."""


class DiscoveryInterfaces(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """``app.discovery.interfaces``: the modules whose REST interfaces are discovered."""

    modules: list[str] = msgspec.field(default_factory=list)
    warn_recommended: bool = True


class DiscoveryModules(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """``app.discovery.modules``: the modules scanned for every component."""

    include: list[str] = msgspec.field(default_factory=list)


class DiscoveryManifest(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """``app.discovery.manifest``: the module listing the components."""

    module: str = ""


class DiscoveryConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """The ``app.discovery`` section."""

    mode: str = "interfaces"
    interfaces: DiscoveryInterfaces = msgspec.field(default_factory=DiscoveryInterfaces)
    modules: DiscoveryModules = msgspec.field(default_factory=DiscoveryModules)
    manifest: DiscoveryManifest = msgspec.field(default_factory=DiscoveryManifest)


_ENGINES: dict[str, Callable[[DiscoveryConfig], DiscoveryResult]] = {
    "interfaces": lambda cfg: InterfacesDiscoveryEngine(
        cfg.interfaces.modules, warn_recommended=cfg.interfaces.warn_recommended
    ).discover(),
    "modules": lambda cfg: ModulesDiscoveryEngine(cfg.modules.include).discover(),
    "manifest": lambda cfg: ManifestDiscoveryEngine(cfg.manifest.module).discover(),
}

DISCOVERY_MODES: Final = frozenset(_ENGINES)
"""The modes :func:`discover` runs an engine for."""


def decode_discovery(
    data: object, *, default_mode: str = "interfaces", strict: bool
) -> DiscoveryConfig:
    """Decode the ``app.discovery`` section.

    Args:
        data: The section, interpolations already resolved.
        default_mode: Mode taken when *data* names none; a Celery worker
            scans ``modules``.
        strict: ``False`` drops the keys the section does not declare instead
            of failing on them, for readers that always tolerated them.

    Raises:
        ConfigError: When the section is not a mapping or a declared key
            fails validation.
    """
    if isinstance(data, Mapping):
        data = {"mode": default_mode, **data}
        if not strict:
            data = _declared_only(data, DiscoveryConfig)
    return convert_section(data, DISCOVERY_PATH, DiscoveryConfig)


def discover(discovery: DiscoveryConfig) -> DiscoveryResult:
    """Run the discovery engine *discovery* names.

    Raises:
        ValueError: When the mode is not one of :data:`DISCOVERY_MODES`.
    """
    engine = _ENGINES.get(discovery.mode)
    if engine is None:
        raise ValueError(f"Unsupported discovery mode: {discovery.mode!r}")
    return engine(discovery)


def _declared_only(data: Mapping[str, Any], struct: type[msgspec.Struct]) -> dict[str, Any]:
    """Return *data* without the keys *struct* does not declare, nested structs included."""
    declared = {field.name: field.type for field in msgspec.structs.fields(struct)}
    kept: dict[str, Any] = {}
    for key, value in data.items():
        if key not in declared:
            continue
        nested = declared[key]
        if (
            isinstance(value, Mapping)
            and isinstance(nested, type)
            and issubclass(nested, msgspec.Struct)
        ):
            value = _declared_only(value, nested)
        kept[key] = value
    return kept
