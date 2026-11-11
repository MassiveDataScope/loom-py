"""The generated ``audit_<store>`` interfaces, and the audit models a composition serves.

A store that writes ``schema.audit.reading`` with ``enabled`` and a ``route``
gets one interface, ``audit_<store>``, serving ``GET <prefix>/`` over
:meth:`~loom.core.audit.ReadAuditLog.for_store`. Its model is the store's
``AuditEntry_<store>``, which joins the discovered models so the store is
built through the normal path; :func:`fold_audit_models` adds it for a
product's own ``ReadAuditLog`` use case as well.

Only ``stores.<name>.schema.audit.reading`` is decoded, never the whole store
section, so building these interfaces resolves no secret: ``loom deploy
manifest`` lists them without reaching AWS.
"""

from __future__ import annotations

import types
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any, Final

from loom.core.audit import AuditEntry, ReadAuditLog, audit_entry_for
from loom.core.config import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.persistence.database import AuditReadingConfig, AuditReadingRoute
from loom.core.persistence.stores import (
    SQLALCHEMY_BACKEND,
    StoreCatalog,
    StoreKey,
    StoreSpec,
)
from loom.core.repository.abc import PaginationMode
from loom.rest.model import RestInterface, RestRoute

AUDIT_FILTERS: Final = ("subject", "table", "row_key", "trace_id", "op", "at")
"""The fields a client may filter the generated route on."""

_MAX_LIMIT: Final = 100
_READING: Final = "schema.audit.reading"


@dataclass(frozen=True)
class AuditInterfaces:
    """What :func:`build_audit_interfaces` read from the store catalog.

    Args:
        interfaces: The generated interface of every store whose reading is
            enabled with a ``route``, by its name ``audit_<store>``.
        readings: The ``reading`` section of every store that writes one.
    """

    interfaces: Mapping[str, type[RestInterface[Any]]]
    readings: Mapping[StoreKey, AuditReadingConfig]


def build_audit_interfaces(catalog: StoreCatalog) -> AuditInterfaces:
    """Decode each store's ``schema.audit.reading`` and build its generated interface.

    Args:
        catalog: The composition's store catalog (``source.catalog``).

    Raises:
        ConfigError: When a store that the SQLAlchemy backend does not serve
            writes ``reading``, or a ``reading`` section does not decode.
    """
    readings: dict[StoreKey, AuditReadingConfig] = {}
    for key, spec in catalog.specs.items():
        reading = _reading(spec)
        if reading is not None:
            readings[key] = reading
    interfaces = {
        f"audit_{key}": _interface(key, reading.route)
        for key, reading in readings.items()
        if reading.enabled and reading.route is not None
    }
    return AuditInterfaces(interfaces=interfaces, readings=readings)


def fold_audit_models(
    discovered: DiscoveryResult,
    readings: Mapping[StoreKey, AuditReadingConfig],
    *,
    unreached: Collection[type[Any]] = (),
) -> DiscoveryResult:
    """Keep the ``AuditEntry_<store>`` of every ``ReadAuditLog`` use case served, and no other.

    A use case reads the store of the ``ReadAuditLog.for_store(key)`` class it
    is, or derives from: the class carries the store, a subclass inherits it.
    The audit models discovery inferred are replaced by these, so an audit
    store is built only for a read this transport serves.

    Args:
        discovered: The discovery result, with the served config interfaces folded in.
        readings: The ``reading`` sections :func:`build_audit_interfaces` decoded.
        unreached: Use cases only interfaces this transport does not serve
            route to (another Lambda function's). Those that read the audit
            log are dropped: neither folded, checked nor compiled here.

    Raises:
        ConfigError: When a use case reads a store whose reading is not enabled,
            or subclasses ``ReadAuditLog`` without ``for_store``; no model is
            added then, so no store is built for it.
    """
    reads = [use_case for use_case in discovered.use_cases if issubclass(use_case, ReadAuditLog)]
    served = [use_case for use_case in reads if use_case not in unreached]
    others = [model for model in discovered.models if not issubclass(model, AuditEntry)]
    read_models = dict.fromkeys(_read_model(use_case, readings) for use_case in served)
    dropped = frozenset(reads).difference(served)
    return discovered.with_changes(
        models=(*others, *read_models),
        use_cases=tuple(uc for uc in discovered.use_cases if uc not in dropped),
    )


def _read_model(
    use_case: type[ReadAuditLog], readings: Mapping[StoreKey, AuditReadingConfig]
) -> type[AuditEntry]:
    """The audit model *use_case* reads, when its store reads its audit log."""
    key = _audit_store(use_case)
    reading = readings.get(key)
    if reading is None or not reading.enabled:
        raise ConfigError(f"for_store({key}) requires stores.{key}.{_READING}.enabled")
    return audit_entry_for(key)


def _reading(spec: StoreSpec) -> AuditReadingConfig | None:
    path = f"{spec.path}.{_READING}"
    if spec.backend == SQLALCHEMY_BACKEND:
        return spec.config.section_optional(path, AuditReadingConfig)
    if spec.config.raw(path) is not None:
        raise ConfigError(
            f"{path} is served by the {SQLALCHEMY_BACKEND!r} backend only, but "
            f"{spec.path}.backend is {spec.backend!r}"
        )
    return None


def _interface(key: StoreKey, route: AuditReadingRoute) -> type[RestInterface[Any]]:
    namespace: dict[str, Any] = {
        "prefix": route.prefix or f"/audit/{key}",
        "requires_roles": route.requires_roles,
        "routes": (
            RestRoute(
                use_case=ReadAuditLog.for_store(key),
                method="GET",
                path="/",
                filterable=AUDIT_FILTERS,
                sortable=("at",),
                pagination_mode=PaginationMode.CURSOR,
                allow_pagination_override=True,
                max_limit=_MAX_LIMIT,
            ),
        ),
        "__module__": __name__,
        "__doc__": f"RestInterface generated from stores.{key}.{_READING}.route.",
    }
    model = audit_entry_for(key)
    base = RestInterface[model]  # type: ignore[valid-type]
    return types.new_class(f"audit_{key}", (base,), exec_body=lambda ns: ns.update(namespace))


def _audit_store(use_case: type[ReadAuditLog]) -> StoreKey:
    """The store *use_case* reads, stamped by ``ReadAuditLog.for_store``."""
    key: StoreKey | None = getattr(use_case, "__audit_store__", None)
    if key is None:
        raise ConfigError(
            f"{use_case.__qualname__} subclasses ReadAuditLog without "
            "ReadAuditLog.for_store(<key>): derive it from that class, so composition "
            "checks that the store reads its audit log"
        )
    return key


__all__ = ["AUDIT_FILTERS", "AuditInterfaces", "build_audit_interfaces", "fold_audit_models"]
