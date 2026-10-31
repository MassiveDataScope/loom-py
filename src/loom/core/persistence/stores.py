"""The store catalog: the ``stores.<name>`` sections and the store each model belongs to.

Each ``stores.<name>`` section declares one store: its ``backend``, one of the
names registered in the ``loom.persistence.backends`` entry-point group, and
the connection that backend reads. A connection-only store
(:data:`CONNECTION_ONLY_BACKENDS`, ``clickhouse``) is no persistence backend:
it serves no model and no repository, never joins a unit of work, and only
hands its connection to the streaming and ETL sinks that name it. A model
declares ``__store__``; without it, it belongs to :data:`DEFAULT_STORE`
(ADR 0008).

Loading the catalog resolves nothing but each ``backend``: it reads the
sections as written, so a deploy check or CI can load it without the
secrets the connections name.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final, NewType, TypeVar

import msgspec

from loom.core.config import ConfigContext, ConfigError, ConfigKey, reject_retired_sections
from loom.core.config.convert import convert_config
from loom.core.model import BaseModel
from loom.core.persistence.registry import registered_backends, unknown_backend_message

T = TypeVar("T")

StoreKey = NewType("StoreKey", str)
"""Name of a store: the ``<name>`` of its ``stores.<name>`` section."""

DEFAULT_STORE: Final = StoreKey("default")
"""The store of every model that declares no ``__store__``."""

_STORE_ATTRIBUTE: Final = "__store__"
_RETIRED_ATTRIBUTE: Final = "__database__"
_BACKEND_KEY: Final = "backend"
_KEY_PATTERN: Final = re.compile(r"[A-Za-z_]\w*", re.ASCII)  # ASCII: \w is [A-Za-z0-9_]
_RETIRED_SECTIONS: Final = ("database", "persistence")

CONNECTION_ONLY_BACKENDS: Final = frozenset({"clickhouse"})
"""Store kinds that only hold a connection: no model, no repository, no unit of work."""


@dataclass(frozen=True, slots=True)
class StoreSpec:
    """One declared store.

    Args:
        key: The store's name.
        backend: Name of the persistence backend serving it, or of a
            connection-only kind (:data:`CONNECTION_ONLY_BACKENDS`).
        section: The ``stores.<key>`` section, ``backend`` included; its
            interpolations still resolve against the whole configuration.
    """

    key: StoreKey
    backend: str
    section: ConfigContext

    @property
    def connection_only(self) -> bool:
        """Whether the store only holds a connection and serves no model."""
        return self.backend in CONNECTION_ONLY_BACKENDS

    @property
    def path(self) -> str:
        """Dotted path of the store's section, for error messages."""
        return f"{ConfigKey.STORES}.{self.key}"

    def settings(self) -> dict[str, Any]:
        """Return the section resolved, without ``backend``: what the backend decodes.

        Raises:
            ConfigError: When an interpolation of the section does not resolve.
        """
        resolved = _resolved(self.section, self.path)
        resolved.pop(_BACKEND_KEY, None)
        return resolved

    def decode(self, target: type[T]) -> T:
        """Decode :meth:`settings` into *target*, as a section of the configuration decodes.

        Raises:
            ConfigError: When the section does not resolve or fails validation,
                naming the store's path.
        """
        try:
            return convert_config(self.settings(), target, strict=False)
        except msgspec.ValidationError as exc:
            raise ConfigError(
                f"Config section {self.path!r} failed validation as {target.__name__!r}: {exc}"
            ) from exc


@dataclass(frozen=True, slots=True)
class StoreCatalog:
    """Every store the configuration declares, by key, in declaration order.

    Args:
        specs: The declared stores; empty when there is no ``stores`` section.
    """

    specs: Mapping[StoreKey, StoreSpec]

    def models_by_store(
        self, models: Sequence[type[BaseModel]]
    ) -> dict[StoreKey, tuple[type[BaseModel], ...]]:
        """Group *models* by the store each belongs to.

        Args:
            models: The discovered models.

        Returns:
            Every declared store, :data:`DEFAULT_STORE` first and the others
            in declaration order, with its models in the order given.

        Raises:
            ConfigError: When a model declares the retired ``__database__``,
                or belongs to a store the catalog does not declare or to a
                connection-only store, naming the model and the store.
        """
        keys = sorted(self.specs, key=lambda key: key != DEFAULT_STORE)
        groups: dict[StoreKey, list[type[BaseModel]]] = {key: [] for key in keys}
        for model in models:
            key = store_of(model)
            if key not in groups:
                raise ConfigError(
                    f"{model.__name__} belongs to store {key!r}, but "
                    f"{ConfigKey.STORES}.{key} is not declared"
                )
            _refuse_connection_only(model, self.specs[key])
            groups[key].append(model)
        return {key: tuple(members) for key, members in groups.items()}


def store_of(model: type[BaseModel]) -> StoreKey:
    """Return the store *model* belongs to: its ``__store__``, else :data:`DEFAULT_STORE`.

    Raises:
        ConfigError: When the model declares the retired ``__database__``, or
            a ``__store__`` that is not a valid store key.
    """
    if hasattr(model, _RETIRED_ATTRIBUTE):
        raise ConfigError(
            f"{model.__name__} declares {_RETIRED_ATTRIBUTE}, which was removed; "
            f"declare {_STORE_ATTRIBUTE} instead (ADR 0008)"
        )
    declared = getattr(model, _STORE_ATTRIBUTE, DEFAULT_STORE)
    if not isinstance(declared, str) or not _KEY_PATTERN.fullmatch(declared):
        raise ConfigError(f"{model.__name__}.{_STORE_ATTRIBUTE} {declared!r} is not a store key")
    return StoreKey(declared)


def require_default_store(models: Sequence[type[BaseModel]], catalog: StoreCatalog) -> None:
    """Fail unless every one of *models* belongs to a :data:`DEFAULT_STORE` that serves models.

    An application composes its default store only; a model of another store
    would otherwise be served by the default store's backend and session.

    Args:
        models: The discovered models.
        catalog: The application's stores.

    Raises:
        ConfigError: As :func:`store_of`, naming the first model of a
            connection-only store, or of a store other than the default one.
    """
    for model in models:
        key = store_of(model)
        spec = catalog.specs.get(key)
        if spec is not None:
            _refuse_connection_only(model, spec)
        if key != DEFAULT_STORE:
            raise ConfigError(
                f"{model.__name__} belongs to store {key!r}, but an application "
                f"composes the {DEFAULT_STORE!r} store only"
            )


def _refuse_connection_only(model: type[BaseModel], spec: StoreSpec) -> None:
    """Fail when *model* belongs to the connection-only store *spec*.

    Raises:
        ConfigError: Naming the model, the store and its kind.
    """
    if spec.connection_only:
        raise ConfigError(
            f"{model.__name__} belongs to store {spec.key!r}, a connection-only "
            f"{spec.backend} store that serves no model; {spec.path} only hands its "
            "connection to the sinks that name it"
        )


def load_catalog(ctx: ConfigContext) -> StoreCatalog:
    """Read the ``stores`` section of *ctx* into a catalog.

    Args:
        ctx: The application configuration.

    Returns:
        The declared stores; an empty catalog without a ``stores`` section.

    Raises:
        ConfigError: When the configuration declares a retired persistence
            key (``database``, ``persistence.backend``, ...), ``stores`` or
            one of its entries is not a mapping, a key is not a store key, or
            a store declares no backend, or one that is neither a registered
            persistence backend nor a connection-only kind.
    """
    reject_retired_sections({section: ctx.raw(section) for section in _RETIRED_SECTIONS})
    written = ctx.raw(ConfigKey.STORES)
    if written is None:
        return StoreCatalog(specs=MappingProxyType({}))
    if not isinstance(written, Mapping):
        raise ConfigError(f"{ConfigKey.STORES} must map each store name to its section")
    specs = {
        StoreKey(key): _spec(ctx, StoreKey(key), section)
        for key, section in ((_store_key(key), section) for key, section in written.items())
    }
    return StoreCatalog(specs=MappingProxyType(specs))


def default_store(ctx: ConfigContext, backend: str) -> StoreSpec:
    """Return the :data:`DEFAULT_STORE` of *ctx*, which *backend* is about to build.

    Args:
        ctx: The application configuration.
        backend: Name of the backend building the store.

    Raises:
        ConfigError: As :func:`load_catalog`, when ``stores.default`` is not
            declared, or when it names another backend.
    """
    spec = load_catalog(ctx).specs.get(DEFAULT_STORE)
    path = f"{ConfigKey.STORES}.{DEFAULT_STORE}"
    if spec is None:
        raise ConfigError(f"the {backend!r} backend needs {path}, which is not declared")
    if spec.backend != backend:
        raise ConfigError(
            f"{path}.backend is {spec.backend!r}, but the {backend!r} backend is building it"
        )
    return spec


def _store_key(key: object) -> str:
    """Return *key* when it is a valid store key.

    Raises:
        ConfigError: Naming the key otherwise.
    """
    if not isinstance(key, str) or not _KEY_PATTERN.fullmatch(key):
        raise ConfigError(
            f"{ConfigKey.STORES} key {key!r} is not a store key: "
            "use letters, digits and underscores, not starting with a digit"
        )
    return key


def _spec(ctx: ConfigContext, key: StoreKey, section: object) -> StoreSpec:
    """Build the spec of store *key* from its *section* as written.

    Raises:
        ConfigError: When the section is not a mapping or its backend is
            missing or not registered.
    """
    path = f"{ConfigKey.STORES}.{key}"
    if not isinstance(section, Mapping):
        raise ConfigError(f"{path} must be a mapping holding the store's backend and connection")
    node = _node(ctx, path)
    backend = node.section(_BACKEND_KEY, str) if _BACKEND_KEY in section else ""
    if not backend:
        raise ConfigError(f"{path}.backend is missing: name the store's persistence backend")
    if backend in CONNECTION_ONLY_BACKENDS:
        return StoreSpec(key=key, backend=backend, section=node)
    registered = registered_backends()
    if backend not in registered:
        connection_only = ", ".join(sorted(CONNECTION_ONLY_BACKENDS))
        raise ConfigError(
            f"{path}.backend: {unknown_backend_message(backend, registered)} "
            f"Connection-only stores: {connection_only}."
        )
    return StoreSpec(key=key, backend=backend, section=node)


def _node(ctx: ConfigContext, path: str) -> ConfigContext:
    """Return the section at *path* as a context still attached to the whole configuration.

    The node keeps its parent, so an interpolation naming another section
    (``${app.name}``) resolves as it does from the root.
    """
    from omegaconf import DictConfig, OmegaConf

    node = OmegaConf.select(ctx._config, path)
    if not isinstance(node, DictConfig):
        raise ConfigError(f"{path} must be a mapping holding the store's backend and connection")
    return ConfigContext(node)


def _resolved(section: ConfigContext, path: str) -> dict[str, Any]:
    """Return *section*, read from *path*, as plain data, interpolations resolved.

    Raises:
        ConfigError: When an interpolation does not resolve, as
            :meth:`ConfigContext.section` reports it.
    """
    from omegaconf import OmegaConf

    try:
        data = OmegaConf.to_container(section._config, resolve=True)
    except Exception as exc:  # omegaconf and resolvers raise their own types
        raise ConfigError(
            f"Failed to resolve config section {path!r} "
            f"(check ${{oc.env:VAR}} interpolations): {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must be a mapping holding the store's backend and connection")
    return {str(key): value for key, value in data.items()}


__all__ = [
    "CONNECTION_ONLY_BACKENDS",
    "DEFAULT_STORE",
    "StoreCatalog",
    "StoreKey",
    "StoreSpec",
    "default_store",
    "load_catalog",
    "require_default_store",
    "store_of",
]
