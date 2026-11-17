"""The store catalog: the ``stores.<name>`` sections and the store each model belongs to.

Each ``stores.<name>`` section declares one store: its ``backend``, one of the
names registered in the ``loom.stores.backends`` entry-point group, and
the connection that backend reads. A connection-only store
(:data:`CONNECTION_ONLY_BACKENDS`, ``clickhouse``) is no store backend:
it serves no model and no repository, never joins a unit of work, and only
hands its connection to the streaming and ETL sinks that name it. A model
declares ``__store__``; without it, it belongs to :data:`DEFAULT_STORE`.

``stores.settings`` is no store: it holds the settings every store shares, one
member of :class:`StoreSetting` per key. A store takes no ``settings`` of its own.

Loading the catalog resolves nothing but each ``backend``: a deploy check or
CI can load it without the secrets the connections name. A store resolves
the rest of its section, and its capabilities, the first time they are read.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, NewType, TypeVar

from loom.core.config import (
    ConfigContext,
    ConfigError,
    ConfigKey,
    reject_retired_keys,
    reject_retired_sections,
)
from loom.core.config.loader import convert_section
from loom.core.connections import ConnectionSettings
from loom.core.model import BaseModel
from loom.core.plugins.entrypoints import registered_names
from loom.core.stores.registry import (
    BACKEND_ENTRY_POINT_GROUP,
    resolve_store_backend,
)

if TYPE_CHECKING:
    from loom.core.stores.backend import StoreBackend

T = TypeVar("T")

StoreKey = NewType("StoreKey", str)
"""Name of a store: the ``<name>`` of its ``stores.<name>`` section."""

DEFAULT_STORE: Final = StoreKey("default")
"""The store of every model that declares no ``__store__``."""

DEFAULT_STORE_PATH: Final = f"{ConfigKey.STORES}.{DEFAULT_STORE}"
"""Dotted path of the default store's section, for messages about a store none names."""

BACKEND_KEY: Final = "backend"
"""The key of a store section naming its backend."""

SETTINGS_KEY: Final = "settings"
"""The reserved store name holding the shared settings, and the key of a store's own settings."""

SQLALCHEMY_BACKEND: Final = "sqlalchemy"
"""The backend whose stores hold a relational database section."""

CLICKHOUSE_BACKEND: Final = "clickhouse"
"""The connection-only kind of a ClickHouse store."""

NONE_BACKEND: Final = "none"
"""The backend that persists nothing."""

CONNECTION_ONLY_BACKENDS: Final = frozenset({CLICKHOUSE_BACKEND})
"""Store kinds that only hold a connection: no model, no repository, no unit of work."""

_STORE_ATTRIBUTE: Final = "__store__"
_RETIRED_ATTRIBUTE: Final = "__database__"
_KEY_PATTERN: Final = re.compile(r"[A-Za-z_]\w*", re.ASCII)


class StoreSetting(StrEnum):
    """A key of ``stores.settings``: a setting every store shares.

    Each one is global: no store overrides it.
    """

    CURSORS = "cursors"
    ROLLBACK_TIMEOUT = "rollback_timeout"

    @property
    def path(self) -> str:
        """Dotted path of the key in the configuration."""
        return f"{ConfigKey.STORES_SETTINGS}.{self.value}"

    @classmethod
    def check_section(cls, shared: object) -> None:
        """Fail unless ``stores.settings`` is a mapping of store settings.

        Args:
            shared: The ``stores.settings`` value as written.

        Raises:
            ConfigError: When it is no mapping, or holds a key that is no store
                setting, naming the key; a ``backend`` key says the section is no store.
        """
        if not isinstance(shared, Mapping):
            raise ConfigError(
                f"{ConfigKey.STORES_SETTINGS} must map each store setting to its value"
            )
        known = {setting.value for setting in cls}
        for name in shared:
            if name == BACKEND_KEY:
                raise ConfigError(
                    f"{ConfigKey.STORES_SETTINGS} is reserved for the shared settings and "
                    f"declares no store: remove {BACKEND_KEY!r}, or name the store otherwise"
                )
            if name not in known:
                raise ConfigError(
                    f"{ConfigKey.STORES_SETTINGS}.{name} is not a store setting; "
                    f"the keys are {', '.join(sorted(known))}"
                )


@dataclass(frozen=True, slots=True, kw_only=True)
class StoreCapabilities:
    """Capabilities of a configured store."""

    can_serve_models: bool
    """Whether the store can serve model repositories."""
    is_transactional: bool
    """Whether its unit of work can roll back."""


MODELS_ONLY: Final = StoreCapabilities(can_serve_models=True, is_transactional=False)
"""A store that serves models, without rollback; the conservative default
of a backend that does not describe itself."""

NO_CAPABILITIES: Final = StoreCapabilities(can_serve_models=False, is_transactional=False)
"""A store that supports none of them: ``none``, or a connection-only kind."""


@dataclass(frozen=True)
class StoreSpec:
    """One declared store.

    Args:
        key: The store's name.
        backend: Name of the store backend serving it, or of a
            connection-only kind (:data:`CONNECTION_ONLY_BACKENDS`).
        config: The whole configuration, against which the section's
            interpolations resolve.
    """

    key: StoreKey
    backend: str
    config: ConfigContext

    @property
    def backend_path(self) -> str:
        """Dotted path of the key naming the store's backend, for error messages."""
        return f"{self.path}.{BACKEND_KEY}"

    def resolve_backend(self) -> StoreBackend:
        """Build the backend the store names.

        Raises:
            ConfigError: When no backend is registered under that name, or it
                needs a module that is not installed.
        """
        return resolve_store_backend(self.backend, path=self.backend_path)

    @functools.cached_property
    def capabilities(self) -> StoreCapabilities:
        """What the store supports, asked of its backend on first read and kept.

        A connection-only store has fixed capabilities; every other store asks
        its backend's required ``capabilities`` method.

        Raises:
            ConfigError: When the backend does not implement ``capabilities``.
        """
        if self.backend in CONNECTION_ONLY_BACKENDS:
            return NO_CAPABILITIES
        backend = self.resolve_backend()
        if not callable(getattr(backend, "capabilities", None)):
            raise ConfigError(
                f"store backend {self.backend!r} ({type(backend).__qualname__}) serving "
                f"{self.path} does not implement the required method 'capabilities'"
            )
        return backend.capabilities(self)

    @property
    def path(self) -> str:
        """Dotted path of the store's section, for error messages."""
        return _path(self.key)

    @functools.cached_property
    def _resolved(self) -> dict[str, Any]:
        """The section without ``backend``, resolved on first read and kept."""
        resolved = self.config.section(self.path, dict)
        resolved.pop(BACKEND_KEY, None)
        return resolved

    def section(self) -> dict[str, Any]:
        """Return a copy of the resolved section without ``backend``.

        It is what the backend decodes.

        Raises:
            ConfigError: When an interpolation of the section does not resolve.
        """
        return dict(self._resolved)

    def decode(self, target: type[T]) -> T:
        """Decode :meth:`section` into *target*, as a section of the configuration decodes.

        A connection type's retired keys fail first, naming their replacement.

        Raises:
            ConfigError: When the section holds a retired key, does not
                resolve or fails validation, naming the store's path.
        """
        if isinstance(target, type) and issubclass(target, ConnectionSettings):
            reject_retired_keys(self._resolved, self.path, target)
        return convert_section(self._resolved, self.path, target)


@dataclass(frozen=True, slots=True)
class StoreCatalog:
    """Every store the configuration declares, by key, in declaration order.

    Args:
        specs: The declared stores; empty when there is no ``stores`` section.
    """

    specs: Mapping[StoreKey, StoreSpec]

    def require(self, key: str, *, backend: str | None = None, purpose: str) -> StoreSpec:
        """Return the store *key*, which *purpose* needs, served by *backend* when given.

        Args:
            key: The store's name.
            backend: The backend the store must declare; ``None`` takes any.
            purpose: Who needs the store, for the message (``"the 'mongo' backend"``).

        Raises:
            ConfigError: When the store is not declared, or declares another backend.
        """
        spec = self.specs.get(StoreKey(key))
        if spec is None:
            raise ConfigError(f"{purpose} needs {_path(key)}, which is not declared")
        if backend is not None and spec.backend != backend:
            raise ConfigError(
                f"{spec.path}.{BACKEND_KEY} is {spec.backend!r}, but {purpose} needs "
                f"{BACKEND_KEY}: {backend}"
            )
        return spec

    def require_installed(self) -> None:
        """Fail unless the backend of every declared store can be built.

        A connection-only store has no backend to build and is left out.

        Raises:
            ConfigError: Naming the first store whose backend is not
                installed, as :meth:`StoreSpec.resolve_backend`.
        """
        for spec in self.specs.values():
            if spec.backend not in CONNECTION_ONLY_BACKENDS:
                spec.resolve_backend()

    def served_by(self, backend: str, key: StoreKey) -> StoreSpec:
        """Return the store *key* that *backend* serves.

        Args:
            backend: The backend the store must declare.
            key: The store's name; a backend takes it from ``runtime.store``.

        Raises:
            ConfigError: As :meth:`require`.
        """
        return self.require(key, backend=backend, purpose=f"the {backend!r} backend")

    def models_by_store(
        self, models: Sequence[type[BaseModel]]
    ) -> dict[StoreKey, tuple[type[BaseModel], ...]]:
        """Group *models* by the store each belongs to.

        Args:
            models: The discovered models.

        Returns:
            :data:`DEFAULT_STORE`, declared or not, then every other store
            that holds a model, in declaration order, each with its models in
            the order given.

        Raises:
            ConfigError: When a model declares the retired ``__database__``,
                or belongs to a store the catalog does not declare or to a
                connection-only store, naming the model and the store.
        """
        groups: dict[StoreKey, list[type[BaseModel]]] = {DEFAULT_STORE: []}
        for model in models:
            key = store_of(model)
            if key != DEFAULT_STORE:
                spec = self.require(key, purpose=f"model {model.__name__}")
                if not spec.capabilities.can_serve_models:
                    raise ConfigError(
                        f"{model.__name__} belongs to store {key!r}, a connection-only "
                        f"{spec.backend} store that serves no model; {spec.path} only hands "
                        "its connection to the sinks that name it"
                    )
            groups.setdefault(key, []).append(model)
        return {key: tuple(groups[key]) for key in (DEFAULT_STORE, *self.specs) if key in groups}


def store_of(model: type[Any]) -> StoreKey:
    """Return the store *model* belongs to: its ``__store__``, else :data:`DEFAULT_STORE`.

    A use case or job without a main model declares its primary store the same way.

    Raises:
        ConfigError: When the model declares the retired ``__database__``, or
            a ``__store__`` that is not a valid store key.
    """
    if hasattr(model, _RETIRED_ATTRIBUTE):
        raise ConfigError(
            f"{model.__name__} declares {_RETIRED_ATTRIBUTE}, which was removed; "
            f"declare {_STORE_ATTRIBUTE} instead"
        )
    declared = getattr(model, _STORE_ATTRIBUTE, DEFAULT_STORE)
    return _store_key(declared, owner=f"{model.__name__}.{_STORE_ATTRIBUTE}")


def store_binding(binding: type[Any], key: StoreKey) -> object:
    """The DI key of store *key*'s *binding*: *binding* itself for the default store.

    The default store's binding stays injectable by type; another store's is
    keyed by ``(binding, key)``.
    """
    return binding if key == DEFAULT_STORE else (binding, key)


def load_catalog(ctx: ConfigContext) -> StoreCatalog:
    """Read the ``stores`` section of *ctx* into a catalog.

    A composition loads it once and shares it: the backends, the sinks and
    the container read that one catalog.

    Args:
        ctx: The application configuration.

    Returns:
        The declared stores; an empty catalog without a ``stores`` section.

    Raises:
        ConfigError: When the configuration declares a retired persistence
            key (``database``, ``persistence``, ...), ``stores`` or
            one of its entries is not a mapping, a key is not a store key, a store declares no
            backend, or one that is neither a registered store backend
            nor a connection-only kind, :data:`DEFAULT_STORE` is
            connection-only, ``stores.settings`` holds a key that is no store
            setting, or a store declares its own ``settings``.
    """
    reject_retired_sections(ctx)
    written = ctx.raw(ConfigKey.STORES)
    if written is None:
        return StoreCatalog(specs=MappingProxyType({}))
    if not isinstance(written, Mapping):
        raise ConfigError(f"{ConfigKey.STORES} must map each store name to its section")
    registered = registered_names(BACKEND_ENTRY_POINT_GROUP)
    specs: dict[StoreKey, StoreSpec] = {}
    for name, section in written.items():
        if name == SETTINGS_KEY:
            StoreSetting.check_section(section)
            continue
        key = _store_key(name, owner=f"{ConfigKey.STORES} key")
        if not isinstance(section, Mapping):
            raise ConfigError(
                f"{_path(key)} must be a mapping holding the store's backend and connection"
            )
        specs[key] = _spec(ctx, key, registered)
    return StoreCatalog(specs=MappingProxyType(specs))


def _path(key: str) -> str:
    """Dotted path of the section of store *key*."""
    return f"{ConfigKey.STORES}.{key}"


def _store_key(value: object, *, owner: str) -> StoreKey:
    """Return *value*, which *owner* declares, when it is a valid store key.

    Raises:
        ConfigError: Naming the owner and the value otherwise.
    """
    if not isinstance(value, str) or not _KEY_PATTERN.fullmatch(value):
        raise ConfigError(
            f"{owner} {value!r} is not a store key: "
            "use letters, digits and underscores, not starting with a digit"
        )
    if value == SETTINGS_KEY:
        raise ConfigError(
            f"{owner} {value!r} is reserved: {ConfigKey.STORES_SETTINGS} holds the shared "
            "settings, so no store can take that name"
        )
    return StoreKey(value)


def _spec(ctx: ConfigContext, key: StoreKey, registered: tuple[str, ...]) -> StoreSpec:
    """Build the spec of store *key*, whose backend is one of *registered* or connection-only.

    Raises:
        ConfigError: When the backend is missing or not registered, or the
            default store is connection-only.
    """
    path = _path(key)
    if ctx.has(f"{path}.{SETTINGS_KEY}"):
        raise ConfigError(
            f"{path}.{SETTINGS_KEY} is not supported: a store takes no settings of its own, "
            f"the shared ones live under {ConfigKey.STORES_SETTINGS}"
        )
    backend = ctx.section_or_default(f"{path}.{BACKEND_KEY}", str, "")
    if not backend:
        raise ConfigError(f"{path}.{BACKEND_KEY} is missing: name the store's store backend")
    connection_only = backend in CONNECTION_ONLY_BACKENDS
    if not connection_only and backend not in registered:
        kinds = ", ".join(sorted(CONNECTION_ONLY_BACKENDS))
        raise ConfigError(
            f"{path}.{BACKEND_KEY}: Unknown store backend {backend!r}. "
            f"Registered backends: {', '.join(registered)}. "
            f"Connection-only stores: {kinds}."
        )
    if connection_only and key == DEFAULT_STORE:
        raise ConfigError(
            f"{path}.{BACKEND_KEY} is {backend!r}, a connection-only store that "
            "serves no model; declare the application's store backend there"
        )
    return StoreSpec(key=key, backend=backend, config=ctx)


__all__ = [
    "BACKEND_KEY",
    "CLICKHOUSE_BACKEND",
    "CONNECTION_ONLY_BACKENDS",
    "DEFAULT_STORE",
    "DEFAULT_STORE_PATH",
    "MODELS_ONLY",
    "NO_CAPABILITIES",
    "NONE_BACKEND",
    "SETTINGS_KEY",
    "SQLALCHEMY_BACKEND",
    "StoreCatalog",
    "StoreCapabilities",
    "StoreKey",
    "StoreSetting",
    "StoreSpec",
    "load_catalog",
    "store_binding",
    "store_of",
]
