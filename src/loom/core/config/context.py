"""Runtime config accessor with typed extraction and binding resolution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.config.binder import StructBinder
from loom.core.config.configurable import ConfigBinding
from loom.core.config.keys import ConfigKey
from loom.core.config.loader import load_config
from loom.core.config.loader import section as _section

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from loom.core.config.resolver import ConfigResolver

T = TypeVar("T")

_ABSENT = object()


class ConfigContext:
    """Runtime config accessor backed by a resolved DictConfig.

    Combines typed section extraction with constructor injection so that
    runner and bootstrap code has a single, consistent entry-point for
    reading config — no scattered ``section()`` calls or bespoke binding
    loops across layers.

    Prefer the factory methods for construction:

    - :meth:`from_yaml` — load from one or more YAML files.
    - :meth:`from_dict` — build from a plain Python mapping (tests, code-defined config).
    - Direct ``ConfigContext(DictConfig)`` — when a DictConfig is already available.

    Args:
        config: Resolved OmegaConf DictConfig.
        binder: Optional :class:`StructBinder`. Defaults to a non-strict binder.
            Pass ``StructBinder(strict=True)`` to disallow implicit coercion.

    Example::

        ctx = ConfigContext.from_yaml("config.yaml")
        db   = ctx.section("database", DatabaseConfig)
        step = ctx.bind(ReadOrdersStep, path="pipeline.steps.orders")
        dep  = ctx.resolve(MyDep.from_config("services.dep", label="prod"))
    """

    __slots__ = ("_binder", "_config")

    def __init__(self, config: DictConfig, *, binder: StructBinder | None = None) -> None:
        self._config = config
        self._binder = binder or StructBinder()

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(
        cls,
        *paths: str,
        resolvers: Sequence[ConfigResolver] = (),
        keyed: Sequence[str] = (),
        binder: StructBinder | None = None,
    ) -> ConfigContext:
        """Build a :class:`ConfigContext` from one or more YAML files.

        Files are merged left-to-right; later files override earlier ones.
        Accepts local paths and cloud URIs (``s3://``, ``gs://``, …). The
        ``LOOM__*`` variables are not applied: an application's configuration
        loads through :func:`loom.core.config.overrides.load_application_config`.

        Args:
            *paths: One or more local paths or cloud URIs.
            resolvers: Optional custom resolvers for ``${name:key}`` placeholders.
            keyed: Dotted paths of the collections merged by key within an
                ``includes`` composition.
            binder: Optional :class:`StructBinder` override.

        Returns:
            A ready-to-use :class:`ConfigContext`.

        Raises:
            ConfigError: If any file cannot be read or parsed, or a keyed
                collection holds a duplicate key or mixes list and mapping forms.
        """
        return cls(load_config(*paths, resolvers=resolvers, keyed=keyed), binder=binder)

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
        *,
        binder: StructBinder | None = None,
    ) -> ConfigContext:
        """Build a :class:`ConfigContext` from a plain Python mapping.

        Primarily used in tests and inline config construction. The mapping
        must resolve to a top-level dictionary (not a list or scalar).

        Args:
            raw: Plain Python mapping of config keys to values.
            binder: Optional :class:`StructBinder` override.

        Returns:
            A ready-to-use :class:`ConfigContext`.

        Raises:
            TypeError: If *raw* does not produce a DictConfig.
        """
        from omegaconf import DictConfig, OmegaConf

        cfg = OmegaConf.create(dict(raw))
        if not isinstance(cfg, DictConfig):
            raise TypeError(f"Config must be a mapping, got {type(cfg).__name__}")
        return cls(cfg, binder=binder)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def has(self, key: str | ConfigKey) -> bool:
        """Return ``True`` if the dot-separated *key* exists in the config.

        Args:
            key: Dot-separated path (e.g. ``"streaming.runtime"``).

        Returns:
            ``True`` when the key resolves to a non-null value.
        """
        from omegaconf import OmegaConf

        return OmegaConf.select(self._config, _key_str(key), default=None) is not None

    def declares(self, key: str | ConfigKey) -> bool:
        """Return ``True`` if the dot-separated *key* is written in the config, even as null.

        Unlike :meth:`has`, an explicit ``null`` counts: it tells a setting
        switched off apart from one left to its default.

        Args:
            key: Dot-separated path (e.g. ``"app.rest.docs_url"``).

        Returns:
            ``True`` when the key is present, whatever its value.
        """
        from omegaconf import OmegaConf

        return OmegaConf.select(self._config, _key_str(key), default=_ABSENT) is not _ABSENT

    def section_optional(self, key: str | ConfigKey, target: type[T]) -> T | None:
        """Extract *key* as *target* when present, otherwise return ``None``.

        Args:
            key: Dot-separated path to the section.
            target: Type to convert the section into.

        Returns:
            Validated instance of *target*, or ``None`` if the key is absent.
        """
        from omegaconf import OmegaConf

        if OmegaConf.select(self._config, _key_str(key), default=None) is None:
            return None
        return _section(self._config, _key_str(key), target)

    def section_or_default(
        self,
        key: str | ConfigKey,
        target: type[T],
        default: T,
    ) -> T:
        """Extract *key* as *target* when present, otherwise return *default*.

        Args:
            key: Dot-separated path to the section.
            target: Type to convert the section into.
            default: Fallback value when the section is absent.

        Returns:
            Validated instance of *target*, or *default* when absent.
        """
        resolved = self.section_optional(key, target)
        return default if resolved is None else resolved

    def section(self, key: str | ConfigKey, target: type[T]) -> T:
        """Extract and validate a typed config section by dot-path.

        A key naming a scalar leaf (``"api.token"``) converts that value.

        Args:
            key: Dot-separated path (e.g. ``"database"``, ``"services.cache"``
                or ``"api.token"``).
            target: Type to convert the section into.

        Returns:
            Validated instance of *target*.

        Raises:
            ConfigError: If the key is absent or the section fails validation.
        """
        return _section(self._config, _key_str(key), target)

    def bind(
        self,
        target: type[T],
        *,
        path: str | ConfigKey = "",
        **overrides: object,
    ) -> T:
        """Instantiate *target* from a config section merged with *overrides*.

        *overrides* take precedence over YAML values. Omit *path* to rely
        entirely on *overrides*.

        Args:
            target: Class to instantiate.
            path: Dot-separated config path. Omit for override-only bindings.
            **overrides: Explicit keyword values applied after YAML values.

        Returns:
            Fully constructed instance of *target*.

        Raises:
            ConfigError: If a required field is missing or type conversion fails.
        """
        raw: dict[str, object] = _section(self._config, _key_str(path), dict) if path else {}
        return self._binder.bind(target, {**raw, **overrides})

    def resolve(self, binding: ConfigBinding) -> object:
        """Materialize a :class:`~loom.core.config.ConfigBinding` into a live object.

        Delegates to :meth:`bind` using the binding's declared path and overrides.
        Compilers and bootstrap code that operate on pre-declared bindings use
        this method rather than calling :meth:`bind` with unpacked fields.

        Args:
            binding: Deferred binding declaration (from
                :meth:`~loom.core.config.Configurable.from_config` or
                :meth:`~loom.core.config.Configurable.configure`).

        Returns:
            Instantiated ``binding.target``.

        Raises:
            ConfigError: If resolution or type conversion fails.
        """
        return self.bind(binding.target, path=binding.config_path, **binding.overrides)


def _key_str(key: str | ConfigKey) -> str:
    """Return the canonical dotted path string for a config key."""
    return str(key)


__all__ = ["ConfigContext"]
