"""Resolution of store backends from the ``loom.stores.backends`` group."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from loom.core.config import ConfigError
from loom.core.plugins.entrypoints import (
    EntryPointNotFoundError,
    MissingDependency,
    load_entry_point,
    select_entry_point,
)

if TYPE_CHECKING:
    from loom.core.stores.plugin import StoreBackend

BACKEND_ENTRY_POINT_GROUP = "loom.stores.backends"
"""Entry point group store backends register under."""


def resolve_store_backend(name: str, *, path: str | None = None) -> StoreBackend:
    """Resolve the store backend registered under ``name``.

    Args:
        name: ``stores.<key>.backend`` value naming the backend.
        path: Configuration path that names the backend, for the message of a
            missing dependency (``"stores.audit.backend"``).

    Returns:
        An instance of the registered backend class.

    Raises:
        ConfigError: When no backend is registered under ``name`` (the message
            lists the registered names), or when the backend needs a third-party
            module that is not installed, naming the module and the extras
            that provide it.
        ModuleNotFoundError: When a module of loom itself is missing.
        DuplicateEntryPointError: When several distributions register ``name``.
    """
    try:
        target = load_entry_point(BACKEND_ENTRY_POINT_GROUP, name, on_duplicate="error")
    except EntryPointNotFoundError as exc:
        raise ConfigError(_describe_unknown_backend(name, exc.available)) from exc
    except ModuleNotFoundError as exc:
        entry = select_entry_point(BACKEND_ENTRY_POINT_GROUP, name, on_duplicate="error")
        missing = None if entry is None else MissingDependency.of(exc, entry)
        if missing is None:
            raise
        owner = f"{path}: " if path else ""
        raise ConfigError(f"{owner}store backend {name!r} {missing.describe()}") from exc
    backend_cls = cast("type[StoreBackend]", target)
    return backend_cls()


def _describe_unknown_backend(name: str, available: tuple[str, ...]) -> str:
    """Return the message naming an unknown backend *name* and the *available* ones."""
    if not available:
        return f"Unknown store backend {name!r}. No store backends are registered."
    registered = ", ".join(available)
    return f"Unknown store backend {name!r}. Registered backends: {registered}."


__all__ = [
    "BACKEND_ENTRY_POINT_GROUP",
    "resolve_store_backend",
]
