"""Resolution of persistence backends from the ``loom.persistence.backends`` group."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from loom.core.config import ConfigError
from loom.core.plugins.entrypoints import (
    EntryPointNotFoundError,
    load_entry_point,
)

# abc imports profile, which imports the store catalog, which imports this module
if TYPE_CHECKING:
    from loom.core.persistence.abc import PersistenceBackend

BACKEND_ENTRY_POINT_GROUP = "loom.persistence.backends"
"""Entry point group persistence backends register under."""


def resolve_backend(name: str) -> PersistenceBackend:
    """Resolve the persistence backend registered under ``name``.

    Args:
        name: ``stores.<name>.backend`` value naming the backend.

    Returns:
        An instance of the registered backend class.

    Raises:
        ConfigError: When no backend is registered under ``name`` (the message
            lists the registered names), or when the backend module cannot be
            imported because its optional extra is not installed.
        DuplicateEntryPointError: When several distributions register ``name``.
    """
    try:
        target = load_entry_point(BACKEND_ENTRY_POINT_GROUP, name, on_duplicate="error")
    except EntryPointNotFoundError as exc:
        raise ConfigError(unknown_backend_message(name, exc.available)) from exc
    except ImportError as exc:
        raise ConfigError(
            f"Persistence backend {name!r} could not be imported ({exc}). Install the "
            f"distribution providing it (built-in backends: `pip install 'loom-kernel[{name}]'`)."
        ) from exc
    backend_cls = cast("type[PersistenceBackend]", target)
    return backend_cls()


def unknown_backend_message(name: str, available: tuple[str, ...]) -> str:
    """Return the message naming an unknown backend *name* and the *available* ones."""
    if not available:
        return f"Unknown persistence backend {name!r}. No persistence backends are registered."
    registered = ", ".join(available)
    return f"Unknown persistence backend {name!r}. Registered backends: {registered}."


__all__ = [
    "BACKEND_ENTRY_POINT_GROUP",
    "resolve_backend",
    "unknown_backend_message",
]
