"""Deploy check of pagination cursors: tokens a deployed application cannot trust.

``loom.cursors.E001``: a process that serves repositories (a ``stores.<name>``
section whose backend is ``sqlalchemy`` or ``mongo``) does not sign its
cursors with a configured key. ``signing: unsigned`` lets any client forge a
position, and ``signing: ephemeral`` signs with a key no other instance or
restart shares. A ``signed`` section without a ``current_key_id`` naming one
of its ``keys`` fails at startup anyway; the check reports it in
``loom check --deploy`` too.

The check reads the keys as written and resolves no placeholder, so it runs
in CI without the keys themselves: ``signing`` must be absent or the literal
``signed``, and ``current_key_id`` a literal entry of ``keys``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loom.core.checks import Finding, deploy_check
from loom.core.config.keys import ConfigKey
from loom.core.persistence.stores import BACKEND_KEY, SQLALCHEMY_BACKEND
from loom.core.repository.abc.cursor import CURSOR_SIGNING_CHECK, CursorConfig

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

_SECTION = ConfigKey.PERSISTENCE_CURSORS
_CURSOR_BACKENDS = frozenset({SQLALCHEMY_BACKEND, "mongo"})
"""Backends whose repositories page with cursors."""
_SIGNED = CursorConfig().signing
_HINT = (
    f"drop {_SECTION}.signing from the deployed configuration (signed is the default) "
    f"and set {_SECTION}.current_key_id and {_SECTION}.keys, each key read from a "
    "mounted file (path) or an injected secret variable (env)."
)


@deploy_check(CURSOR_SIGNING_CHECK)
def _cursors_signed(config: ConfigContext) -> Finding | None:
    if not _serves_repositories(config):
        return None
    written = config.raw(_SECTION, {})
    cursors = written if isinstance(written, dict) else {}
    signing = cursors.get("signing", _SIGNED)
    if signing != _SIGNED:
        return Finding(
            message=f"{_SECTION}.signing is {signing!r}: cursor tokens are not signed "
            "with a key every instance shares.",
            hint=_HINT,
        )
    keys = cursors.get("keys")
    current = cursors.get("current_key_id")
    if isinstance(keys, dict) and isinstance(current, str) and current in keys:
        return None
    return Finding(
        message=f"{_SECTION} names no signing key: current_key_id must name an entry of keys.",
        hint=_HINT,
    )


def _serves_repositories(config: ConfigContext) -> bool:
    """Whether a store, as written, names a backend whose repositories page with cursors."""
    stores = config.raw(ConfigKey.STORES)
    if not isinstance(stores, dict):
        return False
    return any(
        isinstance(store, dict) and store.get(BACKEND_KEY) in _CURSOR_BACKENDS
        for store in stores.values()
    )


__all__: list[str] = []
