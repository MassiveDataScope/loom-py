"""Deploy check of pagination cursors: tokens a deployed application cannot trust.

``loom.cursors.E001``: a process that serves repositories (a ``database``
section for SQLAlchemy, ``persistence.mongo`` for Mongo) does not sign its
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

from typing import TYPE_CHECKING, Final

from loom.core.checks import Finding, deploy_check

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

CURSOR_SIGNING_CHECK: Final = "loom.cursors.E001"
"""Id of the check refusing a deployment whose cursors are not signed with a configured key."""

_SECTION = "persistence.cursors"
_REPOSITORY_SECTIONS = ("database", "persistence.mongo")
_HINT = (
    f"drop {_SECTION}.signing from the deployed configuration (signed is the default) "
    f"and set {_SECTION}.current_key_id and {_SECTION}.keys, each key read from a "
    "mounted file (path) or an injected secret variable (env)."
)


@deploy_check(CURSOR_SIGNING_CHECK)
def _cursors_signed(config: ConfigContext) -> Finding | None:
    if all(config.raw(section) is None for section in _REPOSITORY_SECTIONS):
        return None
    written = config.raw(_SECTION, {})
    cursors = written if isinstance(written, dict) else {}
    signing = cursors.get("signing", "signed")
    if signing != "signed":
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
