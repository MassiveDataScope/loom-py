"""Deploy check of the audit log: ``loom.audit.W001``, a store that records nothing.

``stores.<name>.schema.audit.enabled: false`` keeps the history but stops
recording; every table marked ``Audited`` in that store writes no row until
the switch is back on (FR-008). The check reads the switch as written, so it
runs in CI without the models; the compiled models already refuse an excluded
or secret column in a primary key at startup and in ``create_schema``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import Finding, deploy_check
from loom.core.config.keys import ConfigKey

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

AUDIT_DISABLED_CHECK: Final = "loom.audit.W001"
_STORES: Final = str(ConfigKey.STORES)


@deploy_check(AUDIT_DISABLED_CHECK)
def _audit_disabled(config: ConfigContext) -> Finding | None:
    stores = config.raw(_STORES, {})
    if not isinstance(stores, dict):
        return None
    off = sorted(name for name, store in stores.items() if _audit_off(store))
    if not off:
        return None
    paths = ", ".join(f"{_STORES}.{name}.schema.audit.enabled" for name in off)
    return Finding(
        message=f"{paths} is false: tables marked Audited record nothing in that store.",
        hint="remove the key or set it to true; history is kept either way.",
    )


def _audit_off(store: object) -> bool:
    if not isinstance(store, dict):
        return False
    schema = store.get("schema")
    audit = schema.get("audit") if isinstance(schema, dict) else None
    return isinstance(audit, dict) and audit.get("enabled") is False


__all__ = ["AUDIT_DISABLED_CHECK"]
