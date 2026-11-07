"""``loom.audit.W001``: a store whose audit log is switched off records nothing (FR-008)."""

from __future__ import annotations

from loom.core.checks import DEPLOY_CHECKS
from loom.core.config import ConfigContext
from loom.core.persistence.audit_checks import AUDIT_DISABLED_CHECK


def _run(stores: dict[str, object]) -> object:
    check = dict(DEPLOY_CHECKS.items())[AUDIT_DISABLED_CHECK]
    return check(ConfigContext.from_dict({"stores": stores}))


def test_audit_switched_off_is_reported_with_the_store() -> None:
    finding = _run({"main": {"backend": "sqlalchemy", "schema": {"audit": {"enabled": False}}}})

    assert finding is not None
    assert "stores.main.schema.audit.enabled" in finding.message  # type: ignore[attr-defined]


def test_audit_left_on_or_unset_is_quiet() -> None:
    assert _run({"main": {"backend": "sqlalchemy", "schema": {"audit": {"enabled": True}}}}) is None
    assert _run({"main": {"backend": "sqlalchemy"}}) is None
    assert _run({}) is None
