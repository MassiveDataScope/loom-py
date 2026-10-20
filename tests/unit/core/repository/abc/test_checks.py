"""``loom.cursors.E001``: a deployment serving repositories with cursors it cannot trust.

An ``unsigned`` cursor can be forged by any client and an ``ephemeral`` one
is refused by every other instance and after a restart; a ``signed`` section
needs a ``current_key_id`` naming one of its ``keys``. The check judges the
keys as written, so it runs in CI without the key material.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.repository.abc import CURSOR_SIGNING_CHECK

_DATABASE = {"database": {"url": "${oc.env:DATABASE_URL}"}}
_MONGO = {"persistence": {"mongo": {"uri": "${oc.env:MONGO_URI}", "database": "store"}}}
_SIGNED = {"current_key_id": "2026-10", "keys": {"2026-10": {"env": "CURSOR_KEY_2026_10"}}}


def _found(raw: dict[str, Any]) -> bool:
    report = run_deploy_checks(ConfigContext.from_dict(raw))
    return any(result.id == CURSOR_SIGNING_CHECK for result in report.errors)


def _with_cursors(backend: dict[str, Any], cursors: dict[str, Any]) -> dict[str, Any]:
    persistence = {**backend.get("persistence", {}), "cursors": cursors}
    return {**backend, "persistence": persistence}


@pytest.mark.parametrize("backend", [_DATABASE, _MONGO], ids=["sqlalchemy", "mongo"])
def test_keys_signing_the_cursors_pass(backend: dict[str, Any]) -> None:
    assert not _found(_with_cursors(backend, _SIGNED))
    assert not _found(_with_cursors(backend, {"signing": "signed", **_SIGNED}))


@pytest.mark.parametrize("backend", [_DATABASE, _MONGO], ids=["sqlalchemy", "mongo"])
@pytest.mark.parametrize(
    "cursors",
    [
        {"signing": "unsigned"},
        {"signing": "ephemeral", **_SIGNED},
        {"signing": "${oc.env:CURSOR_SIGNING}", **_SIGNED},
        {},
        {"current_key_id": "2026-11", "keys": _SIGNED["keys"]},
        {"current_key_id": "${oc.env:CURSOR_KEY_ID}", "keys": _SIGNED["keys"]},
    ],
    ids=["unsigned", "ephemeral", "placeholder-mode", "no-key", "unknown-key", "placeholder-key"],
)
def test_cursors_not_signed_with_a_configured_key_fail(
    backend: dict[str, Any], cursors: dict[str, Any]
) -> None:
    assert _found(_with_cursors(backend, cursors))


def test_a_configuration_serving_no_repositories_is_not_judged() -> None:
    assert not _found({"persistence": {"cursors": {"signing": "unsigned"}}})
