"""``loom.config.W001``: a password written inside a configuration URL."""

from __future__ import annotations

from typing import Any

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.config.checks import EMBEDDED_PASSWORD_CHECK

_PASSWORD = "s3cr3t-value"


def _report(raw: dict[str, Any]) -> tuple[list[str], tuple[str, ...]]:
    report = run_deploy_checks(ConfigContext.from_dict(raw))
    found = [result for result in report.results if result.id == EMBEDDED_PASSWORD_CHECK]
    messages = [result.message for result in found]
    return messages, report.silenced


def test_a_url_without_a_password_passes() -> None:
    messages, _silenced = _report({"database": {"url": "postgresql://db.internal/app"}})

    assert messages == []


def test_an_embedded_password_names_the_key_and_hides_the_value() -> None:
    messages, _silenced = _report(
        {"database": {"url": f"postgresql://user:{_PASSWORD}@db.internal/app"}}
    )

    assert messages == ["database.url embeds a password in a URL."]
    assert _PASSWORD not in messages[0]


def test_a_placeholder_is_not_judged() -> None:
    messages, _silenced = _report({"database": {"url": "${oc.env:DATABASE_URL}"}})

    assert messages == []


def test_a_silenced_id_does_not_report() -> None:
    raw = {
        "database": {"url": f"postgresql://user:{_PASSWORD}@db.internal/app"},
        "checks": {"silenced": [EMBEDDED_PASSWORD_CHECK]},
    }

    messages, silenced = _report(raw)

    assert messages == []
    assert EMBEDDED_PASSWORD_CHECK in silenced
