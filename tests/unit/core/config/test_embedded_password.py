"""``loom.config.W001``: a password written inside a configuration URL."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.config.checks import EMBEDDED_PASSWORD_CHECK

_PASSWORD = "s3cr3t-value"


def _report(raw: dict[str, Any]) -> tuple[list[str], tuple[str, ...]]:
    report = run_deploy_checks(ConfigContext.from_dict(raw))
    found = [result for result in report.results if result.id == EMBEDDED_PASSWORD_CHECK]
    messages = [result.message for result in found]
    return messages, report.silenced


class TestEmbeddedPasswords:
    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("postgresql://db.internal/app", id="a_url_without_a_password_passes"),
            pytest.param("${oc.env:DATABASE_URL}", id="a_placeholder_is_not_judged"),
        ],
    )
    def test_a_url_without_a_judged_password_passes(self, url: str) -> None:
        messages, _silenced = _report(
            {"stores": {"default": {"backend": "sqlalchemy", "url": url}}}
        )

        assert messages == []

    def test_an_embedded_password_names_the_key_and_hides_the_value(self) -> None:
        messages, _silenced = _report(
            {
                "stores": {
                    "default": {
                        "backend": "sqlalchemy",
                        "url": f"postgresql://user:{_PASSWORD}@db.internal/app",
                    }
                }
            }
        )

        assert messages == ["stores.default.url embeds a password in a URL."]
        assert _PASSWORD not in messages[0]

    def test_a_silenced_id_does_not_report(self) -> None:
        raw = {
            "stores": {
                "default": {
                    "backend": "sqlalchemy",
                    "url": f"postgresql://user:{_PASSWORD}@db.internal/app",
                }
            },
            "checks": {"silenced": [EMBEDDED_PASSWORD_CHECK]},
        }

        messages, silenced = _report(raw)

        assert messages == []
        assert EMBEDDED_PASSWORD_CHECK in silenced
