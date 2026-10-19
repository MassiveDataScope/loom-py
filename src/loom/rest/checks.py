"""Deploy checks of the REST layer.

``loom.rest.E001``: the API authenticates no caller. The configuration names
the mechanism: ``app.rest.auth.jwt`` for the built-in one, or
``app.rest.auth.custom: true`` for an ``authenticator`` the composition root
passes in code (``create_app`` refuses either without the other).

``loom.rest.W001``: the API publishes its documentation or OpenAPI schema,
which maps every route, parameter and field. The three URLs are on by
default; an explicit ``null`` switches each off.

Both read the keys as written and resolve no placeholder, so
``loom check --deploy`` runs them in CI without the application's secrets. A
value given by a placeholder is judged as written: ``jwt`` must be a literal
mapping and ``custom`` the literal ``true``, since a placeholder may resolve
to null; a placeholder URL counts as published.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import REST_CHECKS, Finding, deploy_check

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

AUTHENTICATION_CHECK: Final = f"{REST_CHECKS}.E001"
"""Id of the check refusing a deployed API that authenticates no caller."""

SCHEMA_CHECK: Final = f"{REST_CHECKS}.W001"
"""Id of the check warning that a deployed API publishes its schema."""

_JWT_KEY = "app.rest.auth.jwt"
_CUSTOM_KEY = "app.rest.auth.custom"
_SCHEMA_KEYS = ("docs_url", "redoc_url", "openapi_url")


@deploy_check(AUTHENTICATION_CHECK)
def _authentication_configured(config: ConfigContext) -> Finding | None:
    auth = config.raw("app.rest.auth", {})
    if isinstance(auth, dict) and (isinstance(auth.get("jwt"), dict) or auth.get("custom") is True):
        return None
    return Finding(
        message=f"the API authenticates no caller: neither {_JWT_KEY} nor {_CUSTOM_KEY} is set.",
        hint=(
            f"configure {_JWT_KEY}, or set '{_CUSTOM_KEY}: true' and pass "
            f"create_app(authenticator=...); list {AUTHENTICATION_CHECK} under "
            "checks.silenced only when a gateway in front authenticates every request."
        ),
    )


@deploy_check(SCHEMA_CHECK)
def _schema_published(config: ConfigContext) -> Finding | None:
    rest = config.raw("app.rest", {})
    written = rest if isinstance(rest, dict) else {}
    published = [f"app.rest.{key}" for key in _SCHEMA_KEYS if written.get(key, key) is not None]
    if not published:
        return None
    return Finding(
        message=f"the API publishes its schema through {', '.join(published)}.",
        hint="set each of them to null in the deployed configuration.",
    )
