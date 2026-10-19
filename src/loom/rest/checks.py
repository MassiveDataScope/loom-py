"""Deploy checks of the REST layer.

``loom.rest.E001``: the API authenticates no caller. The configuration shows
only the built-in JWT mechanism, so ``create_app`` skips this check when the
composition root supplies an ``authenticator`` in code.

``loom.rest.W001``: the API publishes its documentation or OpenAPI schema,
which maps every route, parameter and field. The three URLs are on by
default; an explicit ``null`` switches each off.

Both read the configuration only: they resolve no placeholder, so
``loom check --deploy`` runs them in CI without the application's secrets.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import Finding, deploy_check

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

REST_CHECKS: Final = "loom.rest"
"""Area of the REST checks; a process serving no HTTP skips it."""

AUTHENTICATION_CHECK: Final = f"{REST_CHECKS}.E001"
"""Id of the check refusing a deployed API that authenticates no caller."""

SCHEMA_CHECK: Final = f"{REST_CHECKS}.W001"
"""Id of the check warning that a deployed API publishes its schema."""

_JWT_KEY = "app.rest.auth.jwt"
_SCHEMA_KEYS = ("app.rest.docs_url", "app.rest.redoc_url", "app.rest.openapi_url")


@deploy_check(AUTHENTICATION_CHECK)
def _authentication_configured(config: ConfigContext) -> Finding | None:
    if config.has(_JWT_KEY):
        return None
    return Finding(
        message=f"the API authenticates no caller: {_JWT_KEY} is not configured.",
        hint=(
            f"configure {_JWT_KEY}, pass create_app(authenticator=...), or list "
            f"{AUTHENTICATION_CHECK} under checks.silenced when a gateway in front "
            "authenticates every request."
        ),
    )


@deploy_check(SCHEMA_CHECK)
def _schema_published(config: ConfigContext) -> Finding | None:
    published = [key for key in _SCHEMA_KEYS if config.has(key) or not config.declares(key)]
    if not published:
        return None
    return Finding(
        message=f"the API publishes its schema through {', '.join(published)}.",
        hint="set each of them to null in the deployed configuration.",
    )
