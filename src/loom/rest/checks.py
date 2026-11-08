"""Deploy checks of the REST layer.

``loom.rest.E001``: the API authenticates no caller. The configuration names
the mechanism: ``app.rest.auth.jwt`` for the built-in one, or
``app.rest.auth.custom: true`` for an ``authenticator`` the composition root
passes in code (``create_app`` refuses either without the other). The
``auth`` of an http function of ``lambda.functions``, under its ``trigger``,
replaces ``app.rest.auth`` for that function, so it must name a mechanism too.

``loom.rest.W001``: the API publishes its documentation or OpenAPI schema,
which maps every route, parameter and field. The three URLs are on by
default; an explicit ``null`` switches each off.

``loom.rest.E002`` and ``loom.rest.E003``: a built-in ``jwt`` section names
no ``audience`` or no ``issuer``. Without them a token the same key signed for
another service is accepted here (RFC 8725 §3.8-3.9; ISO/IEC 27001:2022 A.8.5).
Each ``auth`` that ``E001`` reads is judged.

All read the keys as written and resolve no placeholder, so
``loom check --deploy`` runs them in CI without the application's secrets. A
value given by a placeholder is judged as written: ``jwt`` must be a literal
mapping and ``custom`` the literal ``true``, since a placeholder may resolve
to null; a placeholder URL counts as published.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import REST_CHECKS, Finding, deploy_check
from loom.core.config.keys import ConfigKey

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

AUTHENTICATION_CHECK: Final = f"{REST_CHECKS}.E001"
"""Id of the check refusing a deployed API that authenticates no caller."""

SCHEMA_CHECK: Final = f"{REST_CHECKS}.W001"
"""Id of the check warning that a deployed API publishes its schema."""

AUDIENCE_CHECK: Final = f"{REST_CHECKS}.E002"
"""Id of the check refusing a deployed jwt section that names no audience."""

ISSUER_CHECK: Final = f"{REST_CHECKS}.E003"
"""Id of the check refusing a deployed jwt section that names no issuer."""

_AUTH_KEY = "app.rest.auth"
_SCHEMA_KEYS = ("docs_url", "redoc_url", "openapi_url")


@deploy_check(AUTHENTICATION_CHECK)
def _authentication_configured(config: ConfigContext) -> Finding | None:
    unauthenticated = [key for key in _auth_keys(config) if not _authenticates(config.raw(key))]
    if not unauthenticated:
        return None
    return Finding(
        message=(
            "the API authenticates no caller: neither jwt nor 'custom: true' is set in "
            f"{', '.join(unauthenticated)}."
        ),
        hint=(
            f"configure {_AUTH_KEY}.jwt, or set '{_AUTH_KEY}.custom: true' and pass "
            f"create_app(authenticator=...); list {AUTHENTICATION_CHECK} under "
            "checks.silenced only when a gateway in front authenticates every request."
        ),
    )


def _auth_keys(config: ConfigContext) -> list[str]:
    """``app.rest.auth`` and the trigger ``auth`` of each function of ``lambda.functions``."""
    functions = config.raw(f"{ConfigKey.LAMBDA}.functions", {})
    declared = functions.items() if isinstance(functions, dict) else ()
    overrides = [
        f"{ConfigKey.LAMBDA}.functions.{name}.trigger.auth"
        for name, function in declared
        if isinstance(function, dict)
        and isinstance(function.get("trigger"), dict)
        and function["trigger"].get("auth") is not None
    ]
    return [_AUTH_KEY, *overrides]


def _authenticates(auth: object) -> bool:
    return isinstance(auth, dict) and (
        isinstance(auth.get("jwt"), dict) or auth.get("custom") is True
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


@deploy_check(AUDIENCE_CHECK)
def _jwt_audience_named(config: ConfigContext) -> Finding | None:
    return _jwt_claim_named(config, "audience", "aud")


@deploy_check(ISSUER_CHECK)
def _jwt_issuer_named(config: ConfigContext) -> Finding | None:
    return _jwt_claim_named(config, "issuer", "iss")


def _jwt_claim_named(config: ConfigContext, setting: str, claim: str) -> Finding | None:
    """Refuse each built-in ``jwt`` section that does not name *setting*.

    A placeholder counts as named: the check runs in CI without the secrets.
    """
    unnamed = [
        f"{key}.jwt"
        for key in _auth_keys(config)
        if isinstance(jwt := _jwt_section(config.raw(key)), dict) and not _named(jwt.get(setting))
    ]
    if not unnamed:
        return None
    return Finding(
        message=(
            f"the API accepts a token minted for any {claim}: {', '.join(unnamed)} "
            f"{'names' if len(unnamed) == 1 else 'name'} no '{setting}'."
        ),
        hint=f"set '{setting}' to the value the identity provider writes in the '{claim}' claim.",
    )


def _jwt_section(auth: object) -> object:
    return auth.get("jwt") if isinstance(auth, dict) else None


def _named(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())
