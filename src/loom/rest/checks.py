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

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

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
    unauthenticated = [auth.key for auth in _auth_sections(config) if not auth.authenticates]
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


@dataclass(frozen=True, slots=True)
class _AuthSection:
    """One ``auth`` mapping as written, read once so each check asks only its question."""

    key: str
    jwt: Mapping[str, Any] | None
    custom: bool

    @classmethod
    def read(cls, key: str, written: object) -> _AuthSection:
        auth = written if isinstance(written, dict) else {}
        jwt = auth.get("jwt")
        return cls(key, jwt if isinstance(jwt, dict) else None, auth.get("custom") is True)

    @property
    def authenticates(self) -> bool:
        return self.jwt is not None or self.custom

    def names(self, setting: str) -> bool:
        """Whether the built-in ``jwt`` section names *setting*; true without one."""
        if self.jwt is None:
            return True
        value = self.jwt.get(setting)
        return isinstance(value, str) and bool(value.strip())


def _auth_sections(config: ConfigContext) -> list[_AuthSection]:
    """``app.rest.auth`` and the trigger ``auth`` of each function of ``lambda.functions``."""
    keys = [_AUTH_KEY, *_trigger_auth_keys(config.raw(f"{ConfigKey.LAMBDA}.functions", {}))]
    return [_AuthSection.read(key, config.raw(key)) for key in keys]


def _trigger_auth_keys(functions: object) -> list[str]:
    """The ``trigger.auth`` key of each function that declares one."""
    declared = functions.items() if isinstance(functions, dict) else ()
    return [
        f"{ConfigKey.LAMBDA}.functions.{name}.trigger.auth"
        for name, function in declared
        if _trigger_of(function).get("auth") is not None
    ]


def _trigger_of(function: object) -> dict[str, Any]:
    trigger = function.get("trigger") if isinstance(function, dict) else None
    return trigger if isinstance(trigger, dict) else {}


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
    unnamed = [f"{auth.key}.jwt" for auth in _auth_sections(config) if not auth.names(setting)]
    if not unnamed:
        return None
    return Finding(
        message=(
            f"the API accepts a token minted for any {claim}: {', '.join(unnamed)} "
            f"{'names' if len(unnamed) == 1 else 'name'} no '{setting}'."
        ),
        hint=f"set '{setting}' to the value the identity provider writes in the '{claim}' claim.",
    )
