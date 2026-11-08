"""Stateless JWT bearer token issuing.

Mints tokens for a verified :class:`~loom.core.identity.identity.Identity`;
:mod:`loom.rest.auth.jwt` verifies them. Both apply the key rules of
:mod:`loom.rest.auth.keys`.

Install the optional dependency with::

    pip install "loom-kernel[jwt]"
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from loom.core.config.errors import ConfigError
from loom.core.identity import Identity
from loom.core.identity.issuer import IssuedToken
from loom.rest.auth.config import ATTRIBUTE_CLAIM_PREFIX, JwtIssuerConfig
from loom.rest.auth.keys import load_pyjwt

_logger = logging.getLogger(__name__)

_JTI_BYTES = 16


class JwtIssuer:
    """Mints JWT bearer tokens for a verified :class:`Identity`.

    Everything the token says comes from the identity: there is no way for a
    caller to add a claim, so nobody can widen their own roles or speak for
    another subject through this door.

    The signing key stays a ``Secret`` on the config: only this issuer reveals it.

    Every issuing is logged at INFO with the subject and the roles granted. That is
    a deliberate audit trail, not diagnostics: without it an access is not
    attributable to who asked for the token nor to the privileges it carried. The
    subject is usually personal data, so route these logs accordingly. The token and
    the key are never logged.

    Args:
        config: Validated issuer settings.

    Raises:
        ImportError: If the optional ``pyjwt`` dependency is not installed.
        ConfigError: If the signing key cannot sign with the configured algorithm.

    Example::

        issuer = JwtIssuer(JwtIssuerConfig(
            signing_key=Secret(private_pem), algorithm="EdDSA",
            audience="my-api", issuer="my-gateway", roles_claim="loom_sql_roles",
        ))
        issued = issuer.issue(identity)
    """

    def __init__(self, config: JwtIssuerConfig) -> None:
        self._config = config
        self._jwt = load_pyjwt()
        # Kept as the Secret: tools that capture frame locals and ``self`` on an
        # error see ``***``; the material is revealed only for each signature.
        self._key = config.signing_key
        self._max_ttl = timedelta(seconds=config.ttl_seconds)
        headers = {"kid": config.kid, "typ": config.typ}
        self._headers = {name: value for name, value in headers.items() if value} or None
        self._assert_key_signs()

    def _assert_key_signs(self) -> None:
        """Sign once here so a broken key fails startup, not the first login.

        Reading the key is not parsing it: a malformed PEM raises a plain
        ``ValueError`` from ``cryptography``, which is neither a ``PyJWTError``
        nor what the port documents ``ValueError`` to mean, so it would surface
        at the first login disguised as an unrepresentable identity.
        """
        try:
            self._jwt.encode({"probe": 0}, self._key.reveal(), algorithm=self._config.algorithm)
        except Exception:  # noqa: BLE001 - any parse or sign failure means unusable
            pass
        else:
            return
        # Raised outside the handler, so no ``__context__`` keeps the backend error.
        raise ConfigError(f"The JWT issuer signing key cannot sign with {self._config.algorithm}.")

    def issue(self, identity: Identity, *, ttl: timedelta | None = None) -> IssuedToken:
        """Mint a token for *identity*.

        Args:
            identity: Verified caller the token speaks for.
            ttl: Lifetime override, bounded by the configured one.

        Returns:
            The token, its expiry and its ``jti``.

        Raises:
            ValueError: If the identity is anonymous, carries no role while a
                roles claim is configured, holds an attribute that would be
                unreadable once encoded, or *ttl* is out of bounds.
            RuntimeError: If signing fails. The cause is not chained, so the
                caller learns nothing about the key or the algorithm.
        """
        lifetime = self._checked_lifetime(ttl)
        issued_at = datetime.now(tz=UTC)
        expires_at = issued_at + lifetime
        jti = secrets.token_urlsafe(_JTI_BYTES)
        claims = self._claims(identity, issued_at=issued_at, expires_at=expires_at, jti=jti)
        try:
            token = self._jwt.encode(
                claims,
                self._key.reveal(),
                algorithm=self._config.algorithm,
                headers=self._headers,
            )
        except Exception as exc:  # noqa: BLE001 - the boundary: nothing signing-related escapes
            # The operator gets the error type and the algorithm, never the
            # traceback: its frames hold the revealed key, and a handler that
            # captures locals would record it. The caller learns only that
            # issuing failed.
            _logger.error(  # noqa: TRY400 - a traceback would carry the key
                "JWT issuing failed: %s with %s", type(exc).__name__, self._config.algorithm
            )
            raise RuntimeError("JWT issuing failed") from None
        _logger.info(
            "issued token jti=%s sub=%s roles=%s aud=%s kid=%s exp=%s",
            jti,
            identity.subject,
            list(identity.roles),
            self._config.audience,
            self._config.kid,
            int(expires_at.timestamp()),
        )
        return IssuedToken(token=token, expires_at=expires_at, jti=jti)

    def _checked_lifetime(self, ttl: timedelta | None) -> timedelta:
        if ttl is None:
            return self._max_ttl
        if ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
        if ttl > self._max_ttl:
            raise ValueError(
                f"ttl {ttl} exceeds the configured lifetime {self._max_ttl}: "
                "the configured value is the ceiling"
            )
        return ttl

    def _claims(
        self,
        identity: Identity,
        *,
        issued_at: datetime,
        expires_at: datetime,
        jti: str,
    ) -> dict[str, Any]:
        if not identity.subject:
            raise ValueError("cannot issue a token for an identity with no subject")
        roles_claim = self._config.roles_claim
        if not identity.roles:
            # A caller with zero privilege is a role translation that failed, not
            # a legitimate request: a valid token with no role still authenticates.
            raise ValueError(
                f"cannot issue a token with an empty {roles_claim!r}: "
                "an identity with no roles is a translation bug"
            )
        claims: dict[str, Any] = {
            "sub": identity.subject,
            "aud": self._config.audience,
            "iss": self._config.issuer,
            "iat": int(issued_at.timestamp()),
            "exp": int(expires_at.timestamp()),
            "jti": jti,
            roles_claim: list(identity.roles),
        }
        claims.update(_attribute_claims(identity.attributes))
        return claims


def _attribute_claims(attributes: Mapping[str, Any]) -> dict[str, str]:
    """Project identity attributes onto namespaced custom claims.

    Every attribute travels under :data:`ATTRIBUTE_CLAIM_PREFIX`, so none of them
    can occupy a bare claim name a verifier might read as its roles claim.

    Args:
        attributes: Identity attributes. Typed as ``Any`` values because
            :class:`Identity` does not validate them and a non-string would be
            encoded and then silently dropped by the reader.

    Returns:
        The prefixed claims.

    Raises:
        ValueError: If a name already carries the namespace, or a value is not
            a string.
    """
    prefixed: list[str] = []
    unstringly: list[str] = []
    for name, value in attributes.items():
        if name.startswith(ATTRIBUTE_CLAIM_PREFIX):
            prefixed.append(name)
        if not isinstance(value, str):
            unstringly.append(name)
    if prefixed:
        raise ValueError(
            f"identity attributes {sorted(prefixed)} already carry the "
            f"{ATTRIBUTE_CLAIM_PREFIX!r} namespace: the issuer adds it"
        )
    if unstringly:
        raise ValueError(f"identity attributes {sorted(unstringly)} must hold string values")
    return {f"{ATTRIBUTE_CLAIM_PREFIX}{name}": value for name, value in attributes.items()}
