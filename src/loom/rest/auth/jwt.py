"""Stateless JWT bearer authentication mechanism.

Verifies the token and projects its verified claims onto an
:class:`~loom.core.identity.identity.Identity`.  Everything downstream — role
resolution, business policies — consumes that identity and never learns a JWT
was involved.

Install the optional dependency with::

    pip install "loom-kernel[jwt]"
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import Any

from loom.core.config.errors import ConfigError
from loom.core.identity import RESERVED_ATTRIBUTE_PREFIX, Identity
from loom.core.identity.issuer import IssuedToken
from loom.rest.auth.abc import RequestCredentials
from loom.rest.auth.config import RESERVED_CLAIMS, JwtAuthConfig, JwtIssuerConfig

_logger = logging.getLogger(__name__)

MECHANISM_NAME = "jwt"
"""Label recorded on every identity this mechanism issues."""

_AUTHORIZATION_HEADER = "authorization"
_BEARER_SCHEME = "bearer"
_SUBJECT_CLAIM = "sub"


ATTRIBUTE_CLAIM_PREFIX = "attr_"
"""Namespace every issued attribute travels under.

The issuer cannot know how a verifier is configured, so an attribute emitted
under its own name could be read as that verifier's roles claim — privilege
escalation from a value the caller controls. Prefixing removes the possibility
instead of trying to detect it. Reading strips the prefix, and unprefixed custom
claims keep working, so tokens minted elsewhere are unaffected.
"""

_JTI_BYTES = 16

_PYJWT_HINT = (
    "JWT authentication requires the optional dependency 'pyjwt'. "
    "Install it with: pip install 'loom-kernel[jwt]'"
)


class JwtAuthenticator:
    """Authenticates callers from a stateless JWT bearer token.

    Verification is fully stateless: no server-side session storage and no
    remote JWKS fetch, and each key verifies only its own algorithms.
    Signature, ``exp`` and ``sub`` are always required (a token without a
    subject carries no identity to bind an authorization decision to, nor to
    audit afterwards); ``aud``/``iss`` are validated only when configured.

    Args:
        config: Validated JWT settings.

    Raises:
        ImportError: If the optional ``pyjwt`` dependency is not installed.

    Example::

        authenticator = JwtAuthenticator(
            JwtAuthConfig(
                public_keys={"2026-08": JwtPublicKey(alg="EdDSA", pem=public_pem)},
                roles_claim="loom_sql_roles",
            )
        )
    """

    def __init__(self, config: JwtAuthConfig) -> None:
        self._config = config
        self._jwt = _load_pyjwt()
        self._roles_claim = config.roles_claim
        self._keys = config._keyring()
        self._accepted_types = frozenset(_media_type(kind) for kind in config.accepted_types)
        self._decode_options: dict[str, Any] = {
            "require": ["exp", _SUBJECT_CLAIM],
            "verify_aud": config.audience is not None,
        }

    @property
    def name(self) -> str:
        """Return the mechanism label recorded on issued identities."""
        return MECHANISM_NAME

    @property
    def provides_roles(self) -> bool:
        """Whether a verified claim binds roles to the caller identity."""
        return self._roles_claim is not None

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        """Verify the bearer token and project its claims onto an identity.

        Args:
            credentials: Headers and path of the request.

        Returns:
            The verified identity, or ``None`` when the header is absent, uses
            another scheme, or the token fails verification.
        """
        token = _bearer_token(credentials.header(_AUTHORIZATION_HEADER))
        if token is None:
            return None
        claims = self._decode(token)
        if claims is None:
            return None
        return self._to_identity(claims)

    def _decode(self, token: str) -> Mapping[str, Any] | None:
        header = self._header(token)
        if header is None:
            return None
        if self._accepted_types and not self._typed(header):
            _logger.debug("JWT verification failed: the typ header is not an accepted type")
            return None
        # The header is unverified here, which is safe: it only picks among the
        # configured keys, and no key is ever fetched from what the token says.
        key_id = header.get("kid")
        key = self._keys.pick(None if key_id is None else str(key_id))
        if key is None:
            _logger.debug("JWT verification failed: no configured key applies to this token")
            return None
        try:
            claims: dict[str, Any] = self._jwt.decode(
                token,
                key.material,
                algorithms=list(key.algorithms),
                audience=self._config.audience,
                issuer=self._config.issuer,
                leeway=self._config.leeway_seconds,
                options=self._decode_options,
            )
        except self._jwt.PyJWTError as exc:
            # DEBUG only, and never the token itself: no oracle in responses/logs.
            _logger.debug("JWT verification failed: %s: %s", type(exc).__name__, exc)
            return None
        return claims

    def _header(self, token: str) -> Mapping[str, Any] | None:
        """The unverified header, read once per token, or ``None`` when unreadable."""
        try:
            header: Mapping[str, Any] = self._jwt.get_unverified_header(token)
        except self._jwt.PyJWTError as exc:
            _logger.debug("JWT header unreadable: %s", type(exc).__name__)
            return None
        return header

    def _typed(self, header: Mapping[str, Any]) -> bool:
        """Whether the ``typ`` header is one of the configured types (RFC 8725 §3.11)."""
        kind = header.get("typ")
        return isinstance(kind, str) and _media_type(kind) in self._accepted_types

    def _to_identity(self, claims: Mapping[str, Any]) -> Identity | None:
        subject = claims.get(_SUBJECT_CLAIM)
        if not isinstance(subject, str) or not subject:
            return None
        if self._roles_claim is not None and self._roles_claim not in claims:
            # Names that disagree between issuer and verifier lose every role
            # without an error anywhere: say it once, at DEBUG, never the token.
            _logger.debug("JWT carries no %r claim: the caller gets no role", self._roles_claim)
        return Identity(
            subject=subject,
            roles=_roles_from_claim(claims.get(self._roles_claim)) if self._roles_claim else (),
            attributes=_attributes(claims, self._roles_claim),
            mechanism=MECHANISM_NAME,
        )


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
        self._jwt = _load_pyjwt()
        self._key = config.signing_key.reveal()
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
            self._jwt.encode({"probe": 0}, self._key, algorithm=self._config.algorithm)
        except Exception:  # noqa: BLE001 - any parse or sign failure means unusable
            raise ConfigError(
                f"The JWT issuer signing key cannot sign with {self._config.algorithm}."
            ) from None

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
                claims, self._key, algorithm=self._config.algorithm, headers=self._headers
            )
        except Exception:  # noqa: BLE001 - the boundary: nothing signing-related escapes
            # The traceback goes to the operator, who needs to know which key and
            # algorithm failed; the raised error carries none of it, so a caller
            # learns only that issuing failed.
            _logger.exception("JWT issuing failed")
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


def _load_pyjwt() -> ModuleType:
    """Import and return :mod:`jwt`, failing fast with an actionable hint.

    The import is local on purpose: ``pyjwt`` is an optional extra, and
    resolving it at authenticator construction turns a missing dependency into
    a startup error instead of a broken API at first request.

    Returns:
        The imported ``jwt`` module.

    Raises:
        ImportError: If ``pyjwt`` is not installed.
    """
    try:
        import jwt
    except ImportError as exc:
        raise ImportError(_PYJWT_HINT) from exc
    return jwt


def _media_type(kind: str) -> str:
    """Normalise a ``typ`` value as RFC 7515 §4.1.9 compares it.

    Media types compare case-insensitively, and ``application/`` may be left
    out only when the value holds no other ``/``.
    """
    lowered = kind.lower()
    rest = lowered.removeprefix("application/")
    return rest if "/" not in rest else lowered


def _bearer_token(header: str | None) -> str | None:
    """Return the ``Bearer`` token carried by an ``Authorization`` header."""
    if header is None:
        return None
    scheme, _, token = header.strip().partition(" ")
    token = token.strip()
    if scheme.lower() != _BEARER_SCHEME or not token:
        return None
    return token


def _roles_from_claim(value: Any) -> tuple[str, ...]:
    """Read the roles claim as ``str`` or ``list[str]``, refusing anything else.

    Values are never coerced and a malformed claim yields no role at all: a
    list holding one valid role and one number is a broken token, not a
    partially authorized caller.
    """
    if isinstance(value, str):
        return (value,) if value else ()
    if not isinstance(value, (list, tuple)) or not value:
        return ()
    if not all(isinstance(item, str) and item for item in value):
        return ()
    return tuple(dict.fromkeys(value))


def _attributes(claims: Mapping[str, Any], roles_claim: str | None) -> dict[str, str]:
    """Project the caller-describing claims onto identity attributes.

    Only string-valued custom claims cross: structured claims would smuggle
    unverifiable shapes into a domain value object, and the registered claims
    describe the token rather than the caller.
    """
    # Registered claims describe the token, not the caller: never identity attributes.
    excluded = RESERVED_CLAIMS | ({roles_claim} if roles_claim else frozenset())
    crossing = {
        name: value
        for name, value in claims.items()
        if name not in excluded and isinstance(value, str)
    }
    return _unreserved(_unprefixed(crossing))


def _unprefixed(crossing: Mapping[str, str]) -> dict[str, str]:
    """Strip the attribute prefix, letting a namespaced claim win over a plain one."""
    # Filter by prefix on both sides: comparing against the stripped names would
    # keep the wire-format key too, and the domain would see the attribute twice.
    namespaced = {
        name.removeprefix(ATTRIBUTE_CLAIM_PREFIX): value
        for name, value in crossing.items()
        if name.startswith(ATTRIBUTE_CLAIM_PREFIX)
    }
    plain = {
        name: value
        for name, value in crossing.items()
        if not name.startswith(ATTRIBUTE_CLAIM_PREFIX)
    }
    return {**plain, **namespaced}


def _unreserved(attributes: Mapping[str, str]) -> dict[str, str]:
    """Drop loom's own attributes (a delegation, for one): a token never sets them."""
    return {
        name: value
        for name, value in attributes.items()
        if not name.startswith(RESERVED_ATTRIBUTE_PREFIX)
    }
