"""Configuration for the native stateless JWT authentication middleware.

Where a key comes from is the configurator's business: a placeholder such as
``${file:...}``, ``${secrets:...}`` or ``${ssm:...}`` resolves it, and a secret
lands in a :class:`~loom.core.model.redaction.Secret` that no repr, log or dump
shows. These settings only judge the key they are given: its algorithm, its
family and its strength.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Final

from loom.core.config.errors import ConfigError
from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import Secret
from loom.rest.auth.keys import (
    SUPPORTED_ALGORITHMS,
    FixedKey,
    JwtAlgorithm,
    JwtPublicKey,
    Key,
    Keyring,
    KeySet,
    derived_public_key,
    is_symmetric,
    read_key_file,
    require_family,
    require_hmac_strength,
    require_public_key,
    require_signing_key,
    require_supported,
)

DEFAULT_EXCLUDE_PATHS: tuple[str, ...] = (
    "/docs",
    "/redoc",
    "/openapi.json",
    "/metrics",
    "/health",
)
"""Paths served without authentication unless the application says otherwise."""

MAX_LEEWAY_SECONDS: Final = 300
"""Largest clock-skew tolerance a verifier accepts: RFC 7519 §4.1.4 asks for a
small leeway, and a larger one quietly extends every token's lifetime."""

MAX_ISSUER_TTL_SECONDS = 3600
"""Ceiling for a minted token: signing is reading, so a long-lived token is a
standing grant that stateless verification cannot revoke."""

# RFC 7519 §4.1 registered claims. They describe the token, not the caller, so
# they never become identity attributes, and a roles claim named after one is
# silently overwritten when the token is built.
RESERVED_CLAIMS: frozenset[str] = frozenset({"iss", "sub", "aud", "exp", "nbf", "iat", "jti"})


def _require_not_blank(owner: str, **values: str | None) -> None:
    """Refuse an optional setting that is set but blank, naming the first one."""
    for name, value in values.items():
        if value is not None and not value.strip():
            raise ConfigError(f"{owner} {name!r} must not be blank when set.")


def _require_usable_roles_claim(roles_claim: str) -> None:
    """Reject a roles claim that is blank or shadows a registered claim."""
    if not roles_claim or not roles_claim.strip():
        raise ConfigError("JWT issuer requires a non-blank 'roles_claim'.")
    if roles_claim in RESERVED_CLAIMS:
        raise ConfigError(
            f"JWT issuer 'roles_claim' {roles_claim!r} is a registered claim: "
            "the token would carry no roles and raise nothing."
        )


def _one_algorithm(algorithms: tuple[str, ...], *, case: str) -> str:
    """The single HS algorithm a shared secret is used with (RFC 8725 §3.1)."""
    if len(algorithms) != 1:
        raise ConfigError(f"JWT auth {case} signs with exactly one algorithm.")
    require_family(algorithms[0], symmetric=True, case=case, instead="'public_keys'")
    return algorithms[0]


@dataclass(frozen=True, slots=True)
class _SharedSecret:
    """A symmetric key already in memory, used with exactly one HS algorithm."""

    secret: Secret[str]

    def check(self, algorithms: tuple[str, ...]) -> None:
        algorithm = _one_algorithm(algorithms, case="'secret'")
        require_hmac_strength(self.secret.reveal(), algorithm, setting="JWT auth 'secret'")

    def load(self, algorithms: tuple[str, ...]) -> Keyring:
        return FixedKey(Key(self.secret.reveal(), algorithms))


@dataclass(frozen=True, slots=True)
class _SecretFile:
    """The released form: a symmetric key read from *path* when the verifier is built."""

    path: str

    def check(self, algorithms: tuple[str, ...]) -> None:
        _one_algorithm(algorithms, case="'secret_path'")

    def load(self, algorithms: tuple[str, ...]) -> Keyring:
        setting = "JWT auth 'secret_path'"
        material = read_key_file(self.path, setting=setting)
        require_hmac_strength(material, algorithms[0], setting=setting)
        return FixedKey(Key(material, algorithms))


@dataclass(frozen=True, slots=True)
class _PublicKeys:
    """Public keys by ``kid``: each one with its own algorithm, or all with ``algorithms``.

    A :class:`JwtPublicKey` names its algorithm, so ``algorithms`` would be a
    second place saying it and is refused. A bare PEM, the released form,
    takes every algorithm of ``algorithms``. The two never mix: one rule per
    configuration.
    """

    keys: Mapping[str, str | JwtPublicKey]

    def check(self, algorithms: tuple[str, ...]) -> None:
        _require_one_form(self.keys.values(), algorithms)
        for algorithm in algorithms:
            require_family(algorithm, symmetric=False, case="'public_keys'", instead="'secret'")
        for key_id, key in self.load(algorithms).keys.items():
            require_public_key(key_id, key.material)

    def load(self, algorithms: tuple[str, ...]) -> KeySet:
        return KeySet({kid: _as_key(entry, algorithms) for kid, entry in self.keys.items()})


_ALGORITHMS_PLACE: Final = {
    True: "JWT auth 'algorithms' is set by each 'public_keys' entry's 'alg'; remove it.",
    False: "JWT auth requires a non-empty 'algorithms' allowlist.",
}


def _require_one_form(entries: Iterable[str | JwtPublicKey], algorithms: tuple[str, ...]) -> None:
    """Every entry names its ``alg`` and ``algorithms`` is empty, or none does and it is set."""
    forms = {isinstance(entry, JwtPublicKey) for entry in entries}
    if len(forms) > 1:
        raise ConfigError(
            "JWT auth 'public_keys' entries are all '{alg, pem}' or all PEM text, not both."
        )
    typed = True in forms
    # Exactly one place names the algorithms: each entry, or the shared list.
    if typed == bool(algorithms):
        raise ConfigError(_ALGORITHMS_PLACE[typed])


def _as_key(entry: str | JwtPublicKey, algorithms: tuple[str, ...]) -> Key:
    if isinstance(entry, JwtPublicKey):
        return Key(entry.pem, (entry.alg,))
    return Key(entry, algorithms)


class JwtAuthConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Validated settings for :class:`~loom.rest.auth.JwtAuthMiddleware`.

    Binds from the ``app.rest.auth.jwt`` config section.  Validation runs on
    construction (fail-fast): exactly one key source, each algorithm fit for
    its key.

    Attributes:
        secret: The shared secret for symmetric algorithms (HS*), resolved by
            a placeholder such as ``${file:...}`` or ``${secrets:...}``. A
            :class:`~loom.core.model.redaction.Secret`, so no repr, log or dump
            shows it. Takes exactly one algorithm in ``algorithms``.
        secret_path: Filesystem path of the shared secret, read when the
            verifier is built: the released form of ``secret``.
        public_keys: Public keys for asymmetric algorithms (RS*/PS*/ES*/EdDSA),
            keyed by the ``kid`` that selects them. Each entry is a
            :class:`JwtPublicKey` naming its own algorithm, or, in the released
            form, PEM text that takes every algorithm of ``algorithms``.
        algorithms: Accepted algorithms for ``secret``, ``secret_path`` and PEM
            text entries. Refused with :class:`JwtPublicKey` entries, which name
            their own.  The ``none`` algorithm is always forbidden.
        audience: Expected ``aud`` claim.  Validated when set; a deployed API
            must set it (deploy check ``loom.rest.E002``).
        issuer: Expected ``iss`` claim.  Validated when set; a deployed API
            must set it (deploy check ``loom.rest.E003``).
        leeway_seconds: Clock-skew tolerance applied to time-based claims, at
            most :data:`MAX_LEEWAY_SECONDS`.
        accepted_types: Accepted values of the token's ``typ`` header, compared
            case-insensitively and without an ``application/`` prefix; empty
            accepts any. Set ``("at+jwt",)`` for an RFC 9068 access-token IdP,
            so an ID token cannot pass as an access token.
        exclude_paths: Exact request paths that bypass authentication.
        roles_claim: Name of the verified claim carrying the roles the caller
            is authorized to use.  Consumed by the SQL endpoint: the effective
            roles are the claim values intersected with the connection
            allowlist, and the request body can only narrow that set.

    Raises:
        ConfigError: If key sources, algorithms, leeway, or the roles claim
            name are invalid.

    Example YAML::

        app:
          rest:
            auth:
              jwt:
                public_keys:
                  "2026-08": {alg: EdDSA, pem: "${ssm:/myapp/jwt/2026-08}"}
                audience: my-api
                issuer: my-gateway
                roles_claim: loom_sql_roles
    """

    secret: Secret[str] | None = None
    secret_path: str | None = None
    public_keys: dict[str, str | JwtPublicKey] = {}
    algorithms: tuple[JwtAlgorithm, ...] = ()
    audience: str | None = None
    issuer: str | None = None
    leeway_seconds: int = 0
    accepted_types: tuple[str, ...] = ()
    exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS
    roles_claim: str | None = None

    def __repr__(self) -> str:
        return (
            f"JwtAuthConfig(secret={self.secret!r},"
            f" secret_path={self.secret_path!r},"
            f" public_keys={sorted(self.public_keys)!r},"
            f" algorithms={self.algorithms!r},"
            f" audience={self.audience!r},"
            f" issuer={self.issuer!r},"
            f" leeway_seconds={self.leeway_seconds!r},"
            f" accepted_types={self.accepted_types!r},"
            f" exclude_paths={self.exclude_paths!r},"
            f" roles_claim={self.roles_claim!r})"
        )

    @classmethod
    def for_issuer(
        cls,
        issuer: JwtIssuerConfig,
        *,
        previous_keys: Mapping[str, JwtPublicKey] | None = None,
        leeway_seconds: int = 0,
        exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS,
    ) -> JwtAuthConfig:
        """Build the verifier of the tokens *issuer* mints.

        For the service that both issues and verifies. Every value that must
        match comes from *issuer*, so the two cannot drift: the algorithm,
        audience, issuer, roles claim and ``typ``, and the key. An asymmetric
        key publishes only its derived public half under ``issuer.kid``; a
        symmetric one is the same secret.

        Args:
            issuer: Settings of the issuer whose tokens are verified.
            previous_keys: Public keys of earlier ``kid`` values, kept published
                for one rotation window, each with its own algorithm. Reusing
                ``issuer.kid`` is refused: the derived key cannot be overridden.
            leeway_seconds: Clock-skew tolerance, at most :data:`MAX_LEEWAY_SECONDS`.
            exclude_paths: Exact request paths that bypass authentication.

        Returns:
            A validated ``JwtAuthConfig``.

        Raises:
            ConfigError: When an asymmetric issuer has no ``kid``, previous keys
                come with a symmetric one, ``previous_keys`` reuses the ``kid``,
                or the signing key cannot be parsed. Parser errors are chained
                and never carry the material.
        """
        return cls(
            **_verifier_keys(issuer, previous_keys or {}),
            audience=issuer.audience,
            issuer=issuer.issuer,
            roles_claim=issuer.roles_claim,
            accepted_types=(issuer.typ,) if issuer.typ else (),
            leeway_seconds=leeway_seconds,
            exclude_paths=exclude_paths,
        )

    @property
    def _key_source(self) -> _SharedSecret | _SecretFile | _PublicKeys:
        """The configured key, as the object that knows how to check and load it."""
        if self.secret is not None:
            return _SharedSecret(self.secret)
        if self.secret_path is not None:
            return _SecretFile(self.secret_path)
        return _PublicKeys(self.public_keys)

    def __post_init__(self) -> None:
        self._validate_keys()
        if not 0 <= self.leeway_seconds <= MAX_LEEWAY_SECONDS:
            raise ConfigError(f"JWT auth 'leeway_seconds' must be in 0..{MAX_LEEWAY_SECONDS}.")
        if any(not kind.strip() for kind in self.accepted_types):
            raise ConfigError("JWT auth 'accepted_types' entries must not be blank.")
        # A blank audience or issuer is never what was meant: a placeholder
        # resolved to nothing. Refusing it keeps it from reaching PyJWT unnoticed.
        _require_not_blank(
            "JWT auth", roles_claim=self.roles_claim, audience=self.audience, issuer=self.issuer
        )

    def _validate_keys(self) -> None:
        """One key source and supported algorithms, each fit for that key."""
        given = (self.secret is not None, self.secret_path is not None, bool(self.public_keys))
        if sum(given) != 1:
            raise ConfigError(
                "JWT auth requires exactly one of 'secret', 'secret_path' or 'public_keys'."
            )
        for algorithm in self.algorithms:
            require_supported(algorithm, setting="JWT auth 'algorithms' entry")
        self._key_source.check(self.algorithms)

    def _keyring(self) -> Keyring:
        """Load the keys a verifier checks tokens with, once, when it is built.

        A rotated secret therefore takes a restart, as any resolved value does.

        Raises:
            ConfigError: When the ``secret_path`` file cannot be read or holds a
                key shorter than its algorithm's hash.
        """
        return self._key_source.load(self.algorithms)


def _verifier_keys(issuer: JwtIssuerConfig, previous: Mapping[str, JwtPublicKey]) -> dict[str, Any]:
    """The verifier's key settings for *issuer*: the same secret, or the derived public key."""
    if is_symmetric(issuer.algorithm):
        if previous:
            raise ConfigError("previous_keys apply to an asymmetric issuer only.")
        return {"secret": issuer.signing_key, "algorithms": (issuer.algorithm,)}
    if issuer.kid is None:
        raise ConfigError("for_issuer needs the issuer 'kid' to publish its public key.")
    if issuer.kid in previous:
        raise ConfigError(
            f"previous_keys reuses the issuer kid {issuer.kid!r}; "
            "the derived key cannot be overridden."
        )
    derived = JwtPublicKey(
        alg=issuer.algorithm, pem=derived_public_key(issuer.signing_key.reveal())
    )
    return {"public_keys": {issuer.kid: derived, **previous}}


class JwtIssuerConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Validated settings for :class:`~loom.rest.auth.JwtIssuer`.

    Separate from :class:`JwtAuthConfig` on purpose, and not a few extra fields
    on it: a service that only verifies must have no configuration path through
    which signing material can land in its process.  The fields diverge anyway —
    one algorithm instead of an allowlist, ``audience`` and ``issuer`` mandatory
    rather than optional, a lifetime the verifier has no use for.

    Attributes:
        signing_key: The key, as a :class:`~loom.core.model.redaction.Secret`
            that no repr, log or dump shows: a PEM private key for RS*, PS*,
            ES* and EdDSA, a secret of at least the hash size for HS*. Bound
            from configuration, a placeholder resolves it, for example
            ``${file:/run/secrets/jwt.pem}`` or ``${secrets:/myapp/jwt}``.
            A shared secret means every service that verifies can also mint.
        algorithm: The single algorithm tokens are signed with. Issuing chooses
            one; only verification negotiates an allowlist.
        audience: ``aud`` stamped on every token. Mandatory: a token without an
            audience is valid at any service that shares the key.
        issuer: ``iss`` stamped on every token. Mandatory.
        roles_claim: Claim carrying the caller roles. Mandatory, and never a
            registered claim: an issuer that does not own that name lets an
            attribute impersonate it. An identity without roles is refused.
        ttl_seconds: Lifetime of a minted token, in ``1..MAX_ISSUER_TTL_SECONDS``,
            and the ceiling for any per-call override.
        kid: Key identifier stamped on the token header, so a verifier can
            select the right key during a rotation overlap.
        typ: ``typ`` header stamped on every token, for example
            ``at+jwt`` (RFC 9068 §2.1) so a verifier with ``accepted_types``
            accepts them. ``None`` leaves PyJWT's default ``JWT``.

    Raises:
        TypeError: If the signing key is not a ``Secret``.
        ConfigError: If the signing key does not fit the algorithm, or the
            algorithm, audience, issuer, roles claim or lifetime are invalid.

    Example::

        config = JwtIssuerConfig(
            signing_key=Secret(Path("/run/secrets/jwt.pem").read_text()),
            algorithm="EdDSA",
            audience="my-api",
            issuer="my-gateway",
            roles_claim="loom_sql_roles",
            kid="2026-08",
        )
    """

    signing_key: Secret[str]
    algorithm: JwtAlgorithm
    audience: str
    issuer: str
    roles_claim: str
    ttl_seconds: int = 900
    kid: str | None = None
    typ: str | None = None

    def __repr__(self) -> str:
        return (
            f"JwtIssuerConfig(signing_key={self.signing_key!r},"
            f" algorithm={self.algorithm!r},"
            f" audience={self.audience!r},"
            f" issuer={self.issuer!r},"
            f" roles_claim={self.roles_claim!r},"
            f" ttl_seconds={self.ttl_seconds!r},"
            f" kid={self.kid!r},"
            f" typ={self.typ!r})"
        )

    def __post_init__(self) -> None:
        # The algorithm first: a key that does not fit an unknown one says nothing useful.
        require_supported(self.algorithm, setting="JWT issuer 'algorithm'")
        # A plain value never gets here: the struct refuses it before this runs.
        require_signing_key(self.signing_key.reveal(), self.algorithm)
        self._validate_claims()

    def _validate_claims(self) -> None:
        if not self.audience.strip() or not self.issuer.strip():
            raise ConfigError("JWT issuer requires a non-blank 'audience' and 'issuer'.")
        # Optional here is what let an attribute impersonate the verifier's roles
        # claim: the issuer must always own that name.
        _require_usable_roles_claim(self.roles_claim)
        _require_not_blank("JWT issuer", kid=self.kid, typ=self.typ)
        if not 0 < self.ttl_seconds <= MAX_ISSUER_TTL_SECONDS:
            raise ConfigError(f"JWT issuer 'ttl_seconds' must be in 1..{MAX_ISSUER_TTL_SECONDS}.")


__all__ = [
    "DEFAULT_EXCLUDE_PATHS",
    "MAX_ISSUER_TTL_SECONDS",
    "MAX_LEEWAY_SECONDS",
    "RESERVED_CLAIMS",
    "SUPPORTED_ALGORITHMS",
    "JwtAlgorithm",
    "JwtAuthConfig",
    "JwtIssuerConfig",
    "JwtPublicKey",
]
