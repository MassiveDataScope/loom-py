"""Configuration for the native stateless JWT authentication middleware."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from loom.core.config.errors import ConfigError
from loom.core.config.secrets import SecretsManagerResolver
from loom.core.config.ssm import SsmResolver
from loom.core.model import LoomFrozenStruct

DEFAULT_EXCLUDE_PATHS: tuple[str, ...] = (
    "/docs",
    "/redoc",
    "/openapi.json",
    "/metrics",
    "/health",
)
"""Paths served without authentication unless the application says otherwise."""

_FORBIDDEN_ALGORITHM = "none"

_KEY_REF_RESOLVER_FACTORIES: dict[str, Callable[[str | None], Any]] = {
    "secrets": SecretsManagerResolver,
    "ssm": SsmResolver,
}
"""Resolver constructors by ``private_key_ref`` prefix, taking the AWS region.

Importing them is safe without boto3: both modules guard the import and fail
with the install hint when the resolver is actually used.
"""

_SORTED_KEY_REF_RESOLVERS: list[str] = sorted(_KEY_REF_RESOLVER_FACTORIES)

KEY_REF_RESOLVERS: tuple[str, ...] = tuple(_SORTED_KEY_REF_RESOLVERS)
"""Resolver prefixes a ``private_key_ref`` may name, derived from the factories."""

MAX_ISSUER_TTL_SECONDS = 3600
"""Ceiling for a minted token: signing is reading, so a long-lived token is a
standing grant that stateless verification cannot revoke."""

SUPPORTED_ALGORITHMS: frozenset[str] = frozenset(
    {
        "EdDSA",
        "ES256",
        "ES384",
        "ES512",
        "HS256",
        "HS384",
        "HS512",
        "PS256",
        "PS384",
        "PS512",
        "RS256",
        "RS384",
        "RS512",
    }
)
"""Algorithms this framework signs with, checked at startup rather than on the
first request: a typo like ``" HS256"`` would otherwise fail per-call."""


# RFC 7519 registered claims: a roles claim named after one of these is silently
# overwritten when the token is built, leaving no roles and no error.
RESERVED_CLAIMS: frozenset[str] = frozenset({"iss", "sub", "aud", "exp", "nbf", "iat", "jti"})


def _is_symmetric(algorithm: str) -> bool:
    """Return ``True`` when *algorithm* uses a shared secret (HS* family)."""
    return algorithm.upper().startswith("HS")


def _require_supported(algorithm: str, *, setting: str) -> None:
    """Reject ``none`` and anything outside the supported set, at startup.

    Shared by both configs on purpose: a hardening applied to one and not the
    other is how the two drift apart.
    """
    if algorithm.lower() == _FORBIDDEN_ALGORITHM:
        raise ConfigError("The 'none' JWT algorithm is always forbidden.")
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise ConfigError(
            f"{setting} {algorithm!r} is not supported. "
            f"Choose one of: {', '.join(sorted(SUPPORTED_ALGORITHMS))}."
        )


def _read_key_file(path: str, *, setting: str) -> str:
    """Read key material from disk, never chaining the cause.

    An ``OSError`` message carries the path and a ``UnicodeDecodeError`` carries
    the whole file on ``exc.object``: either would publish key material through a
    traceback or a structured log that serializes the exception.
    """
    try:
        return Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        raise ConfigError(f"Could not read {setting}.") from None


def _require_usable_roles_claim(roles_claim: str) -> None:
    """Reject a roles claim that is blank or shadows a registered claim."""
    if not roles_claim or not roles_claim.strip():
        raise ConfigError("JWT issuer requires a non-blank 'roles_claim'.")
    if roles_claim in RESERVED_CLAIMS:
        raise ConfigError(
            f"JWT issuer 'roles_claim' {roles_claim!r} is a registered claim: "
            "the token would carry no roles and raise nothing."
        )


class JwtAuthConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Validated settings for :class:`~loom.rest.auth.JwtAuthMiddleware`.

    Binds from the ``app.rest.auth.jwt`` config section.  Validation runs on
    construction (fail-fast): exactly one key source must be provided and the
    algorithm allowlist must be non-empty and coherent with that key source.

    Attributes:
        secret_path: Filesystem path of the shared secret for symmetric
            algorithms (HS*).  A path and not the value: this is a
            ``msgspec.Struct``, so any serializer emits its fields verbatim and a
            config dump would publish the key that verifies *and signs*.
            Mutually exclusive with ``public_keys``.
        public_keys: Static PEM-encoded public keys for asymmetric algorithms
            (RS*/ES*/EdDSA), keyed by the ``kid`` that selects them.  Mutually
            exclusive with ``secret``.
        algorithms: Explicit allowlist of accepted JWT algorithms.  The
            ``none`` algorithm is always forbidden.
        audience: Expected ``aud`` claim.  Validated only when set.
        issuer: Expected ``iss`` claim.  Validated only when set.
        leeway_seconds: Clock-skew tolerance applied to time-based claims.
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
                secret_path: ${oc.env:LOOM_JWT_SECRET_PATH}
                algorithms: [HS256]
                roles_claim: loom_sql_roles
    """

    secret_path: str | None = None
    public_keys: dict[str, str] = {}
    algorithms: tuple[str, ...] = ()
    audience: str | None = None
    issuer: str | None = None
    leeway_seconds: int = 0
    exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS
    roles_claim: str | None = None

    def __repr__(self) -> str:
        return (
            f"JwtAuthConfig(secret_path={self.secret_path!r},"
            f" public_keys={sorted(self.public_keys)!r},"
            f" algorithms={self.algorithms!r},"
            f" audience={self.audience!r},"
            f" issuer={self.issuer!r},"
            f" leeway_seconds={self.leeway_seconds!r},"
            f" exclude_paths={self.exclude_paths!r},"
            f" roles_claim={self.roles_claim!r})"
        )

    @classmethod
    def from_signing_key(
        cls,
        private_key_path: str | None = None,
        *,
        private_key_ref: str | None = None,
        key_ref_region: str | None = None,
        kid: str,
        algorithms: tuple[str, ...],
        audience: str | None = None,
        issuer: str | None = None,
        leeway_seconds: int = 0,
        exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS,
        roles_claim: str | None = None,
        additional_public_keys: dict[str, str] | None = None,
    ) -> JwtAuthConfig:
        """Build a verifier whose public key is derived from the signing key.

        For the service that both issues and verifies. Two configured values
        that must match are two values that can disagree, and a stale public
        key does not fail loudly — it accepts nothing, or accepts what a
        rotated key signed. Deriving removes the second value.

        Args:
            private_key_path: Filesystem path of the PEM signing key. Only
                the derived public key is kept; the private material is
                dropped. Mutually exclusive with ``private_key_ref``.
            private_key_ref: Managed-store reference of the PEM signing key,
                as accepted by :attr:`JwtIssuerConfig.private_key_ref`.
                Mutually exclusive with ``private_key_path``.
            key_ref_region: AWS region for the ref resolver. Requires
                ``private_key_ref``.
            kid: Key identifier the derived public key is published under.
            algorithms: Explicit allowlist of accepted JWT algorithms.
            audience: Expected ``aud`` claim. Validated only when set.
            issuer: Expected ``iss`` claim. Validated only when set.
            leeway_seconds: Clock-skew tolerance for time-based claims.
            exclude_paths: Exact request paths that bypass authentication.
            roles_claim: Verified claim carrying the caller roles.
            additional_public_keys: Extra public keys by ``kid``, so the
                previous key stays published for one rotation window. Public
                material, so unlike the signing key it is safe to carry as a
                value. Reusing the derived ``kid`` here is refused: this
                method exists so the published key cannot drift from the
                signing key, and an override would reintroduce the drift.

        Returns:
            A validated ``JwtAuthConfig`` publishing the derived key.

        Raises:
            ConfigError: If the sources are not exactly one, or the signing
                key cannot be read or parsed. The cause is chained: parser
                errors name the reason and never carry the material.
        """
        if (private_key_path is None) == (private_key_ref is None):
            raise ConfigError(
                "from_signing_key requires exactly one of 'private_key_path' or 'private_key_ref'."
            )
        if key_ref_region is not None and private_key_ref is None:
            raise ConfigError("from_signing_key 'key_ref_region' requires 'private_key_ref'.")
        try:
            from cryptography.exceptions import UnsupportedAlgorithm
            from cryptography.hazmat.primitives import serialization
        except ImportError as exc:  # pragma: no cover - jwt extra ships it
            raise ConfigError(
                "cryptography is required to derive the JWT public key. "
                "Install it with: pip install loom-kernel[jwt]"
            ) from exc
        if private_key_ref is not None:
            _require_valid_key_ref(private_key_ref)
            material = _fetch_key_ref(private_key_ref, region=key_ref_region)
        else:
            source = private_key_path
            assert source is not None  # narrowed by the exactly-one check above
            material = _read_key_file(source, setting="the JWT signing key")
        try:
            private = serialization.load_pem_private_key(material.encode(), password=None)
            public = private.public_key().public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        except (ValueError, TypeError, UnsupportedAlgorithm) as exc:
            raise ConfigError("Could not derive the JWT public key from the signing key.") from exc
        extra = additional_public_keys or {}
        if kid in extra:
            raise ConfigError(
                f"additional_public_keys reuses the derived kid {kid!r}; the derived "
                "key cannot be overridden."
            )
        keys = {kid: public.decode(), **extra}
        return cls(
            public_keys=keys,
            algorithms=algorithms,
            audience=audience,
            issuer=issuer,
            leeway_seconds=leeway_seconds,
            exclude_paths=exclude_paths,
            roles_claim=roles_claim,
        )

    def __post_init__(self) -> None:
        if (self.secret_path is None) == (not self.public_keys):
            raise ConfigError("JWT auth requires exactly one of 'secret_path' or 'public_keys'.")
        for key_id, material in self.public_keys.items():
            if not key_id.strip() or not material.strip():
                raise ConfigError("JWT auth 'public_keys' entries need a key id and material.")
        if not self.algorithms:
            raise ConfigError("JWT auth requires a non-empty 'algorithms' allowlist.")
        if self.leeway_seconds < 0:
            raise ConfigError("JWT auth 'leeway_seconds' must be >= 0.")
        if self.roles_claim is not None and not self.roles_claim.strip():
            raise ConfigError("JWT auth 'roles_claim' must not be blank.")
        for algorithm in self.algorithms:
            self._validate_algorithm(algorithm)

    def _validate_algorithm(self, algorithm: str) -> None:
        _require_supported(algorithm, setting="JWT auth 'algorithms' entry")
        if _is_symmetric(algorithm) and self.secret_path is None:
            raise ConfigError(
                f"JWT algorithm {algorithm!r} requires 'secret_path', not 'public_keys'."
            )
        if not _is_symmetric(algorithm) and not self.public_keys:
            raise ConfigError(f"JWT algorithm {algorithm!r} requires 'public_keys', not 'secret'.")

    def verification_key(self, key_id: str | None) -> str | None:
        """Return the key that verifies a token, selected by its ``kid``.

        Selection is explicit and never exhaustive: trying every configured key
        in turn would decouple each algorithm from its key family, which is what
        makes algorithm confusion structurally impossible here.

        Args:
            key_id: ``kid`` header of the token, or ``None`` when it carries none.

        Returns:
            The key material, or ``None`` when no configured key applies — an
            unknown ``kid``, or a missing one while several keys are configured.
        """
        if self.secret_path is not None:
            return _read_key_file(self.secret_path, setting="JWT auth 'secret_path'")
        if key_id is not None:
            return self.public_keys.get(key_id)
        if len(self.public_keys) == 1:
            return next(iter(self.public_keys.values()))
        return None


class JwtIssuerConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Validated settings for :class:`~loom.rest.auth.JwtIssuer`.

    Separate from :class:`JwtAuthConfig` on purpose, and not a few extra fields
    on it: a service that only verifies must have no configuration path through
    which signing material can land in its process.  The fields diverge anyway —
    one algorithm instead of an allowlist, ``audience`` and ``issuer`` mandatory
    rather than optional, a lifetime the verifier has no use for.

    The private key is read from ``private_key_path`` and never held as a field.
    ``__repr__`` redaction would not be enough: this is a ``msgspec.Struct``, so
    ``msgspec.json.encode`` and ``to_builtins`` emit every field verbatim without
    going through it, and a config dump would publish the signing key.

    Attributes:
        private_key_path: Filesystem path of the PEM signing key, for
            asymmetric algorithms. Mutually exclusive with the other sources.
        private_key_ref: Managed-store reference of the PEM signing key, as
            ``"<resolver>:<key>"`` with resolver one of ``secrets`` or ``ssm``
            (e.g. ``"secrets:/myapp/prod/jwt-signing-key"``). Resolved once,
            when the issuer loads the key, so the material never touches disk
            or config — and rotating the stored value requires a restart. The
            Secrets Manager resolver navigates dots as JSON paths, so the key
            name must not contain ``.``. Mutually exclusive with the other
            sources.
        key_ref_region: AWS region the ref resolver is built with. Defaults to
            boto3's own resolution chain. Requires ``private_key_ref``.
        secret_path: Filesystem path of the shared secret for HS* algorithms.
            Requires ``allow_symmetric_signing``. Mutually exclusive with the
            other sources.
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
        allow_symmetric_signing: Opt-in required for HS*. With a shared secret
            every verifier can also mint, so the choice must be deliberate.

    Raises:
        ConfigError: If key sources, algorithm, audience, issuer, roles claim
            or lifetime are invalid.

    Note:
        Built programmatically, not bound from a config section: issuing happens in
        an application use case that injects the issuer, not in the REST layer that
        owns ``app.rest.auth``. Resolve the values however the service resolves its
        own settings.

    Example::

        config = JwtIssuerConfig(
            private_key_path=os.environ["JWT_SIGNING_KEY_PATH"],
            algorithm="EdDSA",
            audience="my-api",
            issuer="my-gateway",
            roles_claim="loom_sql_roles",
            ttl_seconds=900,
            kid="2026-08",
        )
    """

    private_key_path: str | None = None
    private_key_ref: str | None = None
    key_ref_region: str | None = None
    secret_path: str | None = None
    algorithm: str = ""
    audience: str = ""
    issuer: str = ""
    roles_claim: str = ""
    ttl_seconds: int = 900
    kid: str | None = None
    allow_symmetric_signing: bool = False

    def __post_init__(self) -> None:
        sources = [
            source
            for source in (self.private_key_path, self.private_key_ref, self.secret_path)
            if source is not None
        ]
        if len(sources) != 1:
            raise ConfigError(
                "JWT issuer requires exactly one of 'private_key_path', "
                "'private_key_ref' or 'secret_path'."
            )
        if self.private_key_ref is not None:
            _require_valid_key_ref(self.private_key_ref)
        if self.key_ref_region is not None and self.private_key_ref is None:
            raise ConfigError("JWT issuer 'key_ref_region' requires 'private_key_ref'.")
        _require_supported(self.algorithm, setting="JWT issuer 'algorithm'")
        if not self.audience.strip():
            raise ConfigError("JWT issuer requires a non-blank 'audience'.")
        if not self.issuer.strip():
            raise ConfigError("JWT issuer requires a non-blank 'issuer'.")
        # Optional here is what let an attribute impersonate the verifier's roles
        # claim: the issuer must always own that name.
        _require_usable_roles_claim(self.roles_claim)
        if self.kid is not None and not self.kid.strip():
            raise ConfigError("JWT issuer 'kid' must not be blank.")
        if not 0 < self.ttl_seconds <= MAX_ISSUER_TTL_SECONDS:
            raise ConfigError(f"JWT issuer 'ttl_seconds' must be in 1..{MAX_ISSUER_TTL_SECONDS}.")
        self._validate_signing_material()

    def _validate_signing_material(self) -> None:
        symmetric = _is_symmetric(self.algorithm)
        if symmetric and not self.allow_symmetric_signing:
            raise ConfigError(
                f"JWT algorithm {self.algorithm!r} shares one key between signer and "
                "verifier, so every verifier could also mint. Set "
                "'allow_symmetric_signing' to accept that."
            )
        if symmetric and self.secret_path is None:
            raise ConfigError(
                f"JWT algorithm {self.algorithm!r} requires 'secret_path', not 'private_key_path'."
            )
        if not symmetric and self.private_key_path is None and self.private_key_ref is None:
            raise ConfigError(
                f"JWT algorithm {self.algorithm!r} requires 'private_key_path' or "
                "'private_key_ref', not 'secret_path'."
            )

    def load_signing_key(self) -> str:
        """Load the signing key, so it lives in the issuer and not in the config.

        Returns:
            The key material, read from disk or resolved from the managed
            store when ``private_key_ref`` is set.

        Raises:
            ConfigError: If the file cannot be read or is not UTF-8 text (the
                cause is never chained: an OS error carries the path, and a
                ``UnicodeDecodeError`` carries the file contents on
                ``exc.object``), or if the managed store cannot answer (that
                cause is chained: resolver errors never carry the material).
        """
        if self.private_key_ref is not None:
            return _fetch_key_ref(self.private_key_ref, region=self.key_ref_region)
        source = self.secret_path or self.private_key_path
        if source is None:  # pragma: no cover - __post_init__ rules this out
            raise ConfigError("JWT issuer has no signing key configured.")
        return _read_key_file(source, setting="the JWT issuer signing key")


def _require_valid_key_ref(ref: str) -> None:
    """Reject a malformed key ref at construction, not on the first load.

    Args:
        ref: Candidate ``"<resolver>:<key>"`` reference.

    Raises:
        ConfigError: If the resolver prefix is unknown or the key is blank.
    """
    name, separator, key = ref.partition(":")
    if not separator or name not in _KEY_REF_RESOLVER_FACTORIES or not key.strip():
        raise ConfigError(
            "'private_key_ref' must be '<resolver>:<key>' with resolver "
            f"in {_SORTED_KEY_REF_RESOLVERS}."
        )


def _fetch_key_ref(ref: str, *, region: str | None) -> str:
    """Resolve a signing key reference into the key material.

    Args:
        ref: Validated ``"<resolver>:<key>"`` reference.
        region: AWS region the resolver is built with, or ``None`` for
            boto3's own resolution chain.

    Returns:
        The key material held by the managed store.

    Raises:
        ConfigError: If the store cannot answer or answers with something
            other than non-empty text. The cause is chained: resolver errors
            name the parameter, never the material.
    """
    name, _, key = ref.partition(":")
    resolver = _KEY_REF_RESOLVER_FACTORIES[name](region)
    try:
        value = resolver.resolve(key)
    except ConfigError:
        raise
    except Exception as exc:
        raise ConfigError(f"Could not resolve the JWT signing key ref via {name!r}.") from exc
    if not isinstance(value, str) or not value.strip():
        raise ConfigError("The resolved JWT signing key is not non-empty text.")
    return value
