"""Configuration for the native stateless JWT authentication middleware."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Protocol

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

MAX_LEEWAY_SECONDS: Final = 300
"""Largest clock-skew tolerance a verifier accepts: RFC 7519 §4.1.4 asks for a
small leeway, and a larger one quietly extends every token's lifetime."""

_HMAC_MINIMUM_BYTES: Final = {"HS256": 32, "HS384": 48, "HS512": 64}
"""Smallest HMAC key per algorithm: the size of its hash, a MUST of RFC 7518 §3.2."""

_KEY_REF_RESOLVER_FACTORIES: dict[str, Callable[[str | None], Any]] = {
    "secrets": SecretsManagerResolver,
    "ssm": SsmResolver,
}
"""Resolver constructors by :class:`ManagedPrivateKey` prefix, taking the AWS region.

Importing them is safe without boto3: both modules guard the import and fail
with the install hint when the resolver is actually used.
"""

_SORTED_KEY_REF_RESOLVERS: list[str] = sorted(_KEY_REF_RESOLVER_FACTORIES)

KEY_REF_RESOLVERS: tuple[str, ...] = tuple(_SORTED_KEY_REF_RESOLVERS)
"""Resolver prefixes a :class:`ManagedPrivateKey` may name, derived from the factories."""

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


# RFC 7519 §4.1 registered claims. They describe the token, not the caller, so
# they never become identity attributes, and a roles claim named after one is
# silently overwritten when the token is built.
RESERVED_CLAIMS: frozenset[str] = frozenset({"iss", "sub", "aud", "exp", "nbf", "iat", "jti"})


_MIN_RSA_BITS: Final = 2048
"""Smallest RSA modulus accepted for signing or verifying: a MUST of RFC 7518 §3.3."""


def _is_symmetric(algorithm: str) -> bool:
    """Return ``True`` when *algorithm* uses a shared secret: the HMAC table is the one list."""
    return algorithm.upper() in _HMAC_MINIMUM_BYTES


def _require_family(algorithm: str, *, symmetric: bool, case: str, instead: str) -> None:
    """Refuse an algorithm of the other key family; every key case shares this one rule."""
    if _is_symmetric(algorithm) != symmetric:
        raise ConfigError(f"JWT algorithm {algorithm!r} needs {instead}, not {case}.")


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


class _Keyring(Protocol):
    """The loaded keys of a verifier, picking the one that checks a token."""

    def pick(self, key_id: str | None) -> str | None:
        """The key for a token with this ``kid``, or ``None`` when none applies."""
        ...


@dataclass(frozen=True, slots=True)
class _FixedKey:
    """One shared secret: every token is checked with it, whatever its ``kid``."""

    secret: str = field(repr=False)

    def pick(self, key_id: str | None) -> str | None:
        return self.secret


@dataclass(frozen=True, slots=True)
class _SharedSecret:
    """A symmetric key read from a file, used with exactly one HS algorithm."""

    path: str

    def check(self, algorithms: tuple[str, ...]) -> None:
        if len(algorithms) != 1:
            # RFC 8725 §3.1: a key is used with exactly one algorithm.
            raise ConfigError("JWT auth 'secret_path' signs with exactly one algorithm.")
        _require_family(
            algorithms[0], symmetric=True, case="'secret_path'", instead="'public_keys'"
        )

    def fields(self) -> tuple[str | None, dict[str, str]]:
        """The ``secret_path`` and ``public_keys`` that express this case."""
        return self.path, {}

    def load(self, algorithms: tuple[str, ...]) -> _Keyring:
        return _FixedKey(_load_secret(self.path, algorithms[0], setting="JWT auth 'secret_path'"))


@dataclass(frozen=True, slots=True)
class _PublicKeySet:
    """Public keys by ``kid``, for the asymmetric algorithms only.

    Selection is explicit and never exhaustive: trying every key in turn would
    decouple each algorithm from its key family, which is what makes algorithm
    confusion structurally impossible here.
    """

    keys: Mapping[str, str]

    def check(self, algorithms: tuple[str, ...]) -> None:
        for key_id, material in self.keys.items():
            _require_public_key(key_id, material)
        for algorithm in algorithms:
            _require_family(
                algorithm, symmetric=False, case="'public_keys'", instead="'secret_path'"
            )

    def fields(self) -> tuple[str | None, dict[str, str]]:
        """The ``secret_path`` and ``public_keys`` that express this case."""
        return None, dict(self.keys)

    def load(self, algorithms: tuple[str, ...]) -> _Keyring:
        # Copied: ``frozen`` stops rebinding, not mutation, and this is an allowlist.
        return _PublicKeySet(dict(self.keys))

    def pick(self, key_id: str | None) -> str | None:
        if key_id is not None:
            return self.keys.get(key_id)
        return next(iter(self.keys.values())) if len(self.keys) == 1 else None


def _require_public_key(key_id: str, material: str) -> None:
    """A usable entry holding public material only: the struct must never carry a secret."""
    if not key_id.strip() or not material.strip():
        raise ConfigError("JWT auth 'public_keys' entries need a key id and material.")
    if "PRIVATE KEY" in material:
        raise ConfigError(
            f"JWT auth 'public_keys' entry {key_id!r} holds a private key; "
            "publish only its public half."
        )
    _require_rsa_strength(material, public=True, setting=f"JWT auth 'public_keys' {key_id!r}")


def _require_rsa_strength(material: str, *, public: bool, setting: str) -> None:
    """Refuse an RSA key below 2048 bits (RFC 7518 §3.3); other key types pass.

    The message names the size, never the key.
    """
    bits = _rsa_bits(material, public=public)
    if bits is not None and bits < _MIN_RSA_BITS:
        raise ConfigError(
            f"{setting} is a {bits}-bit RSA key; at least {_MIN_RSA_BITS} bits "
            "are required (RFC 7518 section 3.3)."
        )


def _rsa_bits(material: str, *, public: bool) -> int | None:
    """The modulus size of an RSA key, or ``None`` for any other key.

    Material that does not parse is left to PyJWT, which refuses it on use:
    the strength rule judges size, not format.
    """
    try:
        from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
        from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415
    except ImportError:  # pragma: no cover - the jwt extra ships it
        return None
    load = serialization.load_pem_public_key if public else _load_private_pem
    try:
        key: Any = load(material.encode())
    except (ValueError, TypeError):
        return None
    return key.key_size if isinstance(key, (rsa.RSAPublicKey, rsa.RSAPrivateKey)) else None


def _load_private_pem(data: bytes) -> Any:
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    return serialization.load_pem_private_key(data, password=None)


def _load_secret(path: str, algorithm: str, *, setting: str) -> str:
    """Read a shared secret and refuse it when shorter than its algorithm's hash."""
    secret = _read_key_file(path, setting=setting)
    _require_hmac_strength(secret, algorithm, setting=setting)
    return secret


def _require_hmac_strength(secret: str, algorithm: str, *, setting: str) -> None:
    """Refuse an HMAC key shorter than its algorithm's hash, or made of whitespace only.

    RFC 7518 §3.2 makes the hash size a MUST, so the rule holds in every
    environment. The message names the size, never the key. A trailing line
    break does not count: the key is used byte for byte, so the file must
    carry the full length without it.

    Raises:
        ConfigError: When the key is blank or shorter than *algorithm*'s hash.
    """
    minimum = _HMAC_MINIMUM_BYTES[algorithm.upper()]
    if not secret.strip():
        raise ConfigError(f"{setting} is blank.")
    if len(secret.rstrip("\r\n").encode("utf-8")) < minimum:
        raise ConfigError(
            f"{setting} must be at least {minimum} bytes for {algorithm} (RFC 7518 section 3.2)."
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
                secret_path: ${oc.env:JWT_SECRET_PATH}
                algorithms: [HS256]
                roles_claim: loom_sql_roles
    """

    secret_path: str | None = None
    public_keys: dict[str, str] = {}
    algorithms: tuple[str, ...] = ()
    audience: str | None = None
    issuer: str | None = None
    leeway_seconds: int = 0
    accepted_types: tuple[str, ...] = ()
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
            f" accepted_types={self.accepted_types!r},"
            f" exclude_paths={self.exclude_paths!r},"
            f" roles_claim={self.roles_claim!r})"
        )

    @classmethod
    def for_issuer(
        cls,
        issuer: JwtIssuerConfig,
        *,
        previous_keys: Mapping[str, str] | None = None,
        leeway_seconds: int = 0,
        exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS,
    ) -> JwtAuthConfig:
        """Build the verifier of the tokens *issuer* mints.

        For the service that both issues and verifies. Every value that must
        match comes from *issuer*, so the two cannot drift: the algorithm,
        audience, issuer, roles claim and ``typ``, and the key. An asymmetric
        key publishes only its derived public half under ``issuer.kid``; a
        symmetric one is the same secret file.

        Args:
            issuer: Settings of the issuer whose tokens are verified.
            previous_keys: Public keys of earlier ``kid`` values, kept published
                for one rotation window. Reusing ``issuer.kid`` is refused: the
                derived key cannot be overridden.
            leeway_seconds: Clock-skew tolerance, at most :data:`MAX_LEEWAY_SECONDS`.
            exclude_paths: Exact request paths that bypass authentication.

        Returns:
            A validated ``JwtAuthConfig``.

        Raises:
            ConfigError: When an asymmetric issuer has no ``kid``, previous keys
                come with a symmetric one, ``previous_keys`` reuses the ``kid``,
                or the signing key cannot be read or parsed. Parser errors are
                chained and never carry the material.
        """
        source = issuer.signing_key.verifier_key(issuer.algorithm, issuer.kid, previous_keys or {})
        secret_path, public_keys = source.fields()
        return cls(
            secret_path=secret_path,
            public_keys=public_keys,
            algorithms=(issuer.algorithm,),
            audience=issuer.audience,
            issuer=issuer.issuer,
            roles_claim=issuer.roles_claim,
            accepted_types=(issuer.typ,) if issuer.typ else (),
            leeway_seconds=leeway_seconds,
            exclude_paths=exclude_paths,
        )

    @property
    def _key_source(self) -> _SharedSecret | _PublicKeySet:
        """The configured key, as the object that knows how to check and load it."""
        if self.secret_path is not None:
            return _SharedSecret(self.secret_path)
        return _PublicKeySet(self.public_keys)

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
        """One key source, an allowlist of supported algorithms, each fit for that key."""
        if (self.secret_path is None) == (not self.public_keys):
            raise ConfigError("JWT auth requires exactly one of 'secret_path' or 'public_keys'.")
        if not self.algorithms:
            raise ConfigError("JWT auth requires a non-empty 'algorithms' allowlist.")
        for algorithm in self.algorithms:
            _require_supported(algorithm, setting="JWT auth 'algorithms' entry")
        self._key_source.check(self.algorithms)

    def _keyring(self) -> _Keyring:
        """Load the keys a verifier checks tokens with, once, when it is built.

        A rotated secret file therefore takes a restart, as a rotated
        :class:`ManagedPrivateKey` already does.

        Raises:
            ConfigError: When the ``secret_path`` file cannot be read or holds a
                key shorter than its algorithm's hash.
        """
        return self._key_source.load(self.algorithms)


class _AsymmetricKey(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """What both private-key cases share; each one only says where its key is read."""

    def _read(self) -> str:
        raise NotImplementedError

    def check(self, algorithm: str) -> None:
        """Refuse an algorithm this key cannot sign with."""
        _require_family(
            algorithm,
            symmetric=False,
            case=f"a {type(self).__name__}",
            instead="a SharedSecretFile",
        )

    def load(self, algorithm: str) -> str:
        """Read the key and refuse an RSA key below 2048 bits."""
        material = self._read()
        _require_rsa_strength(material, public=False, setting="the JWT issuer signing key")
        return material

    def verifier_key(
        self, algorithm: str, kid: str | None, previous: Mapping[str, str]
    ) -> _PublicKeySet:
        """The derived public half under *kid*, plus the keys of a rotation window."""
        return _published_keys(self.load(algorithm), kid, previous)


class PrivateKeyFile(_AsymmetricKey, frozen=True, tag="private_key_file", tag_field="type"):
    """An asymmetric signing key read from a PEM file at *path*.

    Attributes:
        path: Filesystem path of the PEM private key. The path, never the key:
            any serializer emits a struct's fields verbatim.
    """

    path: str

    def _read(self) -> str:
        # An OS error is never chained, since it carries the path.
        return _read_key_file(self.path, setting="the JWT issuer signing key")


class ManagedPrivateKey(_AsymmetricKey, frozen=True, tag="managed_private_key", tag_field="type"):
    """An asymmetric signing key resolved from a managed store, never from disk.

    Resolved once, when the issuer loads it, so rotating the stored value takes
    a restart. The Secrets Manager resolver navigates dots as JSON paths, so
    the key name must not contain ``.``.

    Attributes:
        ref: ``"<resolver>:<key>"`` with resolver ``secrets`` or ``ssm``,
            for example ``"secrets:/myapp/prod/jwt-signing-key"``.
        region: AWS region the resolver is built with; boto3's own chain when
            ``None``.
    """

    ref: str
    region: str | None = None

    def __post_init__(self) -> None:
        _require_valid_key_ref(self.ref)

    def _read(self) -> str:
        # A resolver error is chained and never carries the material.
        return _fetch_key_ref(self.ref, region=self.region)


class SharedSecretFile(
    LoomFrozenStruct,
    frozen=True,
    tag="shared_secret_file",
    tag_field="type",
    forbid_unknown_fields=True,
):
    """A symmetric secret read from *path*, at least as long as its algorithm's hash.

    Signer and verifier share it, so every service that verifies can also mint.
    Naming this case is the deliberate choice that accepts it.

    Attributes:
        path: Filesystem path of the secret.
    """

    path: str

    def check(self, algorithm: str) -> None:
        """Refuse an algorithm this key cannot sign with."""
        _require_family(
            algorithm,
            symmetric=True,
            case="a SharedSecretFile",
            instead="a PrivateKeyFile or ManagedPrivateKey",
        )

    def load(self, algorithm: str) -> str:
        """Read the secret and refuse it when shorter than *algorithm*'s hash."""
        return _load_secret(self.path, algorithm, setting="the JWT issuer secret")

    def verifier_key(
        self, algorithm: str, kid: str | None, previous: Mapping[str, str]
    ) -> _SharedSecret:
        """The same secret file: a shared key has no public half to publish."""
        if previous:
            raise ConfigError("previous_keys apply to an asymmetric issuer only.")
        return _SharedSecret(self.path)


SigningKey = PrivateKeyFile | ManagedPrivateKey | SharedSecretFile
"""Where an issuer's signing key comes from: one type per case, each with only its own settings."""


def _published_keys(
    private_pem: str, kid: str | None, previous: Mapping[str, str]
) -> _PublicKeySet:
    """The derived public key under *kid*, plus the earlier keys of a rotation window."""
    if kid is None:
        raise ConfigError("for_issuer needs the issuer 'kid' to publish its public key.")
    if kid in previous:
        raise ConfigError(
            f"previous_keys reuses the issuer kid {kid!r}; the derived key cannot be overridden."
        )
    return _PublicKeySet({kid: _derived_public_key(private_pem), **previous})


class JwtIssuerConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Validated settings for :class:`~loom.rest.auth.JwtIssuer`.

    Separate from :class:`JwtAuthConfig` on purpose, and not a few extra fields
    on it: a service that only verifies must have no configuration path through
    which signing material can land in its process.  The fields diverge anyway —
    one algorithm instead of an allowlist, ``audience`` and ``issuer`` mandatory
    rather than optional, a lifetime the verifier has no use for.

    The key itself is never a field: *signing_key* names where it lives, and it
    is read when the issuer is built. ``__repr__`` redaction would not be
    enough: this is a ``msgspec.Struct``, so ``msgspec.json.encode`` and
    ``to_builtins`` emit every field verbatim.

    Attributes:
        signing_key: Where the signing key comes from, as one of
            :class:`PrivateKeyFile`, :class:`ManagedPrivateKey` or
            :class:`SharedSecretFile`. Each case carries only its own settings.
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
        ConfigError: If the signing key does not fit the algorithm, or the
            algorithm, audience, issuer, roles claim or lifetime are invalid.

    Note:
        Built programmatically, not bound from a config section: issuing happens in
        an application use case that injects the issuer, not in the REST layer that
        owns ``app.rest.auth``. Resolve the values however the service resolves its
        own settings.

    Example::

        config = JwtIssuerConfig(
            signing_key=PrivateKeyFile(os.environ["JWT_SIGNING_KEY_PATH"]),
            algorithm="EdDSA",
            audience="my-api",
            issuer="my-gateway",
            roles_claim="loom_sql_roles",
            ttl_seconds=900,
            kid="2026-08",
        )
    """

    signing_key: SigningKey
    algorithm: str
    audience: str
    issuer: str
    roles_claim: str
    ttl_seconds: int = 900
    kid: str | None = None
    typ: str | None = None

    def __post_init__(self) -> None:
        # The algorithm first: a key that does not fit an unknown one says nothing useful.
        _require_supported(self.algorithm, setting="JWT issuer 'algorithm'")
        self.signing_key.check(self.algorithm)
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

    def load_signing_key(self) -> str:
        """Load the signing key, so it lives in the issuer and not in the config.

        Returns:
            The key material, read from disk or resolved from the managed store.

        Raises:
            ConfigError: If the file cannot be read or is not UTF-8 text (the
                cause is never chained: an OS error carries the path, and a
                ``UnicodeDecodeError`` carries the file contents on
                ``exc.object``), or if the managed store cannot answer (that
                cause is chained: resolver errors never carry the material),
                or if a shared secret is shorter than its algorithm's hash.
        """
        return self.signing_key.load(self.algorithm)


def _derived_public_key(private_pem: str) -> str:
    """The PEM public half of a private key; only it leaves this function.

    Raises:
        ConfigError: When cryptography is missing or the key cannot be parsed;
            the parser error is chained and never carries the material.
    """
    try:
        from cryptography.exceptions import UnsupportedAlgorithm  # noqa: PLC0415
        from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - the jwt extra ships it
        raise ConfigError(
            "cryptography is required to derive the JWT public key. "
            "Install it with: pip install loom-kernel[jwt]"
        ) from exc
    try:
        private = serialization.load_pem_private_key(private_pem.encode(), password=None)
        public = private.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    except (ValueError, TypeError, UnsupportedAlgorithm) as exc:
        raise ConfigError("Could not derive the JWT public key from the signing key.") from exc
    return public.decode()


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
            "A ManagedPrivateKey 'ref' must be '<resolver>:<key>' with resolver "
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
