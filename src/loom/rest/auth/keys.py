"""JWT keys: the supported algorithms and the rules every key must meet.

The one place each rule lives, shared by the verifier and the issuer so a
hardening applied to one cannot miss the other: the algorithm is supported and
of the key's family, an HMAC secret is at least its hash, an RSA key at least
2048 bits, a PEM key is never a shared secret, and a public key never carries
private material. The keyring here is what a verifier picks a token's key from.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Literal, Protocol, get_args

from loom.core.config.errors import ConfigError
from loom.core.model import LoomFrozenStruct

_FORBIDDEN_ALGORITHM = "none"

_HMAC_MINIMUM_BYTES: Final = {"HS256": 32, "HS384": 48, "HS512": 64}
"""Smallest HMAC key per algorithm: the size of its hash, a MUST of RFC 7518 §3.2."""

JwtAlgorithm = Literal[
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
]
"""An algorithm this framework signs and verifies with; ``none`` is never one."""

SUPPORTED_ALGORITHMS: frozenset[str] = frozenset(get_args(JwtAlgorithm))
"""The values of :data:`JwtAlgorithm`, checked at startup rather than on the
first request: a typo like ``" HS256"`` would otherwise fail per-call."""


_PEM_MARKER: Final = "-----BEGIN "
"""What every PEM key starts with: never the material of a shared secret."""

_MIN_RSA_BITS: Final = 2048
"""Smallest RSA modulus accepted for signing or verifying: a MUST of RFC 7518 §3.3."""


def is_symmetric(algorithm: str) -> bool:
    """Return ``True`` when *algorithm* uses a shared secret: the HMAC table is the one list."""
    return algorithm in _HMAC_MINIMUM_BYTES


def require_family(algorithm: str, *, symmetric: bool, case: str, instead: str) -> None:
    """Refuse an algorithm of the other key family; every key case shares this one rule."""
    if is_symmetric(algorithm) != symmetric:
        raise ConfigError(f"JWT algorithm {algorithm!r} needs {instead}, not {case}.")


def require_supported(algorithm: str, *, setting: str) -> None:
    """Reject ``none`` and anything outside :data:`JwtAlgorithm`, at startup.

    The type already refuses them when the settings are decoded; this covers
    the settings built in code, which a struct does not type-check.
    """
    if algorithm.lower() == _FORBIDDEN_ALGORITHM:
        raise ConfigError("The 'none' JWT algorithm is always forbidden.")
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise ConfigError(
            f"{setting} {algorithm!r} is not supported. "
            f"Choose one of: {', '.join(sorted(SUPPORTED_ALGORITHMS))}."
        )


@dataclass(frozen=True, slots=True)
class Key:
    """Key material and the only algorithms it verifies (RFC 8725 §3.1)."""

    material: str = field(repr=False)
    algorithms: tuple[str, ...]


class Keyring(Protocol):
    """The loaded keys of a verifier, picking the one that checks a token."""

    def pick(self, key_id: str | None) -> Key | None:
        """The key for a token with this ``kid``, or ``None`` when none applies."""
        ...


@dataclass(frozen=True, slots=True)
class FixedKey:
    """One shared secret: every token is checked with it, whatever its ``kid``."""

    key: Key

    def pick(self, key_id: str | None) -> Key | None:
        return self.key


@dataclass(frozen=True, slots=True)
class KeySet:
    """Keys by ``kid``.

    Selection is explicit and never exhaustive: trying every key in turn would
    decouple each algorithm from its key, which is what makes algorithm
    confusion structurally impossible here.
    """

    keys: Mapping[str, Key]

    def pick(self, key_id: str | None) -> Key | None:
        if key_id is not None:
            return self.keys.get(key_id)
        return next(iter(self.keys.values())) if len(self.keys) == 1 else None


class JwtPublicKey(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """A public key and the one algorithm it verifies (RFC 8725 §3.1).

    The token cannot choose the algorithm: it is the key's. Rotating to
    another key type is one more entry under another ``kid``.

    Attributes:
        alg: The asymmetric algorithm this key verifies.
        pem: The PEM public key, written inline or resolved by a placeholder
            such as ``${file:...}`` or ``${ssm:...}``.
    """

    alg: JwtAlgorithm
    pem: str

    def __post_init__(self) -> None:
        require_supported(self.alg, setting="JWT public key 'alg'")
        require_family(self.alg, symmetric=False, case="a public key", instead="a 'secret'")


def require_public_key(key_id: str, material: str) -> None:
    """A usable entry holding public material only: the struct must never carry a secret."""
    if not key_id.strip() or not material.strip():
        raise ConfigError("JWT auth 'public_keys' entries need a key id and material.")
    if "PRIVATE KEY" in material:
        raise ConfigError(
            f"JWT auth 'public_keys' entry {key_id!r} holds a private key; "
            "publish only its public half."
        )
    require_rsa_strength(material, public=True, setting=f"JWT auth 'public_keys' {key_id!r}")


def require_rsa_strength(material: str, *, public: bool, setting: str) -> None:
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


def require_hmac_strength(secret: str, algorithm: str, *, setting: str) -> None:
    """Refuse an HMAC key that is blank, a PEM key, or shorter than its algorithm's hash.

    RFC 7518 §3.2 makes the hash size a MUST, so the rule holds in every
    environment. The message names the size, never the key. A trailing line
    break does not count: the key is used byte for byte, so the file must
    carry the full length without it.

    Raises:
        ConfigError: When the key is blank, a PEM key, or shorter than
            *algorithm*'s hash.
    """
    minimum = _HMAC_MINIMUM_BYTES[algorithm]
    if not secret.strip():
        raise ConfigError(f"{setting} is blank.")
    if _PEM_MARKER in secret:
        # A PEM key used as an HMAC secret is algorithm confusion: whoever holds
        # the public half could sign. The key fits an asymmetric algorithm.
        raise ConfigError(f"{setting} is a PEM key; {algorithm} needs a shared secret.")
    if len(secret.rstrip("\r\n").encode("utf-8")) < minimum:
        raise ConfigError(
            f"{setting} must be at least {minimum} bytes for {algorithm} (RFC 7518 section 3.2)."
        )


def read_key_file(path: str, *, setting: str) -> str:
    """Read key material from disk, never chaining the cause.

    An ``OSError`` message carries the path and a ``UnicodeDecodeError`` carries
    the whole file on ``exc.object``: either would publish key material through a
    traceback or a structured log that serializes the exception.
    """
    try:
        return Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        raise ConfigError(f"Could not read {setting}.") from None


def require_signing_key(material: str, algorithm: str) -> None:
    """A key that can sign with *algorithm*: a long enough secret, or a strong private key."""
    setting = "the JWT issuer signing key"
    if is_symmetric(algorithm):
        require_hmac_strength(material, algorithm, setting=setting)
        return
    if _PEM_MARKER not in material or "PRIVATE KEY" not in material:
        raise ConfigError(f"{algorithm} needs a PEM private key as {setting}.")
    require_rsa_strength(material, public=False, setting=setting)


def derived_public_key(private_pem: str) -> str:
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
