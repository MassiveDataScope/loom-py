"""JWT keys: the supported algorithms and the rules every key must meet.

The one place each rule lives, shared by the verifier and the issuer so a
hardening applied to one cannot miss the other: the algorithm is supported and
of the key's family, an HMAC secret is at least its hash, an RSA key at least
2048 bits, a PEM key is never a shared secret, and a public key never carries
private material. The keyring here is what a verifier picks a token's key from.
"""

from __future__ import annotations

import base64
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import ModuleType
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

_PRIVATE_PEM: Final = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
"""The header of a PEM private key, matched as a header: the words alone may
sit in an OpenSSH public key's free-text comment."""

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
    """Key material and the only algorithms it verifies (RFC 8725 §3.1).

    ``material`` is the shared secret for HS*, or the public key already parsed
    and checked at startup: the JWT library verifies with that object and never
    parses the text a second time, so what was checked is what verifies.
    """

    material: Any = field(repr=False)
    algorithms: tuple[str, ...]


class Keyring(Protocol):
    """The loaded keys of a verifier, picking the one that checks a token."""

    def pick(self, key_id: str | None, /) -> Key | None:
        """The key for a token with this ``kid``, or ``None`` when none applies."""
        ...


@dataclass(frozen=True, slots=True)
class FixedKey:
    """One shared secret: every token is checked with it, whatever its ``kid``."""

    key: Key

    def pick(self, _key_id: str | None, /) -> Key | None:
        return self.key


@dataclass(frozen=True, slots=True)
class KeySet:
    """Keys by ``kid``.

    Selection is explicit and never exhaustive: trying every key in turn would
    decouple each algorithm from its key, which is what makes algorithm
    confusion structurally impossible here.
    """

    keys: Mapping[str, Key]

    def pick(self, key_id: str | None, /) -> Key | None:
        if key_id is not None:
            # A list or object ``kid`` is unhashable: refuse it here rather
            # than rely on the JWT library to have rejected it first.
            return self.keys.get(key_id) if isinstance(key_id, str) else None
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
        # Refused here too, not only by the config holding it: ``pem`` is plain
        # text, so this object must never exist with private material in it.
        if _PRIVATE_PEM.search(self.pem):
            raise ConfigError("A JWT public key holds a private key; publish only its public half.")


def public_key(key_id: str, material: str, algorithms: tuple[str, ...]) -> Key:
    """The key with only those of *algorithms* it can verify with, checked at startup.

    A PEM-text entry shares one ``algorithms`` list with keys of other types,
    so each keeps the algorithms of its own type, never another's; a
    ``{alg, pem}`` entry brings exactly one. A key that fits none, or does not
    parse, would only fail on the first request: every valid token refused.
    The message names the key id and the key type, never the material.
    """
    setting = f"JWT auth 'public_keys' entry {key_id!r}"
    if not key_id.strip() or not material.strip():
        raise ConfigError("JWT auth 'public_keys' entries need a key id and material.")
    if _PRIVATE_PEM.search(material):
        raise ConfigError(f"{setting} holds a private key; publish only its public half.")
    parsed = _public_key(material, setting=setting)
    return Key(parsed, _fitting(parsed, algorithms, setting))


def _fitting(key: Any, algorithms: tuple[str, ...], setting: str) -> tuple[str, ...]:
    """Those of *algorithms* that verify with this kind of key; an RSA one must be strong."""
    kind = _key_kind(key)
    fitting = tuple(alg for alg in algorithms if _ALGORITHM_KEY_KIND[alg] == kind)
    if not fitting:
        raise ConfigError(
            f"{setting} is a {kind} key; none of {list(algorithms)} verifies with it."
        )
    if kind == "RSA":
        _require_rsa_bits(key.key_size, setting=setting)
    return fitting


_ALGORITHM_KEY_KIND: Final = {
    **dict.fromkeys(("RS256", "RS384", "RS512", "PS256", "PS384", "PS512"), "RSA"),
    "ES256": "P-256",
    "ES384": "P-384",
    "ES512": "P-521",
    "EdDSA": "OKP",
}
"""The key each asymmetric algorithm verifies with (RFC 7518 §3, RFC 8037)."""

_CURVE_KIND: Final = {"secp256r1": "P-256", "secp384r1": "P-384", "secp521r1": "P-521"}

_SSH_PUBLIC_PREFIXES: Final = ("ssh-", "ecdsa-sha2-")
"""OpenSSH public key lines a JWT can verify with, accepted next to PEM as the
released verifier did. FIDO ``sk-`` keys sign another message format and never
verify a JWT, so they are not among them."""

_SSH_PREFIXES: Final = (*_SSH_PUBLIC_PREFIXES, "sk-")
"""Every OpenSSH key line: none of them is ever a shared secret."""


def _attempt(load: Any, data: bytes) -> Any | None:
    """What *load* parses from *data*, or ``None`` when it refuses it.

    The one place a parser error is swallowed: it is never chained nor kept as
    context, since parser messages and ``UnicodeDecodeError.object`` can carry
    key material. Callers raise their own error, outside any handler.
    """
    from cryptography.exceptions import UnsupportedAlgorithm  # noqa: PLC0415

    try:
        return load(data)
    except (ValueError, TypeError, UnsupportedAlgorithm):
        return None


def _public_key(material: str, *, setting: str) -> Any:
    """The parsed PEM or OpenSSH public key."""
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    text = material.strip()
    load = (
        serialization.load_ssh_public_key
        if text.startswith(_SSH_PUBLIC_PREFIXES)
        else serialization.load_pem_public_key
    )
    parsed = _attempt(load, text.encode())
    if parsed is None:
        raise ConfigError(f"{setting} is not a PEM or OpenSSH public key.")
    return parsed


def _key_kind(key: Any) -> str:
    """``RSA``, the curve of an EC key, ``OKP`` for Ed25519/Ed448, or the type name."""
    from cryptography.hazmat.primitives.asymmetric import ec, ed448, ed25519, rsa  # noqa: PLC0415

    if isinstance(key, rsa.RSAPublicKey):
        return "RSA"
    if isinstance(key, ec.EllipticCurvePublicKey):
        return _CURVE_KIND.get(key.curve.name, key.curve.name)
    if isinstance(key, (ed25519.Ed25519PublicKey, ed448.Ed448PublicKey)):
        return "OKP"
    return type(key).__name__


def _require_rsa_bits(bits: int, *, setting: str) -> None:
    """Refuse an RSA key below 2048 bits (RFC 7518 §3.3), naming the size, never the key."""
    if bits < _MIN_RSA_BITS:
        raise ConfigError(
            f"{setting} is a {bits}-bit RSA key; at least {_MIN_RSA_BITS} bits "
            "are required (RFC 7518 section 3.3)."
        )


def _private_key(material: str, *, setting: str) -> Any:
    """The parsed, unencrypted PEM private key."""
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    parsed = _attempt(
        lambda pem: serialization.load_pem_private_key(pem, password=None), material.encode()
    )
    if parsed is None:
        raise ConfigError(f"{setting} is not an unencrypted PEM private key.")
    return parsed


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
    if _is_key_material(secret):
        # A key used as an HMAC secret is algorithm confusion: whoever holds
        # the public half could sign. The key fits an asymmetric algorithm.
        raise ConfigError(
            f"{setting} is a key (PEM, OpenSSH, DER or JWK), not a shared secret; "
            f"{algorithm} needs the raw secret."
        )
    if len(secret.rstrip("\r\n").encode("utf-8")) < minimum:
        raise ConfigError(
            f"{setting} must be at least {minimum} bytes for {algorithm} (RFC 7518 section 3.2)."
        )


def _is_key_material(value: str) -> bool:
    """A PEM block, an OpenSSH key line, a JWK or a base64 DER key: never a shared secret."""
    text = value.strip()
    return (
        _PEM_MARKER in text
        or text.startswith(_SSH_PREFIXES)
        or (text.startswith("{") and '"kty"' in text)
        or _is_der_key(text)
    )


def _is_der_key(text: str) -> bool:
    """Whether *text* is a key or certificate in bare base64 DER.

    Identity providers publish public keys this way, with no PEM header
    (Keycloak's realm ``public_key``). A random base64 secret decodes to bytes
    no DER parser accepts, so it is never mistaken for one.
    """
    der = _base64_bytes("".join(text.split()))
    return der is not None and any(_attempt(load, der) is not None for load in _der_loaders())


def _base64_bytes(text: str) -> bytes | None:
    """*text* decoded as standard or URL-safe base64, padded or not, else ``None``."""
    padded = text + "=" * (-len(text) % 4)
    try:
        return base64.b64decode(padded, validate=True)
    except ValueError:  # binascii.Error is a ValueError
        pass
    if not _URLSAFE_BASE64.fullmatch(padded):
        return None
    return base64.urlsafe_b64decode(padded)


_URLSAFE_BASE64: Final = re.compile(r"[A-Za-z0-9_-]+={0,2}")


def _der_loaders() -> tuple[Any, ...]:
    from cryptography import x509  # noqa: PLC0415
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    return (
        serialization.load_der_public_key,
        _load_der_private,
        x509.load_der_x509_certificate,
    )


def _load_der_private(der: bytes) -> object:
    """A DER private key; an encrypted one counts as a key, not as random bytes."""
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    try:
        return serialization.load_der_private_key(der, password=None)
    except TypeError:  # encrypted: a key all the same, only the password is missing
        return der


def require_signing_key(material: str, algorithm: str) -> None:
    """A key that can sign with *algorithm*: a long enough secret, or a strong private key."""
    setting = "the JWT issuer signing key"
    if is_symmetric(algorithm):
        require_hmac_strength(material, algorithm, setting=setting)
        return
    if not _PRIVATE_PEM.search(material):
        raise ConfigError(f"{algorithm} needs a PEM private key as {setting}.")
    # The verifiers' rule, applied to the public half: key type, EC curve and
    # RSA size all fit the one algorithm, so the issuer cannot mint tokens
    # that loom's own verifier (or any RFC 7518 one) would refuse.
    _fitting(_private_key(material, setting=setting).public_key(), (algorithm,), setting)


def derived_public_key(private_pem: str) -> str:
    """The PEM public half of a private key; only it leaves this function.

    Raises:
        ConfigError: When the key cannot be parsed; the parser error is
            dropped, never chained or kept as context.
    """
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    public = _private_key(private_pem, setting="the JWT issuer signing key").public_key()
    pem: bytes = public.public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return pem.decode()


_PYJWT_HINT: Final = (
    "JWT authentication requires the optional dependency 'pyjwt'. "
    "Install it with: pip install 'loom-kernel[jwt]'"
)


def load_pyjwt() -> ModuleType:
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
