"""A built-in JWT setup that already meets loom's hardening, for tests.

``jwt_test_auth`` makes a fresh 32-byte HS256 secret and returns the matching
:class:`~loom.rest.auth.JwtAuthConfig`, with an audience and an issuer, plus a
minter for tokens that config accepts. A test then never trips the key-size,
audience or issuer rules, and never commits a weak secret to the repository.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Final

from loom.core.model.redaction import Secret
from loom.rest.auth.config import JwtAuthConfig

TEST_AUDIENCE: Final = "loom-test-api"
"""Audience every token of :func:`jwt_test_auth` is minted for."""

TEST_ISSUER: Final = "loom-test-issuer"
"""Issuer every token of :func:`jwt_test_auth` names."""

_ALGORITHM = "HS256"


@dataclass(frozen=True, slots=True)
class JwtTestAuth:
    """A compliant JWT config and the secret that signs its tokens.

    Attributes:
        config: Verifier settings: HS256, the test audience and issuer.
        secret: The 32-byte key, hex-encoded, held by ``config.secret``.
    """

    config: JwtAuthConfig
    secret: str = field(repr=False)

    def token(self, subject: str, *, ttl_seconds: int = 300, **claims: Any) -> str:
        """Mint a token the config accepts.

        Args:
            subject: The ``sub`` claim.
            ttl_seconds: Seconds until ``exp``.
            **claims: Extra claims, such as the roles claim.

        Returns:
            The encoded token, for an ``Authorization: Bearer`` header.
        """
        import jwt  # noqa: PLC0415 - the ``rest`` extra; loaded only when a test mints

        now = int(time.time())
        payload = {
            "sub": subject,
            "aud": self.config.audience,
            "iss": self.config.issuer,
            "iat": now,
            "exp": now + ttl_seconds,
            **claims,
        }
        return jwt.encode(payload, self.secret, algorithm=self.config.algorithms[0])


def jwt_test_auth(**overrides: Any) -> JwtTestAuth:
    """Make a fresh HS256 secret and return its compliant setup.

    Args:
        **overrides: ``JwtAuthConfig`` fields to change, such as ``roles_claim``.
            The key fields and ``algorithms`` are the helper's own.

    Returns:
        The config and the secret that signs its tokens.
    """
    owned = {"secret", "secret_path", "public_keys", "algorithms"} & overrides.keys()
    if owned:
        raise TypeError(f"jwt_test_auth writes its own {sorted(owned)}; do not override them")
    secret = secrets.token_hex(32)
    settings: dict[str, Any] = {
        "secret": Secret(secret),
        "algorithms": (_ALGORITHM,),
        "audience": TEST_AUDIENCE,
        "issuer": TEST_ISSUER,
        **overrides,
    }
    return JwtTestAuth(config=JwtAuthConfig(**settings), secret=secret)
