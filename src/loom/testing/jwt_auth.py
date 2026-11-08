"""A built-in JWT setup that already meets loom's hardening, for tests.

``jwt_test_auth`` writes a fresh 32-byte HS256 secret and returns the matching
:class:`~loom.rest.auth.JwtAuthConfig`, with an audience and an issuer, plus a
minter for tokens that config accepts. A test then never trips the key-size,
audience or issuer rules, and never commits a weak secret to the repository.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

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
        secret: The 32-byte key, hex-encoded, as written to ``config.secret_path``.
    """

    config: JwtAuthConfig
    secret: str

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
        return jwt.encode(payload, self.secret, algorithm=_ALGORITHM)


def jwt_test_auth(directory: Path, **overrides: Any) -> JwtTestAuth:
    """Write a fresh HS256 secret under *directory* and return its compliant setup.

    Args:
        directory: Where the secret file goes, usually pytest's ``tmp_path``.
        **overrides: ``JwtAuthConfig`` fields to change, such as ``roles_claim``.

    Returns:
        The config and the secret that signs its tokens.
    """
    secret = secrets.token_hex(32)
    path = directory / "loom-test-jwt.key"
    path.write_text(secret, encoding="utf-8")
    settings: dict[str, Any] = {
        "secret_path": str(path),
        "algorithms": (_ALGORITHM,),
        "audience": TEST_AUDIENCE,
        "issuer": TEST_ISSUER,
        **overrides,
    }
    return JwtTestAuth(config=JwtAuthConfig(**settings), secret=secret)
