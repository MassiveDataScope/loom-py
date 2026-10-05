"""Contract every authentication mechanism implements.

The framework never learns what a token is: it hands an
:class:`Authenticator` the credentials of one request and receives an
:class:`~loom.core.identity.identity.Identity` or a refusal.  A JWT, a mutual
TLS certificate, a signed header or an opaque session all fit the same shape.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from loom.core.errors import LoomError
from loom.core.errors.codes import ErrorCode
from loom.core.identity import Identity


@dataclass(frozen=True, slots=True)
class RequestCredentials:
    """What an authenticator is allowed to look at, free of any ASGI type.

    Deliberately narrow: everything an authentication mechanism may legitimately
    read, and nothing that would let it reach into request handling.  The body
    is absent on purpose — authenticating on it would require buffering the
    request before deciding whether the caller exists.

    Attributes:
        headers: Request headers keyed by lowercase name.
        path: Request path, so a mechanism can scope itself per route.
        client_host: Peer address when the server exposes one, else ``None``.

    Example::

        credentials = RequestCredentials(
            headers={"authorization": "Bearer ..."},
            path="/sql/analytics",
        )
    """

    headers: Mapping[str, str]
    path: str
    client_host: str | None = None

    def __post_init__(self) -> None:
        """Freeze the header mapping so an authenticator cannot rewrite it."""
        lowered = {name.lower(): value for name, value in self.headers.items()}
        object.__setattr__(self, "headers", MappingProxyType(lowered))

    def header(self, name: str) -> str | None:
        """Return a header value by case-insensitive name.

        Args:
            name: Header name in any casing.

        Returns:
            The header value, or ``None`` when the header is absent.
        """
        return self.headers.get(name.lower())


class AuthenticationUnavailable(LoomError):
    """Raised by an authenticator that cannot decide on the credentials right now.

    An outage of something the mechanism depends on — an identity provider's
    key set, a session store, a revocation list — is neither a refusal nor a
    bug.  :class:`~loom.rest.auth.middleware.AuthenticationMiddleware` answers
    it with ``503 service_unavailable``: a ``401`` would make clients discard
    credentials that are still valid, and a ``500`` would report a bug where
    there is an outage.
    Any other exception an authenticator raises keeps propagating unchanged.

    Raised from an authenticator, the message goes to the server log, never to
    the caller, so it may name the unreachable dependency; raised anywhere
    else, :class:`~loom.rest.errors.HttpErrorMapper` answers ``503`` with it,
    like any :class:`~loom.core.errors.LoomError`.  It must never carry the
    credential.  Chain the underlying error with ``raise ... from exc`` to keep
    its traceback.

    Args:
        message: Operator-facing description of what is unavailable.

    Example::

        try:
            keys = await self._jwks.fetch()
        except httpx.HTTPError as exc:
            raise AuthenticationUnavailable("issuer key set unreachable") from exc
    """

    def __init__(self, message: str = "Authentication is temporarily unavailable") -> None:
        super().__init__(message, code=ErrorCode.SERVICE_UNAVAILABLE)


@runtime_checkable
class Authenticator(Protocol):
    """Turns the credentials of one request into a verified identity.

    Implementations must be stateless with respect to the request and safe to
    share across concurrent calls: one instance serves the whole application.

    Example::

        class ApiKeyAuthenticator:
            name = "api-key"
            provides_roles = True

            async def authenticate(self, credentials):
                key = credentials.header("x-api-key")
                owner = await self._keys.owner_of(key) if key else None
                if owner is None:
                    return None
                return Identity(subject=owner.id, roles=owner.roles, mechanism=self.name)
    """

    @property
    def name(self) -> str:
        """Short label of the mechanism, recorded on every identity it issues."""
        ...

    @property
    def provides_roles(self) -> bool:
        """Whether the mechanism binds roles to the identity.

        Startup gates rely on this: an endpoint whose authorization is
        role-based refuses to mount behind a mechanism that issues none, rather
        than letting every authenticated caller pick their own privileges.
        """
        ...

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        """Verify *credentials* and return the caller they identify.

        Args:
            credentials: Headers, path and peer address of the request.

        Returns:
            The verified identity, or ``None`` to refuse the request.  The
            refusal carries no reason on purpose: the response must not become
            an oracle about which part of the credentials failed.

        Raises:
            AuthenticationUnavailable: When the credentials cannot be verified
                right now because a dependency of the mechanism is down.
        """
        ...
