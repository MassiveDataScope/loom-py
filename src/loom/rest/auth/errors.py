"""The errors an authenticator raises; free of any ASGI or framework type."""

from __future__ import annotations

from loom.core.errors import LoomError
from loom.core.errors.codes import ErrorCode


class AuthenticationUnavailable(LoomError):
    """Raised by an authenticator that cannot decide on the credentials right now.

    An outage of something the mechanism depends on — an identity provider's
    key set, a session store, a revocation list — is neither a refusal nor a
    bug.  :class:`~loom.rest.auth.middleware.AuthenticationMiddleware` answers
    it with ``503 service_unavailable``: a ``401`` would make clients discard
    credentials that are still valid, and a ``500`` would report a bug where
    there is an outage.
    Any other exception an authenticator raises keeps propagating unchanged.

    The message goes to the server log, never to the caller, so it may name
    the unreachable dependency; raised outside an authenticator,
    :class:`~loom.rest.errors.HttpErrorMapper` answers ``503``, like any
    ``5xx`` :class:`~loom.core.errors.LoomError`.  It must never carry the
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


__all__ = ["AuthenticationUnavailable"]
