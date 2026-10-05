"""Authentication for the Loom REST layer.

The layer is mechanism-agnostic: :class:`AuthenticationMiddleware` drives any
:class:`Authenticator` and publishes the resulting
:class:`~loom.core.identity.identity.Identity` for the duration of the
request.  :class:`JwtAuthenticator` and its ready-made
:class:`JwtAuthMiddleware` are the batteries-included implementation, bound
from the ``app.rest.auth.jwt`` config section.

Install the optional JWT dependency with::

    pip install "loom-kernel[jwt]"
"""

from loom.rest.auth.abc import AuthenticationUnavailable, Authenticator, RequestCredentials
from loom.rest.auth.config import JwtAuthConfig, JwtIssuerConfig
from loom.rest.auth.jwt import JwtAuthenticator, JwtIssuer
from loom.rest.auth.middleware import AuthenticationMiddleware, JwtAuthMiddleware

__all__ = [
    "AuthenticationMiddleware",
    "AuthenticationUnavailable",
    "Authenticator",
    "JwtAuthConfig",
    "JwtAuthMiddleware",
    "JwtAuthenticator",
    "JwtIssuer",
    "JwtIssuerConfig",
    "RequestCredentials",
]
