"""Validated CORS settings for the REST layer.

The framework exposes CORS as configuration, not for convenience but because
the combination every tutorial suggests is unsafe and Starlette does not refuse
it: with ``allow_origins: ["*"]`` **and** ``allow_credentials: true``, Starlette
stops sending the wildcard and instead reflects the request's ``Origin`` back
together with ``Access-Control-Allow-Credentials: true``.  The wildcard silently
becomes "any origin, with cookies".

Binding the settings here makes that shape unrepresentable: it fails at config
parse, with the reason.
"""

from __future__ import annotations

from loom.core.config.errors import ConfigError
from loom.core.model import LoomFrozenStruct

WILDCARD_ORIGIN = "*"

_CREDENTIALS_WILDCARD_ERROR = (
    "CORS 'allow_origins' contains '*' together with 'allow_credentials: true'. "
    "Starlette does not reject that combination: it reflects the caller's Origin "
    "header and answers with 'Access-Control-Allow-Credentials: true', so every "
    "site becomes able to make credentialed requests to this API. List the exact "
    "origins, or turn 'allow_credentials' off."
)


class CorsConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Validated settings for the CORS middleware.

    Binds from the ``app.rest.cors`` config section.  The section is optional:
    without it no CORS middleware is mounted and the application behaves exactly
    as before.

    Attributes:
        allow_origins: Exact origins allowed to call the API.  ``["*"]`` is
            only accepted when ``allow_credentials`` is off.
        allow_origin_regex: Regular expression matching allowed origins.
            Anchored by Starlette; keep it as narrow as the origin list you
            would otherwise write.
        allow_methods: HTTP methods allowed in cross-origin requests.
        allow_headers: Request headers the caller may send.
        allow_credentials: Whether cookies and ``Authorization`` may be sent
            cross-origin.  Mutually exclusive with a wildcard origin.
        expose_headers: Response headers the browser may read.
        max_age: Seconds a preflight response may be cached.

    Raises:
        ConfigError: When a wildcard origin is combined with credentials.

    Example YAML::

        app:
          rest:
            cors:
              allow_origins: ["https://app.example.com"]
              allow_credentials: true
              allow_methods: [GET, POST]
    """

    allow_origins: tuple[str, ...] = ()
    allow_origin_regex: str | None = None
    allow_methods: tuple[str, ...] = ("GET",)
    allow_headers: tuple[str, ...] = ()
    allow_credentials: bool = False
    expose_headers: tuple[str, ...] = ()
    max_age: int = 600

    def __post_init__(self) -> None:
        if self.allow_credentials and WILDCARD_ORIGIN in self.allow_origins:
            raise ConfigError(_CREDENTIALS_WILDCARD_ERROR)
