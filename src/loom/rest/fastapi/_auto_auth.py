"""Authentication wiring for the FastAPI composition root."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from fastapi import FastAPI

from loom.core.config.errors import ConfigError
from loom.rest.auth import AuthenticationMiddleware, Authenticator, JwtAuthConfig, JwtAuthenticator
from loom.rest.auth.config import DEFAULT_EXCLUDE_PATHS

if TYPE_CHECKING:
    from loom.rest.fastapi.auto import _AppConfig, _RestAuthConfig


@dataclass(frozen=True)
class _AuthWiring:
    """Resolved authentication for the auto-bootstrap REST app.

    Args:
        authenticator: Mechanism authenticating callers, or ``None`` when the
            application configures none.
        exclude_paths: Paths served without authentication.
        jwt_config: JWT settings when the built-in mechanism is the one in use.
            Kept so the JWT-specific startup gates stay where the JWT contract
            is known, instead of leaking into the agnostic layers.
    """

    authenticator: Authenticator | None
    exclude_paths: tuple[str, ...]
    jwt_config: JwtAuthConfig | None


def _resolve_authentication(
    app_cfg: _AppConfig,
    authenticator: Authenticator | None,
    documentation_paths: tuple[str, ...],
) -> _AuthWiring:
    """Pick the single authentication mechanism of the application.

    Args:
        app_cfg: Parsed ``app`` section.
        authenticator: Mechanism supplied by the composition root, if any.
        documentation_paths: Effective docs/schema/metrics/health paths, used
            as the default exclusion list.

    The configuration names the mechanism, so ``loom check --deploy`` sees
    what ``create_app`` builds: ``app.rest.auth.jwt`` for the built-in one,
    ``app.rest.auth.custom: true`` for *authenticator*.

    Raises:
        ConfigError: When ``custom: true`` and ``jwt`` are both declared, or
            when *authenticator* and ``custom: true`` do not come together.
    """
    auth_cfg = app_cfg.rest.auth
    jwt_cfg = auth_cfg.jwt
    _require_declared_mechanism(auth_cfg, authenticator)
    if authenticator is not None:
        return _AuthWiring(
            authenticator=authenticator,
            exclude_paths=_effective_exclusions(auth_cfg.exclude_paths, documentation_paths),
            jwt_config=None,
        )
    if jwt_cfg is not None:
        return _AuthWiring(
            authenticator=JwtAuthenticator(jwt_cfg),
            exclude_paths=_effective_exclusions(jwt_cfg.exclude_paths, documentation_paths),
            jwt_config=jwt_cfg,
        )
    return _AuthWiring(authenticator=None, exclude_paths=(), jwt_config=None)


def _require_declared_mechanism(
    auth_cfg: _RestAuthConfig, authenticator: Authenticator | None
) -> None:
    """Refuse a mechanism the configuration does not name, or names twice."""
    if auth_cfg.custom and auth_cfg.jwt is not None:
        raise ConfigError(
            "'app.rest.auth.custom: true' and 'app.rest.auth.jwt' are both configured. "
            "Exactly one authentication mechanism may be active: drop one of them."
        )
    if authenticator is not None and not auth_cfg.custom:
        raise ConfigError(
            "create_app received an 'authenticator' but 'app.rest.auth.custom' is not "
            "true. Set 'app.rest.auth.custom: true' so the configuration says "
            "authentication is supplied in code, and drop any 'app.rest.auth.jwt'."
        )
    if auth_cfg.custom and authenticator is None:
        raise ConfigError(
            "'app.rest.auth.custom' is true but create_app received no 'authenticator': "
            "pass create_app(authenticator=...) or drop 'custom: true'."
        )


def _effective_exclusions(
    configured: tuple[str, ...],
    documentation_paths: tuple[str, ...],
) -> tuple[str, ...]:
    """Resolve which paths are served without authentication.

    Left at its default, the exclusion list follows the paths the application
    actually publishes: a hardcoded ``/docs`` excludes nothing when the operator
    moved Swagger elsewhere, while still opening a hole if a route captures it.
    An explicit list is honoured as written.
    """
    if configured != DEFAULT_EXCLUDE_PATHS:
        return tuple(dict.fromkeys(configured))
    return documentation_paths


def _require_jwt_audience(subject: str, jwt_cfg: JwtAuthConfig) -> None:
    """Binding an endpoint to a token is void without a validated ``aud`` (§4).

    Args:
        subject: What is being gated, already formatted for the message — for
            example ``"SQL connection 'analytics'"`` or ``"Agent 'analyst'"``.
        jwt_cfg: JWT settings of the application.
    """
    if jwt_cfg.audience is not None:
        return
    raise ConfigError(
        f"{subject}: the endpoint requires a verified caller but "
        "'app.rest.auth.jwt.audience' is not set. Without a validated 'aud' any "
        "token signed by the same key — including tokens minted for another "
        "service — would be accepted as that verified caller, with whatever "
        "claims it carries."
    )


def _mount_authentication(app: FastAPI, auth: _AuthWiring) -> None:
    """Mount :class:`AuthenticationMiddleware` when a mechanism is configured.

    Mounted before the optional middlewares so ``TraceIdMiddleware`` wraps it
    and 401 bodies carry a trace id. A missing ``pyjwt`` extra surfaces at app
    startup as an ``ImportError`` with an install hint (fail-closed).
    """
    if auth.authenticator is None:
        return
    app.add_middleware(
        AuthenticationMiddleware,
        authenticator=auth.authenticator,
        exclude_paths=auth.exclude_paths,
    )
