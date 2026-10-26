"""SQL wiring for the FastAPI composition root."""

from __future__ import annotations

import warnings

from loom.composition._sql import SqlWiring, resolve_sql
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.sql import SqlConfig
from loom.core.sql.config import roles_need_identity_binding
from loom.rest.auth import Authenticator
from loom.rest.fastapi._auto_auth import _AuthWiring, _require_jwt_audience
from loom.rest.fastapi.sql import _connection_mechanism, _role_exposure_notice, _roles_mechanism


def _resolve_sql(ctx: ConfigContext, auth: _AuthWiring) -> SqlWiring:
    """Load the optional ``sql:`` section into its registry and service.

    Absent section → no registry and the null service, keeping
    ``SqlQueryService`` always resolvable with an actionable error (spec M5).
    Present section → startup auth gate (spec §4) plus a registry whose
    connections only open inside the app lifespan.
    """
    sql = resolve_sql(ctx)
    if sql.config is not None:
        _validate_sql_endpoint_auth(sql.config, auth)
    return sql


def _validate_sql_endpoint_auth(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Enforce the startup gates of every mounted identity-bound endpoint (§4)."""
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled or not endpoint.binds_identity:
            continue
        authenticator = _require_authenticator(name, auth)
        if auth.jwt_config is not None:
            _require_jwt_audience(f"SQL connection {name!r}", auth.jwt_config)
        _require_role_binding(name, connection.allowed_roles, authenticator)
        _require_authenticated_path(name, endpoint.path or f"/sql/{name}", auth.exclude_paths)


def _require_authenticator(name: str, auth: _AuthWiring) -> Authenticator:
    """An identity-bound endpoint without a mechanism has no identity to bind (§4)."""
    if auth.authenticator is not None:
        return auth.authenticator
    raise ConfigError(
        f"SQL connection {name!r}: sql_endpoint.auth requires a verified caller but the "
        "application configures no authentication. Add the 'app.rest.auth.jwt' section, "
        "pass create_app(authenticator=...), or switch the endpoint to auth: external."
    )


def _require_role_binding(
    name: str,
    allowed_roles: tuple[str, ...],
    authenticator: Authenticator,
) -> None:
    """A mounted multi-role endpoint must bind its roles to the identity (§4)."""
    if not roles_need_identity_binding(
        allowed_roles, mechanism_binds_roles=authenticator.provides_roles
    ):
        return
    raise ConfigError(
        f"SQL connection {name!r}: 'allowed_roles' is not empty but the "
        f"{authenticator.name!r} authentication mechanism binds no role to the caller "
        "identity. Without that binding the endpoint would let any authenticated caller "
        "pick any allowlisted role. Either configure the mechanism to provide roles "
        "(for JWT: 'app.rest.auth.jwt.roles_claim'), or leave 'allowed_roles' empty and "
        "pin a single 'default_role'."
    )


def _require_authenticated_path(name: str, path: str, exclude_paths: tuple[str, ...]) -> None:
    """A mounted SQL path listed in the exclusions would bypass auth (§4)."""
    if path not in exclude_paths:
        return
    raise ConfigError(
        f"SQL connection {name!r}: the endpoint path {path!r} is listed in the "
        "authentication 'exclude_paths', which would serve SQL without "
        "authentication. Remove it from the exclusion list."
    )


def _warn_sql_endpoints(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Emit the spec §4 startup warnings for enabled SQL endpoints."""
    mechanism = _roles_mechanism(auth.authenticator)
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled:
            continue
        if not connection.readonly:
            warnings.warn(
                f"SQL connection {name!r} has an enabled endpoint with 'readonly: false' — "
                "callers can mutate data through it. Ensure this is intentional.",
                stacklevel=3,
            )
        if endpoint.auth is None:
            continue
        path = endpoint.path or f"/sql/{name}"
        # Narrowed per connection: a global mechanism does not bind the roles of
        # a connection whose allowlist is empty (see _connection_mechanism).
        bound = _connection_mechanism(connection, mechanism)
        warnings.warn(
            f"SQL endpoint mounted at {path} (connection={name!r}, "
            f"readonly={connection.readonly}, auth={endpoint.auth}, "
            f"allowed_roles={len(connection.allowed_roles)}). "
            "'auth' only authenticates the caller; the roles it may use come from the "
            "identity binding: "
            f"{_role_exposure_notice(bound, len(connection.allowed_roles))}"
            f"{_roles_source_detail(auth, bound)}.",
            stacklevel=3,
        )


def _roles_source_detail(auth: _AuthWiring, bound_mechanism: str | None) -> str:
    """Name the JWT claim carrying the roles, when the JWT mechanism is in use.

    The agnostic layers only know "the mechanism binds roles"; the composition
    root knows which claim, and operators need that to debug a denied caller.
    """
    jwt_cfg = auth.jwt_config
    if bound_mechanism is None or jwt_cfg is None or jwt_cfg.roles_claim is None:
        return ""
    return f" (JWT roles claim: {jwt_cfg.roles_claim!r})"
