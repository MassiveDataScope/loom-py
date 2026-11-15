"""SQL wiring for the FastAPI composition root."""

from __future__ import annotations

import warnings

from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.sql import SqlConfig
from loom.core.sql.config import reject_retired_sql_keys, roles_need_identity_binding
from loom.rest.auth import Authenticator
from loom.rest.fastapi._auto_auth import _AuthWiring
from loom.rest.fastapi.sql import _connection_mechanism, _role_exposure_notice, _roles_mechanism


def _check_sql_section(ctx: ConfigContext, auth: _AuthWiring) -> None:
    """Refuse an ``sql:`` section whose endpoints the authentication cannot guard."""
    reject_retired_sql_keys(ctx.raw(ConfigKey.SQL))
    sql_cfg = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    if sql_cfg is not None:
        _validate_sql_endpoint_auth(sql_cfg, auth)


def _validate_sql_endpoint_auth(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Enforce the startup gates of every mounted identity-bound endpoint."""
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled or not endpoint.binds_identity:
            continue
        subject = f"SQL connection {name!r}"
        authenticator = auth.require_authenticator(subject)
        auth.require_jwt_audience(subject)
        _require_role_binding(name, connection.allowed_roles, authenticator)
        auth.refuse_excluded(subject, endpoint.mount_path(name))


def _require_role_binding(
    name: str,
    allowed_roles: tuple[str, ...],
    authenticator: Authenticator,
) -> None:
    """Refuse a mounted multi-role endpoint whose mechanism binds no role to the caller."""
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


def _warn_sql_endpoints(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Emit the startup warnings for enabled SQL endpoints."""
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
        path = endpoint.mount_path(name)
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
    """Name the JWT claim carrying the roles, when the JWT mechanism is in use."""
    jwt_cfg = auth.jwt_config
    if bound_mechanism is None or jwt_cfg is None or jwt_cfg.roles_claim is None:
        return ""
    return f" (JWT roles claim: {jwt_cfg.roles_claim!r})"
