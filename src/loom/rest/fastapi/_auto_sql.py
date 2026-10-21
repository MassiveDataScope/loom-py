"""SQL wiring for the FastAPI composition root."""

from __future__ import annotations

import warnings
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import (
    CallerBoundSql,
    NullSqlQueryService,
    SqlConfig,
    SqlExecutor,
    SqlQueryService,
)
from loom.core.sql.config import roles_need_identity_binding
from loom.rest.auth import Authenticator
from loom.rest.fastapi._auto_auth import _AuthWiring, _require_jwt_audience
from loom.rest.fastapi.sql import _connection_mechanism, _role_exposure_notice, _roles_mechanism

if TYPE_CHECKING:
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry


@dataclass(frozen=True)
class _SqlWiring:
    """Resolved SQL subsystem pieces for the auto-bootstrap REST app.

    Args:
        config: Parsed ``sql:`` section, or ``None`` when absent.
        registry: ClickHouse connection registry entered by the app lifespan,
            or ``None`` when no section is configured.
        service: Query service registered in the container — the null
            implementation when *config* is ``None`` (spec M5).
    """

    config: SqlConfig | None
    registry: ClickHouseConnectionRegistry | None
    service: SqlQueryService


class _RegistryExecutors(Mapping[str, SqlExecutor]):
    """Lazy executor view over the ClickHouse connection registry.

    Lets ``create_app`` construct the SQL service and bind endpoints without
    opening any connection: executors resolve on first access, once the app
    lifespan has entered the registry.
    """

    def __init__(self, registry: ClickHouseConnectionRegistry, names: tuple[str, ...]) -> None:
        self._registry = registry
        self._names = names

    def __getitem__(self, name: str) -> SqlExecutor:
        if name not in self._names:
            raise KeyError(name)
        return self._registry.executor(name)

    def __contains__(self, name: object) -> bool:
        # Membership from config alone: never touches the registry, so the
        # service policy checks work outside the lifespan as well.
        return name in self._names

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def _resolve_sql(ctx: ConfigContext, auth: _AuthWiring) -> _SqlWiring:
    """Load the optional ``sql:`` section into its registry and service.

    Absent section → no registry and the null service, keeping
    ``SqlQueryService`` always resolvable with an actionable error (spec M5).
    Present section → startup auth gate (spec §4) plus a registry whose
    connections only open inside the app lifespan.
    """
    sql_cfg = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    if sql_cfg is None:
        return _SqlWiring(config=None, registry=None, service=NullSqlQueryService())
    _validate_sql_endpoint_auth(sql_cfg, auth)
    registry = _build_sql_registry(sql_cfg)
    executors = _RegistryExecutors(registry, tuple(sql_cfg.connections))
    service = SqlQueryService(executors=executors, config=sql_cfg)
    return _SqlWiring(config=sql_cfg, registry=registry, service=service)


def _build_sql_registry(sql_cfg: SqlConfig) -> ClickHouseConnectionRegistry:
    """Construct the ClickHouse registry — no connection is opened here."""
    # Local import: clickhouse-connect is an optional dependency
    # (loom-kernel[clickhouse]) and must not be required without a 'sql' section.
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry

    return ClickHouseConnectionRegistry(config=sql_cfg)


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


def _register_sql_collaborators(
    container: LoomContainer, sql: _SqlWiring, observability: ObservabilityRuntime
) -> None:
    """Register both SQL collaborators (APPLICATION scope) — always present (M5).

    ``SqlQueryService`` is the unbound path, for system work with no caller;
    ``CallerBoundSql`` derives the roles from the verified identity and is what
    a use case acting on behalf of a caller injects. Both are always
    resolvable, so neither choice depends on the config being present.

    The runtime is handed to the bound path so its queries leave the same audit
    trail as the REST endpoint: one span per query, labelled with the effective
    roles and the caller subject.
    """
    service = sql.service
    config = sql.config if sql.config is not None else SqlConfig(connections={})
    caller_bound = CallerBoundSql(service, config, observability)
    container.register(SqlQueryService, lambda: service, scope=Scope.APPLICATION)
    container.register(CallerBoundSql, lambda: caller_bound, scope=Scope.APPLICATION)


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
