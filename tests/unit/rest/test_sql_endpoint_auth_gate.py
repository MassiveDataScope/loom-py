"""Config-driven auth gate tests for the SQL endpoint in ``create_app`` (spec §4/§7.11).

These tests drive :func:`loom.rest.fastapi.auto.create_app` with temporary YAML
configs plus a minimal discoverable app package, mirroring the structure used by
``tests/integration/celery_bootstrap``. ``create_app`` must not open any network
connection: the ClickHouse registry is only entered during the app lifespan,
which these tests never start.

The last test drives :func:`loom.rest.fastapi.sql.bind_sql_endpoints` directly:
the same invariant must hold for a hand-written composition root, where no YAML
gate ever runs.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi import FastAPI

from loom.core.config.errors import ConfigError
from loom.core.identity import Identity
from loom.core.sql.config import SqlConfig, SqlConnectionConfig, SqlEndpointConfig
from loom.core.sql.service import NullSqlQueryService
from loom.rest.auth import AuthenticationMiddleware, RequestCredentials
from loom.rest.fastapi.auto import create_app
from loom.rest.fastapi.sql import bind_sql_endpoints

_SECRET_PATH = str(Path(tempfile.mkdtemp()) / "hs.key")
Path(_SECRET_PATH).write_text("unit-test-secret")

_APP_MODULE = "loom_sqlgate_fixture_app"


class _StubAuthenticator:
    """Minimal non-JWT mechanism used to drive the composition-root gates."""

    name = "stub"

    def __init__(self, *, provides_roles: bool = True) -> None:
        self.provides_roles = provides_roles

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        del credentials
        return Identity(subject="stub-user", mechanism=self.name)


_APP_SOURCE = '''\
"""Minimal discoverable app used by the SQL endpoint auth-gate tests."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class SqlGateRecord(BaseModel):
    __tablename__ = "sqlgate_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class SqlGatePingUseCase(UseCase[SqlGateRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class SqlGatePingInterface(RestInterface[str]):
    # The prefix must NOT start with "/sql": the tests assert that no route
    # under "/sql" exists when the endpoint does not mount.
    prefix = "/gate-ping"
    routes = (RestRoute(use_case=SqlGatePingUseCase, method="GET", path="/"),)
'''


def _sql_section(endpoint: dict[str, Any], **conn_overrides: Any) -> dict[str, Any]:
    """SQL section with a single 'analytics' connection and the given endpoint.

    The default connection is the single-role shape (empty allowlist plus a
    fixed ``default_role``): an allowlist is only representable on a mounted
    endpoint under ``sql_endpoint.auth: jwt`` (spec §4).
    """
    connection: dict[str, Any] = {
        "backend": "clickhouse",
        "url": "http://localhost:8123",
        "allowed_roles": [],
        "default_role": "role_viz_reader",
        "sql_endpoint": endpoint,
    }
    connection.update(conn_overrides)
    return {"connections": {"analytics": connection}}


def _write_project(
    tmp_path: Path,
    *,
    sql_section: dict[str, Any] | None,
    jwt_section: dict[str, Any] | None = None,
    custom_auth: bool = False,
) -> str:
    """Write the fixture app module plus a YAML config; return the config path."""
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "sqlgate-demo",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    auth: dict[str, Any] = {}
    if jwt_section is not None:
        auth["jwt"] = jwt_section
    if custom_auth:
        auth["custom"] = True
    if auth:
        config["app"]["rest"] = {"auth": auth}
    if sql_section is not None:
        config["sql"] = sql_section
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _route_paths(app: FastAPI) -> set[str]:
    return {str(getattr(route, "path", "")) for route in app.routes}


_JWT_SECTION: dict[str, Any] = {
    "secret_path": _SECRET_PATH,
    "algorithms": ["HS256"],
    "audience": "loom-api",
}


def test_auth_identity_without_any_mechanism_fails_at_startup(tmp_path: Path) -> None:
    """``auth: identity`` demands an authentication mechanism: its absence aborts."""
    config_path = _write_project(
        tmp_path, sql_section=_sql_section({"enabled": True, "auth": "identity"})
    )
    with pytest.raises(ConfigError, match="authentication"):
        create_app(config_path)


def test_auth_external_mounts_the_endpoint(tmp_path: Path) -> None:
    """``auth: external`` mounts the route (operator acknowledges external auth)."""
    config_path = _write_project(
        tmp_path, sql_section=_sql_section({"enabled": True, "auth": "external"})
    )
    app = create_app(config_path)
    assert "/sql/analytics" in _route_paths(app)


def test_auth_external_warns_at_startup(tmp_path: Path) -> None:
    """Every mounted SQL endpoint emits a startup warning naming its path."""
    config_path = _write_project(
        tmp_path, sql_section=_sql_section({"enabled": True, "auth": "external"})
    )
    with pytest.warns(UserWarning, match="/sql/analytics"):
        create_app(config_path)


def test_multi_role_endpoint_warns_naming_the_claim_that_binds_the_roles(tmp_path: Path) -> None:
    """The startup warning must name the verified claim the roles are bound to.

    ``allowed_roles`` is the ceiling of the connection; the effective roles come
    from the claim, so the warning states both (threat model in
    ``docs/rest/sql.md``).
    """
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section(
            {"enabled": True, "auth": "identity"},
            allowed_roles=["role_viz_reader", "role_viz_sales"],
        ),
        jwt_section={**_JWT_SECTION, "roles_claim": "loom_sql_roles"},
    )
    with pytest.warns(UserWarning, match="loom_sql_roles"):
        create_app(config_path)


def test_multi_role_endpoint_without_a_configured_roles_claim_fails_at_startup(
    tmp_path: Path,
) -> None:
    """A non-empty allowlist needs a claim to bind it to, or startup aborts."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section(
            {"enabled": True, "auth": "identity"},
            allowed_roles=["role_viz_reader", "role_viz_sales"],
        ),
        jwt_section=_JWT_SECTION,
    )
    with pytest.raises(ConfigError, match="roles_claim"):
        create_app(config_path)


def test_single_role_endpoint_warns_that_every_caller_shares_the_default_role(
    tmp_path: Path,
) -> None:
    """Without an allowlist there is no per-caller distinction: say so explicitly."""
    config_path = _write_project(
        tmp_path, sql_section=_sql_section({"enabled": True, "auth": "external"})
    )
    with pytest.warns(UserWarning, match="default_role"):
        create_app(config_path)


def test_auth_jwt_without_audience_fails_at_startup(tmp_path: Path) -> None:
    """Binding roles to a claim is void if tokens minted for other services pass."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "identity"}),
        jwt_section={"secret_path": _SECRET_PATH, "algorithms": ["HS256"]},
    )
    with pytest.raises(ConfigError, match="audience"):
        create_app(config_path)


def test_sql_path_excluded_from_jwt_authentication_fails_at_startup(tmp_path: Path) -> None:
    """A mounted SQL path listed in ``exclude_paths`` would bypass authentication."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "identity"}),
        jwt_section={**_JWT_SECTION, "exclude_paths": ["/docs", "/sql/analytics"]},
    )
    with pytest.raises(ConfigError, match="exclude_paths"):
        create_app(config_path)


def test_custom_sql_path_excluded_from_jwt_authentication_fails_at_startup(
    tmp_path: Path,
) -> None:
    """The check follows the configured ``path``, not only the default one."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "identity", "path": "/query/analytics"}),
        jwt_section={**_JWT_SECTION, "exclude_paths": ["/query/analytics"]},
    )
    with pytest.raises(ConfigError, match="exclude_paths"):
        create_app(config_path)


def test_enabled_endpoint_without_auth_field_does_not_mount(tmp_path: Path) -> None:
    """``enabled: true`` without ``auth`` boots the app but mounts nothing."""
    config_path = _write_project(tmp_path, sql_section=_sql_section({"enabled": True}))
    app = create_app(config_path)
    assert not any(path.startswith("/sql") for path in _route_paths(app))


def test_auth_identity_with_a_jwt_section_mounts_the_endpoint(tmp_path: Path) -> None:
    """``auth: identity`` plus a configured mechanism mounts the route."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "identity"}),
        jwt_section=_JWT_SECTION,
    )
    app = create_app(config_path)
    assert "/sql/analytics" in _route_paths(app)


def test_the_deprecated_jwt_auth_mode_still_mounts_the_endpoint(tmp_path: Path) -> None:
    """Existing configs keep working: ``auth: jwt`` is an alias of ``auth: identity``."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "jwt"}),
        jwt_section=_JWT_SECTION,
    )
    with pytest.warns(DeprecationWarning, match="identity"):
        app = create_app(config_path)
    assert "/sql/analytics" in _route_paths(app)


def test_a_configured_mechanism_mounts_the_authentication_middleware(tmp_path: Path) -> None:
    """A present ``app.rest.auth.jwt`` section wires authentication into the app."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "identity"}),
        jwt_section=_JWT_SECTION,
    )
    app = create_app(config_path)
    assert any(m.cls is AuthenticationMiddleware for m in app.user_middleware)


def test_a_custom_authenticator_satisfies_the_identity_gate(tmp_path: Path) -> None:
    """Agnosticism at the composition root: any mechanism unlocks the endpoint."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "identity"}),
        custom_auth=True,
    )
    app = create_app(config_path, authenticator=_StubAuthenticator())
    assert "/sql/analytics" in _route_paths(app)


def test_a_custom_authenticator_binds_the_allowlisted_roles(tmp_path: Path) -> None:
    """A mechanism declaring ``provides_roles`` unlocks a multi-role endpoint."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section(
            {"enabled": True, "auth": "identity"},
            allowed_roles=["role_viz_reader", "role_viz_sales"],
        ),
        custom_auth=True,
    )
    app = create_app(config_path, authenticator=_StubAuthenticator())
    assert "/sql/analytics" in _route_paths(app)


def test_a_custom_authenticator_without_roles_fails_the_multi_role_gate(tmp_path: Path) -> None:
    """A mechanism binding no role would let any caller pick any allowlisted one."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section(
            {"enabled": True, "auth": "identity"},
            allowed_roles=["role_viz_reader", "role_viz_sales"],
        ),
        custom_auth=True,
    )
    authenticator = _StubAuthenticator(provides_roles=False)
    with pytest.raises(ConfigError, match="binds no role"):
        create_app(config_path, authenticator=authenticator)


def test_two_authentication_mechanisms_at_once_fail_at_startup(tmp_path: Path) -> None:
    """Two mechanisms mean two sources of truth for the caller: refuse to boot."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "identity"}),
        jwt_section=_JWT_SECTION,
        custom_auth=True,
    )
    authenticator = _StubAuthenticator()
    with pytest.raises(ConfigError, match="Exactly one authentication mechanism"):
        create_app(config_path, authenticator=authenticator)


def test_non_readonly_enabled_endpoint_warns_at_startup(tmp_path: Path) -> None:
    """``readonly: false`` plus an enabled endpoint triggers a startup warning."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "external"}, readonly=False),
    )
    with pytest.warns(UserWarning, match="readonly"):
        create_app(config_path)


def test_without_sql_section_no_sql_routes_are_mounted(tmp_path: Path) -> None:
    """Absent ``sql:`` section means zero changes: the app boots with no /sql routes."""
    config_path = _write_project(tmp_path, sql_section=None)
    app = create_app(config_path)
    assert not any(path.startswith("/sql") for path in _route_paths(app))


def test_binder_refuses_a_multi_role_endpoint_without_an_identity_binding() -> None:
    """The invariant holds for any composition root, not only for create_app.

    The two guards run at different moments and this one is load-bearing: the
    "no binding" branch of role resolution is only safe because the binder
    refuses to mount a non-empty allowlist without one.
    """
    config = SqlConfig(
        connections={
            "analytics": SqlConnectionConfig(
                backend="clickhouse",
                url="clickhouse://user:pw@host:8123/default",
                allowed_roles=("role_a", "role_b"),
                sql_endpoint=SqlEndpointConfig(enabled=True, auth="identity"),
            )
        }
    )
    app = FastAPI()
    service = NullSqlQueryService()

    with pytest.raises(ConfigError, match="binds them to the caller identity"):
        bind_sql_endpoints(app, service=service, config=config)
