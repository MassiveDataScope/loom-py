"""Parsing and fail-fast validation tests for the ``sql:`` section (spec §3/§7.1)."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.sql.config import SqlConfig, SqlEndpointConfig


def _minimal(**overrides: Any) -> dict[str, Any]:
    """Minimal valid connection per the spec (backend + url)."""
    base: dict[str, Any] = {"backend": "clickhouse", "url": "http://localhost:8123"}
    base.update(overrides)
    return base


def _parse(connection: dict[str, Any]) -> SqlConfig:
    """Parse a single 'analytics' connection via ConfigContext + ConfigKey.SQL."""
    ctx = ConfigContext.from_dict({"sql": {"connections": {"analytics": connection}}})
    return ctx.section(ConfigKey.SQL, SqlConfig)


def test_config_key_sql_points_at_the_sql_section() -> None:
    """``ConfigKey.SQL`` exists and resolves to the 'sql' YAML path."""
    assert str(ConfigKey.SQL) == "sql"


def test_parses_defaults_when_connection_is_minimal() -> None:
    """A connection with only backend+url adopts every default from spec §3."""
    conn = _parse(_minimal()).connections["analytics"]
    assert (
        conn.readonly,
        conn.default_limit,
        conn.max_limit,
        conn.max_execution_time,
        conn.max_sql_bytes,
        conn.connect_timeout,
        conn.send_receive_timeout,
        conn.executor_threads,
        conn.pool_size,
    ) == (True, 1000, 10000, 30, 262144, 10, 60, None, None)


def test_endpoint_is_disabled_when_not_declared() -> None:
    """``sql_endpoint.enabled`` is opt-in: default False (double opt-in of §4)."""
    conn = _parse(_minimal()).connections["analytics"]
    assert conn.sql_endpoint.enabled is False


def test_extra_settings_are_empty_when_not_declared() -> None:
    """``settings:`` defaults to an empty mapping (no implicit CH settings)."""
    conn = _parse(_minimal()).connections["analytics"]
    assert dict(conn.settings) == {}


def test_parses_allowed_roles_and_default_role_when_declared() -> None:
    """The allowlist and default_role from YAML arrive typed in the config."""
    conn = _parse(
        _minimal(
            allowed_roles=["role_viz_reader", "role_viz_sales"],
            default_role="role_viz_reader",
        )
    ).connections["analytics"]
    assert (tuple(conn.allowed_roles), conn.default_role) == (
        ("role_viz_reader", "role_viz_sales"),
        "role_viz_reader",
    )


def test_parses_multiple_connections_when_more_than_one() -> None:
    """Every entry under ``connections`` yields its own named connection."""
    ctx = ConfigContext.from_dict(
        {"sql": {"connections": {"analytics": _minimal(), "reporting": _minimal()}}}
    )
    config = ctx.section(ConfigKey.SQL, SqlConfig)
    assert set(config.connections) == {"analytics", "reporting"}


def test_fails_when_url_is_missing() -> None:
    """``url`` is mandatory: its absence aborts the parse with ConfigError."""
    with pytest.raises(ConfigError):
        _parse({"backend": "clickhouse"})


def test_fails_when_an_allowlist_role_has_invalid_format() -> None:
    """Roles outside ``^[A-Za-z0-9_]+$`` are rejected fail-fast (anti-injection)."""
    connection = _minimal(allowed_roles=["role-bad!"])
    with pytest.raises(ConfigError):
        _parse(connection)


def test_fails_when_an_allowlist_role_has_unicode_word_characters() -> None:
    """Role validation is ASCII-only: Unicode word characters are rejected."""
    connection = _minimal(allowed_roles=["señor"])
    with pytest.raises(ConfigError):
        _parse(connection)


def test_fails_when_default_role_has_invalid_format() -> None:
    """``default_role`` follows the same format validation as the allowlist."""
    connection = _minimal(default_role="bad role;drop")
    with pytest.raises(ConfigError):
        _parse(connection)


def test_fails_when_backend_is_unknown() -> None:
    """Only ``clickhouse`` is supported: any other backend => ConfigError."""
    connection = _minimal(backend="postgres")
    with pytest.raises(ConfigError):
        _parse(connection)


def test_fails_when_default_limit_exceeds_max_limit() -> None:
    """``default_limit <= max_limit`` is validated at parse time, not at runtime."""
    connection = _minimal(default_limit=100, max_limit=10)
    with pytest.raises(ConfigError):
        _parse(connection)


def test_fails_when_endpoint_enabled_without_default_role_or_allowlist() -> None:
    """``enabled: true`` without any role cannot mount an endpoint (fail-closed)."""
    connection = _minimal(sql_endpoint={"enabled": True, "auth": "external"})
    with pytest.raises(ConfigError):
        _parse(connection)


def test_repr_redacts_the_dsn_credentials() -> None:
    """``repr`` never exposes the DSN password (docs promise redaction)."""
    conn = _parse(
        _minimal(url="clickhouse://loom_user:s3cr3t_pw@ch.internal:8123/analytics")
    ).connections["analytics"]
    rendered = repr(conn)
    assert ("s3cr3t_pw" in rendered, "://***@ch.internal" in rendered) == (False, True)


def test_credentials_default_to_none_when_not_declared() -> None:
    """Without explicit credentials the driver keeps whatever the DSN carries."""
    conn = _parse(_minimal()).connections["analytics"]
    assert (conn.username, conn.password) == (None, None)


def test_parses_explicit_credentials_when_declared() -> None:
    """``username``/``password`` arrive typed, whatever characters they carry."""
    conn = _parse(_minimal(username="mcp_reader", password="p4ss#with%23hash")).connections[
        "analytics"
    ]
    assert (conn.username, conn.password) == ("mcp_reader", "p4ss#with%23hash")


def test_repr_never_exposes_the_explicit_password() -> None:
    """The hand-written ``repr`` omits the credential fields entirely."""
    conn = _parse(_minimal(username="mcp_reader", password="s3cr3t_pw")).connections["analytics"]
    assert "s3cr3t_pw" not in repr(conn)


def test_fails_when_auth_has_an_unknown_value() -> None:
    """``auth`` only admits 'jwt' or 'external'; any other value => ConfigError."""
    connection = _minimal(
        allowed_roles=["role_viz_reader"],
        default_role="role_viz_reader",
        sql_endpoint={"enabled": True, "auth": "basic"},
    )
    with pytest.raises(ConfigError):
        _parse(connection)


# ---------------------------------------------------------------------------
# Identity binding: the insecure shape must be unrepresentable (spec §4)
# ---------------------------------------------------------------------------


def test_parses_an_endpoint_with_an_allowlist_under_identity_bound_auth() -> None:
    """An allowlist is representable under the auth mode that carries an identity."""
    conn = _parse(
        _minimal(
            allowed_roles=["role_viz_reader"],
            sql_endpoint={"enabled": True, "auth": "jwt"},
        )
    ).connections["analytics"]
    assert (conn.sql_endpoint.enabled, conn.sql_endpoint.auth) == (True, "jwt")


def test_allows_an_enabled_endpoint_with_an_empty_allowlist_and_default_role() -> None:
    """The single-role shape stays valid: no allowlist means no role to escalate to."""
    conn = _parse(
        _minimal(
            allowed_roles=[],
            default_role="role_viz_reader",
            sql_endpoint={"enabled": True, "auth": "external"},
        )
    ).connections["analytics"]
    assert (conn.sql_endpoint.enabled, conn.sql_endpoint.auth) == (True, "external")


def test_fails_when_an_allowlisted_endpoint_uses_external_auth() -> None:
    """External auth exposes no verified identity, so it can never bind roles."""
    connection = _minimal(
        allowed_roles=["role_viz_reader", "role_viz_sales"],
        default_role="role_viz_reader",
        sql_endpoint={"enabled": True, "auth": "external"},
    )
    with pytest.raises(ConfigError, match="allowed_roles"):
        _parse(connection)


def test_fails_when_an_allowlisted_endpoint_declares_no_auth() -> None:
    """Without an auth mode there is no identity to bind the allowlist to."""
    connection = _minimal(
        allowed_roles=["role_viz_reader"],
        default_role="role_viz_reader",
        sql_endpoint={"enabled": True},
    )
    with pytest.raises(ConfigError, match="allowed_roles"):
        _parse(connection)


def test_endpoint_config_does_not_expose_a_roles_claim_field() -> None:
    """The claim name belongs to the auth mechanism, not to the ``sql:`` section."""
    assert "roles_claim" not in SqlEndpointConfig.__struct_fields__
