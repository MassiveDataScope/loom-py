"""Typed configuration for the backend-agnostic ``sql:`` section.

Parsed via ``ctx.section(ConfigKey.SQL, SqlConfig)``. Validation is fail-fast:
invalid values abort the parse and surface as
:class:`~loom.core.config.errors.ConfigError` through the config loader.
"""

from __future__ import annotations

import re
import warnings
from collections.abc import Sequence
from typing import Any, Literal

import msgspec

from loom.core.model import LoomFrozenStruct
from loom.core.model.types import PositiveInt

# re.ASCII keeps the exact [A-Za-z0-9_] charset: without it \w would also
# accept Unicode word characters, widening this security allowlist.
_ROLE_PATTERN = re.compile(r"^\w+$", re.ASCII)
_URL_CREDENTIALS_RE = re.compile(r"://[^@]+@")

# The only ``sql_endpoint.auth`` mode carrying a verified caller identity, and
# therefore the only one able to bind a request to a subset of the allowlist.
# ``jwt`` is the deprecated spelling of the same mode, kept because it named a
# mechanism where the contract only ever needed "the framework knows the caller".
_IDENTITY_BOUND_AUTH = "identity"
_DEPRECATED_AUTH_ALIAS = "jwt"
_IDENTITY_BOUND_AUTH_MODES = frozenset({_IDENTITY_BOUND_AUTH, _DEPRECATED_AUTH_ALIAS})


def _redact_url(url: str) -> str:
    return _URL_CREDENTIALS_RE.sub("://***@", url)


def roles_need_identity_binding(
    allowed_roles: Sequence[str],
    *,
    mechanism_binds_roles: bool,
) -> bool:
    """Report whether an allowlist would be left for the caller to pick from.

    A connection allowing several roles only makes sense when something binds a
    caller to a subset of them.  Without that binding the allowlist stops being
    a ceiling and becomes a menu, so every layer that can mount such an endpoint
    refuses to.  The predicate lives here, next to the config it judges; each
    layer phrases its own error.

    Args:
        allowed_roles: Connection allowlist.
        mechanism_binds_roles: Whether the configured authentication mechanism
            binds roles to the verified caller identity.

    Returns:
        ``True`` when the configuration is unsafe as it stands.
    """
    return bool(allowed_roles) and not mechanism_binds_roles


class SqlEndpointConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Opt-in REST endpoint settings for a SQL connection.

    Attributes:
        enabled: Whether to mount the generic ``POST /sql/{name}`` endpoint.
            Defaults to ``False`` (double opt-in).
        auth: Mandatory when ``enabled``: ``"identity"`` (the framework
            authenticates the caller with the configured mechanism and binds
            roles to their verified identity) or ``"external"`` (explicit
            acknowledgement that the operator provides authentication, with no
            identity the framework can read).  ``"jwt"`` is a deprecated alias
            of ``"identity"``.
        path: Mount path override. Defaults to ``/sql/{name}`` when ``None``.
        include_in_schema: Whether the endpoint appears in the OpenAPI schema.
    """

    enabled: bool = False
    auth: Literal["identity", "jwt", "external"] | None = None
    path: str | None = None
    include_in_schema: bool = False

    def __post_init__(self) -> None:
        if self.auth != _DEPRECATED_AUTH_ALIAS:
            return
        warnings.warn(
            f"sql_endpoint.auth: {_DEPRECATED_AUTH_ALIAS!r} is deprecated because it names "
            f"a mechanism instead of a contract. Use {_IDENTITY_BOUND_AUTH!r}: the endpoint "
            "requires a verified caller, whichever authenticator provides it.",
            DeprecationWarning,
            stacklevel=3,
        )

    @property
    def binds_identity(self) -> bool:
        """Whether this endpoint requires an identity the framework can read.

        Returns:
            ``True`` for ``"identity"`` and its deprecated ``"jwt"`` alias.
        """
        return self.auth in _IDENTITY_BOUND_AUTH_MODES


class SqlConnectionConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Named SQL connection with role policy, limits and driver tuning.

    Attributes:
        backend: Backend identifier. Only ``"clickhouse"`` is supported; any
            other value fails the parse.
        url: Backend DSN. Canonically injected via environment or secret
            resolver, never inline.
        username: Driver username, handed to the driver as an explicit
            argument. Takes precedence over any credential embedded in
            ``url``.
        password: Driver password, handed to the driver as an explicit
            argument. Unlike a DSN credential it is never URL-parsed, so any
            character is safe (a ``#`` inside a DSN password truncates it as
            a fragment delimiter and the driver does not unescape quoted
            ones), and it never appears in ``repr`` or in dumps of ``url``.
        allowed_roles: Ceiling of roles this connection may ever apply — the
            last barrier, not a per-caller permission. Empty means every
            caller-provided role is rejected (fail-closed). A mounted endpoint
            with a non-empty allowlist requires the ``sql_endpoint.auth`` mode
            that binds roles to a verified identity.
        default_role: Role applied when the request carries none. Without it,
            a request without role is refused. Never a fallback for a request
            whose roles are bound to verified claims.
        readonly: Whether queries run in read-only mode. Defaults to ``True``.
        default_limit: Row limit applied when the request brings none.
        max_limit: Hard cap for any requested limit.
        max_execution_time: Per-query execution timeout in seconds, above
            zero: ``0`` means no limit to ClickHouse.
        max_sql_bytes: Maximum accepted SQL statement size in bytes.
        connect_timeout: Driver connect timeout in seconds, above zero.
        send_receive_timeout: Driver send/receive timeout in seconds, above
            zero.
        executor_threads: Async client thread pool size, above zero (driver
            default if ``None``).
        pool_size: HTTP connection pool size, above zero (driver default if
            ``None``).
        settings: Extra backend settings handed to the executor at
            construction. They can never override the policy.
        sql_endpoint: Optional REST endpoint settings for this connection.

    Raises:
        ValueError: On invalid role format, ``default_limit`` above
            ``max_limit``, or an enabled endpoint without role/auth, including
            a non-empty allowlist under an auth mode that carries no verified
            identity. Surfaced as ``ConfigError`` when parsed through the
            config loader.
    """

    backend: Literal["clickhouse"]
    url: str
    username: str | None = None
    password: str | None = None
    allowed_roles: tuple[str, ...] = ()
    default_role: str | None = None
    readonly: bool = True
    default_limit: int = 1000
    max_limit: int = 10000
    max_execution_time: PositiveInt = 30
    max_sql_bytes: int = 262144
    connect_timeout: PositiveInt = 10
    send_receive_timeout: PositiveInt = 60
    executor_threads: PositiveInt | None = None
    pool_size: PositiveInt | None = None
    settings: dict[str, Any] = msgspec.field(default_factory=dict)
    sql_endpoint: SqlEndpointConfig = msgspec.field(default_factory=SqlEndpointConfig)

    def __post_init__(self) -> None:
        _validate_roles(self.allowed_roles, self.default_role)
        _validate_limits(self.default_limit, self.max_limit)
        _validate_endpoint(self.sql_endpoint, self.allowed_roles, self.default_role)

    def __repr__(self) -> str:
        return (
            f"SqlConnectionConfig(backend={self.backend!r},"
            f" url={_redact_url(self.url)!r},"
            f" allowed_roles={self.allowed_roles!r},"
            f" default_role={self.default_role!r},"
            f" readonly={self.readonly!r},"
            f" sql_endpoint={self.sql_endpoint!r})"
        )


class SqlConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Root of the ``sql:`` config section.

    Attributes:
        connections: Named SQL connections available to the query service.
    """

    connections: dict[str, SqlConnectionConfig]


def _validate_roles(allowed_roles: tuple[str, ...], default_role: str | None) -> None:
    for role in allowed_roles:
        _validate_role_format(role)
    if default_role is not None:
        _validate_role_format(default_role)


def _validate_role_format(role: str) -> None:
    if not _ROLE_PATTERN.fullmatch(role):
        raise ValueError(
            f"Invalid SQL role {role!r}: roles must contain only "
            "ASCII letters, digits or underscores"
        )


def _validate_limits(default_limit: int, max_limit: int) -> None:
    if default_limit > max_limit:
        raise ValueError(f"default_limit ({default_limit}) must not exceed max_limit ({max_limit})")


def _validate_endpoint(
    endpoint: SqlEndpointConfig,
    allowed_roles: tuple[str, ...],
    default_role: str | None,
) -> None:
    if not endpoint.enabled:
        return
    if not allowed_roles and default_role is None:
        raise ValueError(
            "sql_endpoint.enabled requires a 'default_role' or a non-empty "
            "'allowed_roles' on the connection"
        )
    if allowed_roles and not endpoint.binds_identity:
        raise ValueError(
            "sql_endpoint.enabled with a non-empty 'allowed_roles' requires "
            f"'sql_endpoint.auth: {_IDENTITY_BOUND_AUTH}': it is the only mode whose "
            "verified identity can bind a caller to a subset of the allowlist. With "
            f"auth={endpoint.auth!r} the endpoint would let any caller pick any "
            "allowlisted role. Either switch the auth mode, or leave 'allowed_roles' "
            "empty and pin a single 'default_role'"
        )
