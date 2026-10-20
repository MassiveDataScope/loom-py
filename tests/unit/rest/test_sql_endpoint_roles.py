"""Identity-bound SQL role resolution at the REST edge (spec §4).

The effective roles of a query come from the VERIFIED identity, never from the
request body. The body may only narrow that set; the connection allowlist is
the last barrier. Every test here pins one leg of that invariant: identity and
body can only ever restrict, never widen.

The endpoint is mechanism-agnostic: the same rules must hold whether the caller
was authenticated by JWT or by anything else, which is why the suite runs the
whole chain twice — once through the real JWT mechanism and once through an
authenticator that has never heard of a token.
"""

from __future__ import annotations

import pathlib
import tempfile
import time
from typing import Any

import jwt as pyjwt
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.observability.event import LifecycleEvent
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.config import SqlEndpointConfig
from loom.core.sql.service import SqlQueryService
from loom.rest.auth import (
    AuthenticationMiddleware,
    JwtAuthConfig,
    JwtAuthenticator,
    RequestCredentials,
)
from loom.rest.fastapi.sql import bind_sql_endpoints
from tests.unit.core.sql._fakes import (
    FakeSqlExecutor,
    make_connection_config,
    make_sql_config,
)

ROLES_CLAIM = "loom_sql_roles"
ROLE_A = "role_viz_reader"
ROLE_B = "role_viz_sales"
ROLE_FORBIDDEN = "role_admin_evil"
SECRET = "unit-test-secret"
SECRET_PATH = str(pathlib.Path(tempfile.mkdtemp()) / "hs.key")
pathlib.Path(SECRET_PATH).write_text(SECRET)
AUDIENCE = "loom-api"
SUBJECT_HEADER = "x-subject"
ROLES_HEADER = "x-roles"
_SQL_PATH = "/sql/analytics"
_SELECT_ONE = {"sql": "SELECT 1"}


class _RoleAuthenticator:
    """Non-JWT mechanism: roles come from a plain header, verified elsewhere."""

    name = "test-headers"
    provides_roles = True

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        subject = credentials.header(SUBJECT_HEADER)
        if subject is None:
            return None
        raw = credentials.header(ROLES_HEADER) or ""
        roles = tuple(part for part in raw.split(",") if part)
        return Identity(subject=subject, roles=roles, mechanism=self.name)


class _RecordingObserver:
    """Captures every lifecycle event emitted through the observability runtime."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


class _IdentityMiddleware:
    """Publishes a fixed identity, standing in for any authentication mechanism."""

    def __init__(self, app: Any, *, identity: Identity | None) -> None:
        self._app = app
        self._identity = identity

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or self._identity is None:
            await self._app(scope, receive, send)
            return
        token = set_identity(self._identity)
        try:
            await self._app(scope, receive, send)
        finally:
            reset_identity(token)


def _app(
    executor: FakeSqlExecutor,
    *,
    runtime: ObservabilityRuntime | None = None,
    **conn_overrides: Any,
) -> FastAPI:
    connection = make_connection_config(
        sql_endpoint=SqlEndpointConfig(enabled=True, auth="identity"),
        **conn_overrides,
    )
    config = make_sql_config(analytics=connection)
    app = FastAPI()
    bind_sql_endpoints(
        app,
        service=SqlQueryService(executors={"analytics": executor}, config=config),
        config=config,
        authenticator=_RoleAuthenticator(),
        observability_runtime=runtime,
    )
    return app


def _client(
    executor: FakeSqlExecutor,
    *,
    identity: Identity | None,
    runtime: ObservabilityRuntime | None = None,
    **conn_overrides: Any,
) -> TestClient:
    app = _app(executor, runtime=runtime, **conn_overrides)
    app.add_middleware(_IdentityMiddleware, identity=identity)
    return TestClient(app, raise_server_exceptions=False)


def _post(client: TestClient, **body: Any) -> Any:
    payload: dict[str, Any] = dict(_SELECT_ONE)
    payload.update(body)
    return client.post(_SQL_PATH, json=payload)


def _identity(roles: tuple[str, ...], *, subject: str = "user-1") -> Identity:
    return Identity(subject=subject, roles=roles, mechanism="test")


# ---------------------------------------------------------------------------
# The identity is the only source of authorization
# ---------------------------------------------------------------------------


def test_denies_when_the_request_carries_no_identity() -> None:
    """No identity in the request context means no caller: 403, nothing executed."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=None))
    assert (response.status_code, executor.calls) == (403, [])


def test_denies_an_anonymous_identity() -> None:
    """The anonymous identity authorizes nothing; it is not a caller."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=Identity(subject="")))
    assert (response.status_code, executor.calls) == (403, [])


def test_denies_when_the_identity_holds_no_role() -> None:
    """An authenticated caller without roles is not an implicit 'everything'."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=_identity(())))
    assert (response.status_code, executor.calls) == (403, [])


def test_denies_with_the_standard_error_body() -> None:
    """Denials reuse the framework error body so callers get code + trace_id."""
    detail = _post(_client(FakeSqlExecutor(), identity=None)).json()["detail"]
    assert (detail["code"], "trace_id" in detail) == ("forbidden", True)


def test_accepts_a_single_role_identity() -> None:
    """One role is the degenerate case of the same contract."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=_identity((ROLE_A,))))
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A,))


# ---------------------------------------------------------------------------
# Intersection with the allowlist — the identity can never widen it
# ---------------------------------------------------------------------------


def test_intersects_the_identity_roles_with_the_connection_allowlist() -> None:
    """A held role outside ``allowed_roles`` is dropped, the rest still runs."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=_identity((ROLE_A, ROLE_FORBIDDEN))))
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A,))


def test_denies_when_no_held_role_survives_the_allowlist() -> None:
    """An empty intersection is a denial, never a fallback."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=_identity((ROLE_FORBIDDEN,))))
    assert (response.status_code, executor.calls) == (403, [])


def test_default_role_never_replaces_the_identity_intersection() -> None:
    """``default_role`` is not a fallback once roles are bound to the identity."""
    executor = FakeSqlExecutor()
    client = _client(executor, identity=_identity((ROLE_FORBIDDEN,)), default_role=ROLE_A)
    response = _post(client)
    assert (response.status_code, executor.calls) == (403, [])


def test_a_missing_identity_does_not_fall_back_to_the_default_role() -> None:
    """Without an identity the query never runs with ``default_role`` either."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=None, default_role=ROLE_A))
    assert (response.status_code, executor.calls) == (403, [])


# ---------------------------------------------------------------------------
# The body may only narrow
# ---------------------------------------------------------------------------


def test_applies_every_authorized_role_when_the_body_asks_for_none() -> None:
    """Without ``roles`` in the body the query runs with the union of the identity's."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, identity=_identity((ROLE_A, ROLE_B))))
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A, ROLE_B))


def test_body_narrows_the_authorized_roles() -> None:
    """A body subset restricts the query to exactly those roles."""
    executor = FakeSqlExecutor()
    client = _client(executor, identity=_identity((ROLE_A, ROLE_B)))
    response = _post(client, roles=[ROLE_B])
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_B,))


def test_body_cannot_request_a_role_the_identity_does_not_hold() -> None:
    """An allowlisted role the caller does not hold is still refused: no widening."""
    executor = FakeSqlExecutor()
    client = _client(executor, identity=_identity((ROLE_A,)))
    response = _post(client, roles=[ROLE_B])
    assert (response.status_code, executor.calls) == (403, [])


def test_body_cannot_request_a_role_outside_the_allowlist() -> None:
    """The allowlist stays the last barrier even for a held role."""
    executor = FakeSqlExecutor()
    client = _client(executor, identity=_identity((ROLE_A, ROLE_FORBIDDEN)))
    response = _post(client, roles=[ROLE_FORBIDDEN])
    assert (response.status_code, executor.calls) == (403, [])


def test_empty_body_roles_behave_like_an_absent_field() -> None:
    """``roles: []`` narrows to nothing, so it is read as 'no narrowing'."""
    executor = FakeSqlExecutor()
    client = _client(executor, identity=_identity((ROLE_A, ROLE_B)))
    response = _post(client, roles=[])
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A, ROLE_B))


def test_singular_role_field_is_rejected_by_the_schema() -> None:
    """The body field is ``roles``; the removed singular ``role`` no longer decides."""
    executor = FakeSqlExecutor()
    client = _client(executor, identity=_identity((ROLE_A, ROLE_B)))
    response = _post(client, role=ROLE_B)
    assert (response.status_code, executor.calls) == (422, [])


# ---------------------------------------------------------------------------
# End to end through real authentication mechanisms
# ---------------------------------------------------------------------------


def _jwt_client(executor: FakeSqlExecutor) -> TestClient:
    app = _app(executor)
    app.add_middleware(
        AuthenticationMiddleware,
        authenticator=JwtAuthenticator(
            JwtAuthConfig(
                secret_path=SECRET_PATH,
                algorithms=("HS256",),
                audience=AUDIENCE,
                roles_claim=ROLES_CLAIM,
            )
        ),
    )
    return TestClient(app, raise_server_exceptions=False)


def _header_client(executor: FakeSqlExecutor) -> TestClient:
    app = _app(executor)
    app.add_middleware(AuthenticationMiddleware, authenticator=_RoleAuthenticator())
    return TestClient(app, raise_server_exceptions=False)


def _token(roles: Any, *, subject: str = "user-1") -> str:
    return pyjwt.encode(
        {
            "sub": subject,
            "aud": AUDIENCE,
            "exp": int(time.time()) + 3600,
            ROLES_CLAIM: roles,
        },
        SECRET,
        algorithm="HS256",
    )


def test_roles_flow_from_a_real_verified_token() -> None:
    """The whole chain: signed token → verified identity → effective roles."""
    executor = FakeSqlExecutor()
    response = _jwt_client(executor).post(
        _SQL_PATH,
        json=_SELECT_ONE,
        headers={"Authorization": f"Bearer {_token([ROLE_A, ROLE_B])}"},
    )
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A, ROLE_B))


def test_a_malformed_roles_claim_is_refused_end_to_end() -> None:
    """A claim that is not a string or a list of strings authorizes nothing."""
    executor = FakeSqlExecutor()
    response = _jwt_client(executor).post(
        _SQL_PATH,
        json=_SELECT_ONE,
        headers={"Authorization": f"Bearer {_token([ROLE_A, 7])}"},
    )
    assert (response.status_code, executor.calls) == (403, [])


def test_unauthenticated_request_never_reaches_the_endpoint() -> None:
    """Without a token the middleware answers 401 before any role resolution."""
    executor = FakeSqlExecutor()
    response = _jwt_client(executor).post(_SQL_PATH, json=_SELECT_ONE)
    assert (response.status_code, executor.calls) == (401, [])


def test_a_non_jwt_mechanism_derives_the_same_roles() -> None:
    """Agnosticism: swapping the mechanism changes no rule and no SQL config."""
    executor = FakeSqlExecutor()
    response = _header_client(executor).post(
        _SQL_PATH,
        json=_SELECT_ONE,
        headers={SUBJECT_HEADER: "user-1", ROLES_HEADER: f"{ROLE_A},{ROLE_FORBIDDEN}"},
    )
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A,))


def test_a_non_jwt_mechanism_narrows_through_the_body_too() -> None:
    """The body rule is the endpoint's, not the mechanism's: it holds for both."""
    executor = FakeSqlExecutor()
    response = _header_client(executor).post(
        _SQL_PATH,
        json={**_SELECT_ONE, "roles": [ROLE_B]},
        headers={SUBJECT_HEADER: "user-1", ROLES_HEADER: f"{ROLE_A},{ROLE_B}"},
    )
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_B,))


def test_a_non_jwt_mechanism_refuses_an_unknown_caller() -> None:
    """The refusal path is the mechanism's, the 401 shape is the framework's."""
    executor = FakeSqlExecutor()
    response = _header_client(executor).post(_SQL_PATH, json=_SELECT_ONE)
    assert (response.status_code, executor.calls) == (401, [])


def test_span_labels_the_effective_roles_the_subject_and_the_mechanism() -> None:
    """Auditability: the span states who ran the query, how, and with which privileges."""
    observer = _RecordingObserver()
    client = _client(
        FakeSqlExecutor(),
        identity=_identity((ROLE_A, ROLE_B), subject="user-42"),
        runtime=ObservabilityRuntime([observer]),
    )

    response = _post(client, roles=[ROLE_B])

    assert response.status_code == 200
    meta = observer.events[0].meta
    assert (meta["roles"], meta["subject"], meta["mechanism"]) == (ROLE_B, "user-42", "test")


# ---------------------------------------------------------------------------
# The binding is per connection, the mechanism is per application
# ---------------------------------------------------------------------------


def _single_role_app(executor: FakeSqlExecutor) -> FastAPI:
    """One single-role connection (empty allowlist + default_role) and a mechanism."""
    connection = make_connection_config(
        allowed_roles=(),
        default_role=ROLE_A,
        sql_endpoint=SqlEndpointConfig(enabled=True, auth="identity"),
    )
    config = make_sql_config(analytics=connection)
    app = FastAPI()
    bind_sql_endpoints(
        app,
        service=SqlQueryService(executors={"analytics": executor}, config=config),
        config=config,
        authenticator=_RoleAuthenticator(),
    )
    return app


def test_a_single_role_connection_still_serves_under_a_role_binding_mechanism() -> None:
    """The mechanism is global, ``allowed_roles`` is per connection.

    Binding a connection with an empty allowlist would intersect against nothing
    and deny every request — a silent deployment trap for any application mixing
    a multi-role connection with a single-role one.
    """
    executor = FakeSqlExecutor()
    app = _single_role_app(executor)
    app.add_middleware(_IdentityMiddleware, identity=_identity((ROLE_B,)))
    client = TestClient(app, raise_server_exceptions=False)

    response = _post(client)

    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A,))


def test_a_single_role_connection_still_rejects_caller_supplied_roles() -> None:
    """The empty allowlist stays the barrier: the body may not pick a role."""
    executor = FakeSqlExecutor()
    app = _single_role_app(executor)
    app.add_middleware(_IdentityMiddleware, identity=_identity((ROLE_B,)))
    client = TestClient(app, raise_server_exceptions=False)

    response = _post(client, roles=[ROLE_B])

    assert (response.status_code, executor.calls) == (403, [])


def test_the_span_names_the_default_role_of_a_single_role_connection() -> None:
    """The audit label must state the privileges the query really ran with."""
    observer = _RecordingObserver()
    executor = FakeSqlExecutor()
    connection = make_connection_config(
        allowed_roles=(),
        default_role=ROLE_A,
        sql_endpoint=SqlEndpointConfig(enabled=True, auth="identity"),
    )
    config = make_sql_config(analytics=connection)
    app = FastAPI()
    bind_sql_endpoints(
        app,
        service=SqlQueryService(executors={"analytics": executor}, config=config),
        config=config,
        authenticator=_RoleAuthenticator(),
        observability_runtime=ObservabilityRuntime([observer]),
    )
    app.add_middleware(_IdentityMiddleware, identity=_identity((ROLE_B,)))

    response = _post(TestClient(app, raise_server_exceptions=False))

    assert response.status_code == 200
    assert observer.events[0].meta["roles"] == ROLE_A
