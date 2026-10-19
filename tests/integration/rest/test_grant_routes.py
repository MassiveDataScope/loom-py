"""ADR 0002 end to end: the grant routes periplo-cloud declares, over an in-memory store."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID, uuid4

import msgspec
from fastapi.testclient import TestClient

from loom.core.bootstrap.bootstrap import bootstrap_app
from loom.core.command import Command
from loom.core.di.container import LoomContainer
from loom.core.errors import NotFound, RateLimited
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case import Created, Input, Query
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RouteSources
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.model import RestInterface, RestRoute

_MEMBERSHIP = UUID("12345678-1234-5678-1234-567812345678")
_FASTAPI_SCHEMAS = frozenset({"HTTPValidationError", "ValidationError"})


class TenantRoleCode(StrEnum):
    ADMIN = "admin"
    MEMBER = "member"


class GrantStateCode(StrEnum):
    LIVE = "live"
    EXPIRED = "expired"
    REVOKED = "revoked"


class GrantView(msgspec.Struct, frozen=True, kw_only=True, rename="camel"):
    grant_id: UUID
    membership_id: UUID
    role_code: TenantRoleCode
    state: GrantStateCode


class GrantPage(msgspec.Struct, frozen=True, kw_only=True, rename="camel"):
    items: list[GrantView]
    next_cursor: str | None = None


class GrantQuery(
    msgspec.Struct, frozen=True, kw_only=True, rename="camel", forbid_unknown_fields=True
):
    membership_id: UUID | None = None
    role_code: TenantRoleCode | None = None
    state: GrantStateCode | None = None
    changed_since: Annotated[datetime, msgspec.Meta(tz=True)] | None = msgspec.field(
        default=None, name="changed_since"
    )
    cursor: str | None = None
    limit: Annotated[int, msgspec.Meta(ge=1, le=200)] = 50


class CreateGrantCommand(Command, frozen=True):
    membership_id: UUID
    role_code: TenantRoleCode


class GrantStore:
    def __init__(self) -> None:
        self._grants: dict[UUID, GrantView] = {}

    def grant(self, membership_id: UUID, role_code: TenantRoleCode) -> tuple[GrantView, bool]:
        for view in self._grants.values():
            if (view.membership_id, view.role_code, view.state) == (
                membership_id,
                role_code,
                GrantStateCode.LIVE,
            ):
                return view, False
        view = GrantView(
            grant_id=uuid4(),
            membership_id=membership_id,
            role_code=role_code,
            state=GrantStateCode.LIVE,
        )
        self._grants[view.grant_id] = view
        return view, True

    def revoke(self, grant_id: UUID) -> GrantView:
        if grant_id not in self._grants:
            raise NotFound("Grant", id=grant_id)
        view = msgspec.structs.replace(self._grants[grant_id], state=GrantStateCode.REVOKED)
        self._grants[grant_id] = view
        return view

    def matching(self, query: GrantQuery) -> list[GrantView]:
        return [
            view
            for view in self._grants.values()
            if query.role_code in (None, view.role_code) and query.state in (None, view.state)
        ][: query.limit]


class ChangeBudget:
    def __init__(self, tokens: int) -> None:
        self._tokens = tokens

    def spend(self) -> None:
        if self._tokens == 0:
            raise RateLimited(retry_after=1.5)
        self._tokens -= 1


class ListGrants(UseCase[Any, GrantPage]):
    def __init__(self, store: GrantStore) -> None:
        super().__init__()
        self._store = store

    async def execute(self, query: GrantQuery = Query()) -> GrantPage:
        return GrantPage(items=self._store.matching(query))


class CreateGrant(UseCase[Any, GrantView | Created[GrantView]]):
    def __init__(self, store: GrantStore, budget: ChangeBudget) -> None:
        super().__init__()
        self._store = store
        self._budget = budget

    async def execute(
        self, command: CreateGrantCommand = Input()
    ) -> GrantView | Created[GrantView]:
        self._budget.spend()
        view, created = self._store.grant(command.membership_id, command.role_code)
        return Created(view, location=f"/v1/grants/{view.grant_id}") if created else view


class RevokeGrant(UseCase[Any, GrantView]):
    def __init__(self, store: GrantStore, budget: ChangeBudget) -> None:
        super().__init__()
        self._store = store
        self._budget = budget

    async def execute(self, grant_id: UUID) -> GrantView:
        self._budget.spend()
        return self._store.revoke(grant_id)


class GrantsInterface(RestInterface[Any]):
    prefix = "/v1/grants"
    tags = ("access",)
    routes = (
        RestRoute(use_case=ListGrants, method="GET", path="", summary="Grants"),
        RestRoute(use_case=CreateGrant, method="POST", path="", summary="Grant a role"),
        RestRoute(
            use_case=RevokeGrant, method="DELETE", path="/{grant_id}", summary="Revoke a grant"
        ),
    )


class _Config:
    debug = False


def _client(tokens: int = 10) -> TestClient:
    def register(container: LoomContainer) -> None:
        container.register_instance(GrantStore, GrantStore())
        container.register_instance(ChangeBudget, ChangeBudget(tokens))

    modules: list[Callable[[LoomContainer], None]] = [register]
    result = bootstrap_app(
        config=_Config(), use_cases=[ListGrants, CreateGrant, RevokeGrant], modules=modules
    )
    app = create_fastapi_app(
        result,
        RouteSources(python=[GrantsInterface]),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    return TestClient(app)


def _grant(client: TestClient, role_code: str = "admin") -> Any:
    return client.post("/v1/grants", json={"membershipId": str(_MEMBERSHIP), "roleCode": role_code})


def test_the_openapi_document_matches_the_golden(assert_output_snapshot: Any) -> None:
    document = _client().get("/openapi.json").json()
    operations = {
        f"{method.upper()} {path}": {
            "parameters": operation.get("parameters", []),
            "requestBody": operation.get("requestBody"),
            "responses": {
                status: response
                for status, response in operation["responses"].items()
                if status.startswith("2")
            },
        }
        for path, methods in document["paths"].items()
        for method, operation in methods.items()
    }
    schemas = {
        name: schema
        for name, schema in document["components"]["schemas"].items()
        if name not in _FASTAPI_SCHEMAS
    }

    assert_output_snapshot(
        {"operations": operations, "schemas": schemas}, "rest_grant_routes_openapi"
    )


def test_a_new_grant_answers_201_and_a_repeated_one_200() -> None:
    client = _client()

    created = _grant(client)
    repeated = _grant(client)

    assert (created.status_code, repeated.status_code) == (201, 200)
    assert created.json() == repeated.json()
    assert created.headers["Location"] == f"/v1/grants/{created.json()['grantId']}"


def test_a_spent_budget_answers_429_with_retry_after() -> None:
    response = _grant(_client(tokens=0))

    assert (response.status_code, response.headers["Retry-After"]) == (429, "2")
    assert response.json()["detail"]["code"] == "rate_limited"
