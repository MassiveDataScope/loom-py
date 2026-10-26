"""Integration tests for auto=True RestInterface discovery and CRUD endpoints.

These tests verify that:
- ``__init_subclass__`` populates routes when ``auto=True``
- All three discovery engines pick up auto-generated UseCases
- Generated HTTP endpoints behave correctly end-to-end
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Generator
from contextlib import asynccontextmanager
from datetime import time
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.bootstrap.bootstrap import bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy import build_sqlalchemy_repository_registration_module
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.rest.autocrud import build_auto_routes
from loom.rest.compiler import RouteSources
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.model import RestInterface, RestRoute
from tests.integration.support.logical_repo_fixtures import (
    GetTaskViewUseCase,
    TaskView,
)

# ---------------------------------------------------------------------------
# Test domain model
# ---------------------------------------------------------------------------


class AutoProduct(BaseModel):
    __tablename__ = "auto_products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    opens_at: time | None = ColumnField(nullable=True, default=None)


# ---------------------------------------------------------------------------
# Test interface (auto-populated at class definition time)
# ---------------------------------------------------------------------------


class AutoProductInterface(RestInterface[AutoProduct]):
    prefix = "/auto-products"
    tags = ("AutoProducts",)
    auto = True
    filterable = ("opens_at",)


class TaskViewInterface(RestInterface[TaskView]):
    prefix = "/task-views"
    routes = (
        RestRoute(
            use_case=GetTaskViewUseCase,
            method="GET",
            path="/{task_id}",
        ),
    )


# ---------------------------------------------------------------------------
# Bootstrap helpers
# ---------------------------------------------------------------------------


def _make_register_repos(
    session_manager: SessionManager,
) -> Callable[[LoomContainer], None]:
    repo: RepositorySQLAlchemy[Any, int] = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=AutoProduct,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )

    def _provider() -> RepositorySQLAlchemy[Any, int]:
        return repo

    token = ("repo", AutoProduct)

    def register(container: LoomContainer) -> None:
        container.register(token, _provider, scope=Scope.APPLICATION)
        container.register_repo(AutoProduct, token)

    return register


def _build_app(db_path: Path) -> FastAPI:
    database_url = f"sqlite+aiosqlite:///{db_path}"
    session_manager = SessionManager(
        database_url,
        echo=False,
        pool_pre_ping=False,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )

    use_cases = tuple(r.use_case for r in AutoProductInterface.routes)

    reset_registry()
    compile_all(AutoProduct)

    result = bootstrap_app(
        config={"name": "test"},
        use_cases=use_cases,
        modules=[_make_register_repos(session_manager)],
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        async with session_manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().create_all)
        try:
            yield
        finally:
            await session_manager.dispose()
            reset_registry()

    return create_fastapi_app(
        result,
        RouteSources(python=(AutoProductInterface,)),
        lifespan=lifespan,
    )


def _build_logical_app(db_path: Path) -> FastAPI:
    database_url = f"sqlite+aiosqlite:///{db_path}"
    session_manager = SessionManager(
        database_url,
        echo=False,
        pool_pre_ping=False,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )

    result = bootstrap_app(
        config={"name": "logical-test"},
        use_cases=(GetTaskViewUseCase,),
        modules=[
            build_sqlalchemy_repository_registration_module(
                session_manager,
                (),
                logical_models=(TaskView,),
                cursor_codec=CursorCodec(signing="ephemeral"),
            )
        ],
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await session_manager.dispose()

    return create_fastapi_app(
        result,
        RouteSources(python=(TaskViewInterface,)),
        lifespan=lifespan,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client(tmp_path: Path) -> Generator[TestClient, None, None]:
    app = _build_app(tmp_path / "auto_test.sqlite")
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# TestAutoInterfaceDiscovery
# ---------------------------------------------------------------------------


class TestAutoInterfaceDiscovery:
    def test_interface_routes_are_populated_by_init_subclass(self) -> None:
        assert len(AutoProductInterface.routes) == 5

    def test_use_cases_have_correct_names(self) -> None:
        names = {r.use_case.__name__ for r in AutoProductInterface.routes}
        assert any("AutoProduct" in n for n in names)

    def test_build_auto_routes_returns_same_use_cases_as_interface(self) -> None:
        direct = build_auto_routes(AutoProduct, include=())
        from_iface = AutoProductInterface.routes
        direct_ucs = {r.use_case for r in direct}
        iface_ucs = {r.use_case for r in from_iface}
        # Since __init_subclass__ already called build_auto_routes with the
        # same model and include=(), the UC classes should be the same objects
        # (cached in _UC_CACHE).
        assert direct_ucs == iface_ucs


# ---------------------------------------------------------------------------
# TestAutoInterfaceCRUD
# ---------------------------------------------------------------------------


class TestAutoInterfaceCRUD:
    def test_post_returns_201(self, client: TestClient) -> None:
        resp = client.post("/auto-products/", json={"name": "Widget"})
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Widget"
        assert "id" in data

    def test_get_returns_200(self, client: TestClient) -> None:
        create = client.post("/auto-products/", json={"name": "Gadget"})
        assert create.status_code == 201
        item_id = create.json()["id"]

        resp = client.get(f"/auto-products/{item_id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "Gadget"

    def test_list_returns_page_result(self, client: TestClient) -> None:
        client.post("/auto-products/", json={"name": "A"})
        client.post("/auto-products/", json={"name": "B"})

        resp = client.get("/auto-products/")
        assert resp.status_code == 200
        data = resp.json()
        assert "totalCount" in data
        assert "items" in data

    def test_patch_updates_item(self, client: TestClient) -> None:
        create = client.post("/auto-products/", json={"name": "Old"})
        assert create.status_code == 201
        item_id = create.json()["id"]

        resp = client.patch(f"/auto-products/{item_id}", json={"name": "New"})
        assert resp.status_code == 200
        updated = resp.json()
        assert updated["name"] == "New"

    def test_delete_returns_bool(self, client: TestClient) -> None:
        create = client.post("/auto-products/", json={"name": "ToDelete"})
        assert create.status_code == 201
        item_id = create.json()["id"]

        resp = client.delete(f"/auto-products/{item_id}")
        assert resp.status_code == 200
        assert resp.json() is True

    def test_get_missing_returns_404(self, client: TestClient) -> None:
        resp = client.get("/auto-products/999")
        assert resp.status_code == 404

    def test_patch_missing_returns_404(self, client: TestClient) -> None:
        resp = client.patch("/auto-products/999", json={"name": "ghost"})
        assert resp.status_code == 404

    def test_manual_interface_can_resolve_non_base_model_main_repo(self, tmp_path: Path) -> None:
        app = _build_logical_app(tmp_path / "logical_test.sqlite")

        with TestClient(app) as client:
            resp = client.get("/task-views/t-1")

        assert resp.status_code == 200
        assert resp.json() == {
            "taskId": "t-1",
            "state": "done",
        }


# ---------------------------------------------------------------------------
# TestAutoInterfaceInclude
# ---------------------------------------------------------------------------


class TestAutoInterfaceInclude:
    def test_ops_not_in_include_not_registered(self) -> None:
        class ReadOnlyInterface(RestInterface[AutoProduct]):
            prefix = "/ro-products"
            auto = True
            include = ("get", "list")

        assert len(ReadOnlyInterface.routes) == 2
        methods = {r.method for r in ReadOnlyInterface.routes}
        assert "POST" not in methods
        assert "DELETE" not in methods


# ---------------------------------------------------------------------------
# TestAwareTimeRefusal
# ---------------------------------------------------------------------------


class TestAwareTimeRefusal:
    """A time with a UTC offset never reaches a Time column; the client gets a 4xx."""

    def test_an_aware_time_filter_value_answers_400(self, client: TestClient) -> None:
        resp = client.get("/auto-products/", params={"opensAt": "08:30:00+02:00"})

        assert resp.status_code == 400
        assert resp.json()["detail"]["code"] == "query_validation"
        assert "08:30" not in resp.text

    def test_an_aware_time_in_a_create_body_answers_422(self, client: TestClient) -> None:
        resp = client.post("/auto-products/", json={"name": "Shop", "opensAt": "08:30:00+02:00"})

        assert resp.status_code == 422
        assert resp.json()["detail"]["code"] == "boundary_validation"
        assert "08:30" not in resp.text

    def test_an_aware_time_in_an_update_body_answers_422(self, client: TestClient) -> None:
        item_id = client.post("/auto-products/", json={"name": "Shop"}).json()["id"]

        resp = client.patch(f"/auto-products/{item_id}", json={"opensAt": "08:30:00+02:00"})

        assert resp.status_code == 422
        assert resp.json()["detail"]["code"] == "boundary_validation"
        assert "08:30" not in resp.text
