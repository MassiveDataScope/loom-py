"""A product's own interface over ``ReadAuditLog.for_store``: composition and serving (US2).

The reader is a fake bound by a caller module, so no Postgres is reached;
the generated route on real Postgres is covered by the RLS suite (T14).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from loom.cli.app import app as cli
from loom.composition import create_app
from loom.composition.build import load_source
from loom.core.audit import AuditEntry, AuditLogReader
from loom.core.config import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.identity import Identity
from loom.core.persistence.stores import StoreKey, store_binding
from loom.core.repository.abc import CursorResult, QuerySpec
from loom.rest.auth import RequestCredentials
from loom.rest.fastapi import FastAPIAdapter
from loom.rest.fastapi.adapter import served_routes
from loom.serverless.aws.lambda_ import LambdaAdapter

_SOURCE = """\
from typing import Annotated

from loom.core.audit import AuditLogReader, AuditSubjects, ReadAuditLog
from loom.core.persistence.stores import InStore, StoreKey
from loom.core.repository.abc import PaginationMode
from loom.rest.model import RestInterface, RestRoute
__DECLARATIONS__

class TeamHistory(RestInterface[object]):
    prefix = "/history"
    routes = (
        RestRoute(
            use_case=__USE_CASE__,
            method="GET",
            path="/",
            pagination_mode=PaginationMode.CURSOR,
        ),
    )
"""

_SUBCLASS = """

class TeamRead(ReadAuditLog.for_store("default")):
    \"\"\"A product's own read, deriving from the store's use case.\"\"\"
"""

_DIRECT = """

class DirectRead(ReadAuditLog):
    def __init__(
        self,
        reader: Annotated[AuditLogReader, InStore(StoreKey("default"))],
        subjects: AuditSubjects,
    ) -> None:
        super().__init__(reader, subjects, StoreKey("default"))
"""

_USE_CASES = {
    "for-store": ("", 'ReadAuditLog.for_store("default")'),
    "subclass": (_SUBCLASS, "TeamRead"),
}

_SQLITE: dict[str, Any] = {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}
_GUARDED: dict[str, Any] = {
    "backend": "sqlalchemy",
    "url": "postgresql+asyncpg://app@db/app",
    "schema": {"mode": "external", "name": "app", "guard": "app_guard"},
}
_REFUSAL = "for_store(default) requires stores.default.schema.audit.reading.enabled"
_HEADERS = {"x-test": "ok"}
_ENTRY = AuditEntry(
    at=datetime(2026, 1, 1, tzinfo=UTC),
    id=7,
    op="U",
    schema_name="app",
    table="orders",
    row_key="42",
    changed={"status": "paid"},
    subject="tester",
    mechanism="test",
    on_behalf_of="",
    trace_id="t-1",
    elevated=(),
    origin="api",
)


class _FakeReader:
    def __init__(self) -> None:
        self.queries: list[QuerySpec] = []

    async def read(self, query: QuerySpec, actors: object) -> CursorResult[AuditEntry]:
        self.queries.append(query)
        return CursorResult(items=(_ENTRY,), next_cursor=None, has_next=False)


class _HeaderAuthenticator:
    name = "test"
    provides_roles = True

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        if credentials.headers.get("x-test") != "ok":
            return None
        return Identity(subject="tester", mechanism=self.name)


def _bind_fake_reader(reader: _FakeReader) -> Callable[[LoomContainer], None]:
    def bind_fake_reader(container: LoomContainer) -> None:
        container.register(
            store_binding(AuditLogReader, StoreKey("default")),
            lambda: reader,
            scope=Scope.APPLICATION,
        )

    return bind_fake_reader


@pytest.fixture
def project(tmp_path: Path) -> Callable[..., str]:
    """Write the product module and its config; *reading* goes under ``stores.default``."""

    def write(
        reading: dict[str, Any] | None = None,
        *,
        store: dict[str, Any] = _SQLITE,
        use_case: tuple[str, str] = _USE_CASES["for-store"],
        **sections: Any,
    ) -> str:
        module = f"audit_product_{tmp_path.name}".replace("-", "_")
        declarations, expression = use_case
        source = _SOURCE.replace("__DECLARATIONS__", declarations).replace(
            "__USE_CASE__", expression
        )
        (tmp_path / f"{module}.py").write_text(source, encoding="utf-8")
        schema: dict[str, Any] = dict(store.get("schema", {}))
        if reading is not None:
            schema["audit"] = {"reading": reading}
        store = {**store, "schema": schema}
        config: dict[str, Any] = {
            "app": {
                "name": "audit-product",
                "code_path": ".",
                "discovery": {
                    "mode": "interfaces",
                    "interfaces": {"modules": [module], "warn_recommended": False},
                },
                "rest": {"auth": {"custom": True}, "openapi_url": None},
            },
            "stores": {"default": store},
            "persistence": {"cursors": {"signing": "ephemeral"}},
            **sections,
        }
        path = tmp_path / "app.yaml"
        path.write_text(yaml.safe_dump(config), encoding="utf-8")
        return str(path)

    return write


def _compose(path: str, adapter: object, reader: _FakeReader | None = None) -> Any:
    modules = () if reader is None else (_bind_fake_reader(reader),)
    return create_app(path, adapter=adapter, modules=modules)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# US2 #4: a store without reading.enabled fails composition
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "adapter",
    [
        pytest.param(lambda: FastAPIAdapter(authenticator=_HeaderAuthenticator()), id="fastapi"),
        pytest.param(lambda: LambdaAdapter(authenticator=_HeaderAuthenticator()), id="lambda"),
    ],
)
@pytest.mark.parametrize("reading", [None, {"enabled": False}], ids=["absent", "disabled"])
@pytest.mark.parametrize("use_case", list(_USE_CASES))
def test_for_store_without_reading_enabled_fails_composition(
    project: Callable[..., str],
    adapter: Callable[[], object],
    reading: dict[str, Any] | None,
    use_case: str,
) -> None:
    """US2 #4; a product subclass of the store's use case reads that store too (S1)."""
    with pytest.raises(ConfigError, match=re.escape(_REFUSAL)):
        _compose(project(reading, use_case=_USE_CASES[use_case]), adapter())


@pytest.mark.parametrize("use_case", list(_USE_CASES))
def test_the_deploy_manifest_reports_the_same_refusal(
    project: Callable[..., str], use_case: str
) -> None:
    functions = {"api": {"trigger": {"source": "http", "interfaces": ["TeamHistory"]}}}
    lambda_section = {"handler": "entry.handler", "functions": functions}
    path = project(
        None,
        use_case=_USE_CASES[use_case],
        checks={"silenced": ["loom.cursors.E001"]},
        **{"lambda": lambda_section},
    )

    result = CliRunner().invoke(cli, ["deploy", "manifest", "--env", "test", path])

    assert result.exit_code != 0
    assert _REFUSAL in result.output


# ---------------------------------------------------------------------------
# US2 #2, composition half: a guarded store with reading enabled, on FastAPI
#
# Serving through a real reader on both runtimes is T14's (real Postgres): a
# SQLite store refuses reading.enabled, and Lambda opens the application, so
# runs the store's startup checks, when it is built.
# ---------------------------------------------------------------------------


def test_the_product_interface_composes_and_serves_the_fake_page_on_fastapi(
    project: Callable[..., str],
) -> None:
    """Built without connecting; the client runs no lifespan, so no startup check connects."""
    reader = _FakeReader()
    path = project({"enabled": True}, store=_GUARDED)
    app = _compose(path, FastAPIAdapter(authenticator=_HeaderAuthenticator()), reader)
    client = TestClient(app)

    page = client.get("/history/", headers=_HEADERS)
    filtered = client.get("/history/", params={"subject": "bob"}, headers=_HEADERS)

    assert page.status_code == 200, page.text
    assert [item["id"] for item in page.json()["items"]] == [7]
    assert len(reader.queries) == 1
    assert filtered.status_code == 400
    assert filtered.json()["detail"]["code"] == "unsupported_query"


def _served_models(path: str) -> list[str]:
    source = load_source(
        path, config_dir=None, environment="test", code_path=None, resolvers=(), caller="test"
    )
    discovered, _ = served_routes(source)
    return [model.__name__ for model in discovered.models]


def test_a_product_subclass_on_a_reading_store_folds_the_store_audit_model(
    project: Callable[..., str],
) -> None:
    path = project({"enabled": True}, store=_GUARDED, use_case=_USE_CASES["subclass"])

    assert _served_models(path) == ["AuditEntry_default"]


def test_a_direct_read_audit_log_subclass_is_refused(project: Callable[..., str]) -> None:
    """S1: a subclass that bypasses for_store would read a store nothing checked."""
    path = project({"enabled": True}, store=_GUARDED, use_case=(_DIRECT, "DirectRead"))

    with pytest.raises(ConfigError, match=r"DirectRead.*ReadAuditLog\.for_store"):
        _compose(path, FastAPIAdapter(authenticator=_HeaderAuthenticator()))
