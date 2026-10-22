"""Capability gate for auto-CRUD interfaces at bootstrap.

The routes an interface mounts depend on the capabilities of the repository
class that will serve its model: the explicit ``repository_for`` class, else
the backend's default repository type.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import msgspec
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import loom.core.repository as repository_package
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.discovery.base import DiscoveryResult
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.environment import Environment
from loom.core.model import BaseModel, ColumnField
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.abc import (
    BulkCreatable,
    Creatable,
    Listable,
    Readable,
    UnsupportedQuery,
)
from loom.core.repository.registry import repository_for
from loom.core.use_case.constants import KEY_SEPARATOR, CrudOp
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.registry import model_entity_key
from loom.core.use_case.use_case import UseCase
from loom.rest.autocrud import build_auto_routes, crud_op_of
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi import auto
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi.auto import _AppConfig, create_app
from loom.rest.fastapi.auto import _build_bootstrap as _build_bootstrap_in
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestApiDefaults, RestInterface, RestRoute
from tests.unit.rest._fixture_app import write_project

# The fixture apps opt out of cursor signing, which a deployed environment refuses.
_build_bootstrap = functools.partial(_build_bootstrap_in, environment=Environment.TEST)

_DYNAMODB_PERSISTENCE = {
    "backend": "dynamodb",
    "dynamodb": {
        "region": "eu-west-1",
        "table": "capability_records",
        "url": "http://localhost:8000",
    },
}
_KEY_OPS = {CrudOp.GET, CrudOp.CREATE, CrudOp.UPDATE, CrudOp.DELETE}


class CapabilityRecord(BaseModel):
    __tablename__ = "capability_records_fixture"

    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=50)


class OtherRecord(BaseModel):
    __tablename__ = "other_records_fixture"

    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=50)


class BulkOnlyRecord(BaseModel):
    __tablename__ = "bulk_only_records_fixture"

    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=50)


@repository_for(BulkOnlyRecord)
class _BulkOnlyRepository(BulkCreatable[BulkOnlyRecord]):
    async def create_many(
        self, data: Sequence[msgspec.Struct]
    ) -> tuple[BulkOnlyRecord, ...]:  # pragma: no cover - never invoked
        raise AssertionError("not exercised")


class ReadCreateRecord(BaseModel):
    __tablename__ = "read_create_records_fixture"

    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=50)


@repository_for(ReadCreateRecord)
class _ReadCreateRepository(Creatable[ReadCreateRecord], Readable[ReadCreateRecord]):
    async def get_by_id(
        self, obj_id: Any, profile: str = "default"
    ) -> ReadCreateRecord | None:  # pragma: no cover - never invoked
        raise AssertionError("not exercised")

    async def get_by(
        self, field: str, value: Any, profile: str = "default"
    ) -> ReadCreateRecord | None:  # pragma: no cover - never invoked
        raise AssertionError("not exercised")

    async def create(self, data: msgspec.Struct) -> ReadCreateRecord:  # pragma: no cover
        raise AssertionError("not exercised")


class TestSupportedOpsFollowTheRegisteredRepository:
    def test_a_bulk_only_repository_yields_no_route(self) -> None:
        assert build_auto_routes(BulkOnlyRecord, ()) == ()

    def test_ops_come_from_the_capabilities_in_declaration_order(self) -> None:
        routes = build_auto_routes(ReadCreateRecord, ())
        assert tuple(crud_op_of(route.use_case) for route in routes) == (CrudOp.CREATE, CrudOp.GET)


def _ctx(persistence: Mapping[str, object] | None = None, **sections: object) -> ConfigContext:
    cursors = {"cursors": {"signing": "ephemeral"}}
    return ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "database": {"url": "sqlite+aiosqlite:///"},
            "persistence": {**cursors, **(persistence or {})},
            **sections,
        }
    )


def _discovery(*interfaces: type, models: tuple[type[BaseModel], ...] = ()) -> DiscoveryResult:
    """Mirror discovery: the use cases are those the interface routes reference."""
    typed = tuple(cast(type[RestInterface[Any]], klass) for klass in interfaces)
    use_cases = tuple(dict.fromkeys(route.use_case for iface in typed for route in iface.routes))
    return DiscoveryResult(
        models=models or (CapabilityRecord,), use_cases=use_cases, interfaces=typed
    )


def _ops(interface: type[RestInterface[Any]]) -> set[CrudOp]:
    return {crud_op_of(route.use_case) for route in interface.routes}


@pytest.fixture(autouse=True)
def aws_test_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture
def discover(monkeypatch: pytest.MonkeyPatch) -> Any:
    def _install(*interfaces: type, models: tuple[type[BaseModel], ...] = ()) -> None:
        result = _discovery(*interfaces, models=models)
        monkeypatch.setattr(auto, "_build_discovery_result", lambda _cfg: result)

    return _install


class TestDefaultInclude:
    def test_dynamodb_mounts_only_the_key_operations(self, discover: Any) -> None:
        class RecordsOnDynamo(RestInterface[CapabilityRecord]):
            prefix = "/records"
            auto = True

        discover(RecordsOnDynamo)

        _build_bootstrap(_AppConfig(name="demo"), _ctx(persistence=_DYNAMODB_PERSISTENCE))

        assert _ops(RecordsOnDynamo) == _KEY_OPS

    def test_booting_again_on_sqlalchemy_restores_the_list_route(self, discover: Any) -> None:
        class RecordsOnBoth(RestInterface[CapabilityRecord]):
            prefix = "/records"
            auto = True

        discover(RecordsOnBoth)

        _build_bootstrap(_AppConfig(name="demo"), _ctx(persistence=_DYNAMODB_PERSISTENCE))
        assert CrudOp.LIST not in _ops(RecordsOnBoth)

        _build_bootstrap(_AppConfig(name="demo"), _ctx(persistence={"backend": "sqlalchemy"}))
        assert _ops(RecordsOnBoth) == _KEY_OPS | {CrudOp.LIST}

    def test_a_pruned_use_case_mounted_elsewhere_survives(self, discover: Any) -> None:
        """Only a use case no interface mounts any more leaves the discovery result."""
        shared_list = next(
            r.use_case
            for r in build_auto_routes(CapabilityRecord, ())
            if crud_op_of(r.use_case) is CrudOp.LIST
        )

        class RecordsPruned(RestInterface[CapabilityRecord]):
            prefix = "/records"
            auto = True

        class RecordsListedByHand(RestInterface[str]):
            prefix = "/record-listing"
            routes = (
                RestRoute(use_case=shared_list, method="GET", path="/"),
                RestRoute(use_case=_LookupByName, method="GET", path="/lookup"),
            )

        class OthersPruned(RestInterface[OtherRecord]):
            prefix = "/others"
            auto = True

        discover(
            RecordsPruned,
            RecordsListedByHand,
            OthersPruned,
            models=(CapabilityRecord, OtherRecord),
        )

        runtime, _wiring, discovered, _python_interfaces = _build_bootstrap(
            _AppConfig(name="demo"), _ctx(persistence=_DYNAMODB_PERSISTENCE)
        )

        assert shared_list in discovered.use_cases
        assert _LookupByName in discovered.use_cases
        keys = set(runtime.registry.keys())
        assert f"{model_entity_key(CapabilityRecord)}{KEY_SEPARATOR}list" in keys
        assert f"{model_entity_key(OtherRecord)}{KEY_SEPARATOR}list" not in keys
        assert f"{model_entity_key(OtherRecord)}{KEY_SEPARATOR}get" in keys

    def test_no_supported_operation_is_a_compilation_error(self, discover: Any) -> None:
        class BulkOnly(RestInterface[BulkOnlyRecord]):
            prefix = "/bulk-only"
            auto = True

        discover(BulkOnly, models=(BulkOnlyRecord,))
        app_config = _AppConfig(name="demo")
        ctx = _ctx(persistence={"backend": "sqlalchemy"})

        with pytest.raises(RuntimeError, match="BulkOnly") as exc_info:
            _build_bootstrap(app_config, ctx)

        message = str(exc_info.value)
        assert "BulkOnlyRecord" in message
        assert "'sqlalchemy'" in message
        assert "'list'" in message
        assert "'get'" in message

    def test_backend_none_is_a_compilation_error(self, discover: Any) -> None:
        class RecordsOnNothing(RestInterface[CapabilityRecord]):
            prefix = "/records"
            auto = True

        discover(RecordsOnNothing)
        app_config = _AppConfig(name="demo")
        ctx = _ctx(persistence={"backend": "none"})

        with pytest.raises(RuntimeError, match="RecordsOnNothing") as exc_info:
            _build_bootstrap(app_config, ctx)

        message = str(exc_info.value)
        assert "CapabilityRecord" in message
        assert "'none'" in message


class TestExplicitInclude:
    def test_absent_operation_names_model_op_and_backend(self, discover: Any) -> None:
        class RecordListing(RestInterface[CapabilityRecord]):
            prefix = "/records"
            auto = True
            include = ("get", "list")

        discover(RecordListing)
        app_config = _AppConfig(name="demo")
        ctx = _ctx(persistence=_DYNAMODB_PERSISTENCE)

        with pytest.raises(RuntimeError, match="RecordListing") as exc_info:
            _build_bootstrap(app_config, ctx)

        message = str(exc_info.value)
        assert "CapabilityRecord" in message
        assert "'list'" in message
        assert "'dynamodb'" in message

    def test_supported_operations_are_kept_as_declared(self, discover: Any) -> None:
        class RecordReads(RestInterface[CapabilityRecord]):
            prefix = "/records"
            auto = True
            include = ("get", "create")

        discover(RecordReads)

        _build_bootstrap(_AppConfig(name="demo"), _ctx(persistence=_DYNAMODB_PERSISTENCE))

        assert [crud_op_of(r.use_case) for r in RecordReads.routes] == [CrudOp.GET, CrudOp.CREATE]


class TestCrudOpOf:
    def test_maps_every_generated_use_case_to_its_operation(self) -> None:
        ops = [crud_op_of(route.use_case) for route in build_auto_routes(CapabilityRecord, ())]

        assert ops == list(CrudOp)

    def test_rejects_a_hand_written_use_case(self) -> None:
        class Ping(UseCase[CapabilityRecord, str]):
            async def execute(self) -> str:  # pragma: no cover - never invoked
                return "pong"

        with pytest.raises(ValueError, match="Ping"):
            crud_op_of(Ping)


class _CountRecords(UseCase[Any, int]):
    read_only = True

    def __init__(self, records: Listable[CapabilityRecord]) -> None:
        self._records = records

    async def execute(self) -> int:  # pragma: no cover - never invoked
        return 0


class _CountInterface(RestInterface[int]):
    prefix = "/record-count"
    routes = (RestRoute(use_case=_CountRecords, method="GET", path="/"),)


class TestHandWrittenCapabilityDependency:
    def test_fails_at_create_app_on_a_backend_without_it(
        self, tmp_path: Path, discover: Any
    ) -> None:
        discover(_CountInterface)
        config_path = write_project(tmp_path, persistence=_DYNAMODB_PERSISTENCE, database=None)

        with pytest.raises(ResolutionError, match="_CountRecords") as exc_info:
            create_app(config_path)

        message = str(exc_info.value)
        assert "records" in message
        assert "Listable" in message
        assert "CapabilityRecord" in message

    def test_boots_on_a_backend_that_binds_it(self, tmp_path: Path, discover: Any) -> None:
        discover(_CountInterface)
        config_path = write_project(tmp_path, persistence={"backend": "sqlalchemy"})

        assert create_app(config_path) is not None


class _EchoRecord(UseCase[CapabilityRecord, str]):
    read_only = True

    async def execute(self) -> str:  # pragma: no cover - never invoked
        return "pong"


class _EchoInterface(RestInterface[str]):
    prefix = "/echo"
    routes = (RestRoute(use_case=_EchoRecord, method="GET", path="/"),)


def test_model_bound_use_case_on_backend_none_fails_at_create_app(
    tmp_path: Path, discover: Any
) -> None:
    """The implicit ``main_repo`` has no provider on ``none``; refuse at boot, not per request."""
    discover(_EchoInterface)
    config_path = write_project(tmp_path, persistence={"backend": "none"}, database=None)

    with pytest.raises(ResolutionError, match="_EchoRecord") as exc_info:
        create_app(config_path)

    message = str(exc_info.value)
    assert "main_repo" in message
    assert "CapabilityRecord" in message


class _LookupByName(UseCase[Any, str]):
    read_only = True

    async def execute(self) -> str:
        raise UnsupportedQuery("dynamodb", "CapabilityRecord", "only the key 'id' can be queried")


class _LookupInterface(RestInterface[str]):
    prefix = "/lookups"
    routes = (RestRoute(use_case=_LookupByName, method="GET", path="/"),)


def _lookup_client() -> TestClient:
    compiler = UseCaseCompiler()
    compiler.compile(_LookupByName)
    factory = UseCaseFactory(LoomContainer())
    factory.register(_LookupByName)
    app = FastAPI()
    register_error_handlers(app)
    bind_interfaces(
        app,
        RestInterfaceCompiler(compiler, defaults=RestApiDefaults()).compile(_LookupInterface),
        factory,
        RuntimeExecutor(compiler),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    return TestClient(app, raise_server_exceptions=False)


def test_unsupported_query_answers_400_with_its_code() -> None:
    response = _lookup_client().get("/lookups/")

    assert response.status_code == 400
    body = response.json()["detail"]
    assert body["code"] == "unsupported_query"
    assert "dynamodb" in body["message"]
    assert "CapabilityRecord" in body["message"]


def test_repository_package_never_raises_not_implemented() -> None:
    """SC-003: argument-shaped gaps raise ``UnsupportedQuery``, never ``NotImplementedError``."""
    root = Path(cast(str, repository_package.__file__)).parent

    offenders = [
        path for path in root.rglob("*.py") if "raise NotImplementedError" in path.read_text()
    ]

    assert offenders == []
