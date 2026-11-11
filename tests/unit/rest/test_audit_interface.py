"""The generated ``audit_<store>`` interface: naming, prefixes, model folding and startup refusals.

A store whose ``schema.audit.reading`` is enabled with a ``route`` gets one
generated interface, ``audit_<store>``, serving ``GET <prefix>/``. These tests
compose without opening the application: a guarded Postgres store connects only
when it opens, which the real-Postgres suite covers (T14).
"""

from __future__ import annotations

import re
import tempfile
from collections.abc import Callable, Collection, Mapping
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from fastapi import FastAPI

from loom.composition import CompositionSource, compose_application, create_app
from loom.composition.build import load_source
from loom.core.audit import ReadAuditLog, audit_entry_for
from loom.core.config import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.persistence.stores import StoreKey
from loom.rest.audit import AUDIT_FILTERS, build_audit_interfaces, fold_audit_models
from loom.rest.compiler import RouteSources, published_routes
from loom.rest.fastapi import FastAPIAdapter
from loom.rest.fastapi.adapter import served_routes
from loom.rest.model import RestInterface
from loom.serverless.aws.lambda_ import LambdaAdapter

pytestmark = pytest.mark.filterwarnings("ignore:No function of lambda.functions serves")

_SECRET_PATH = Path(tempfile.mkdtemp()) / "hs.key"
_SECRET_PATH.write_text("unit-test-secret-of-thirty-two-bytes", encoding="utf-8")
_JWT: dict[str, Any] = {
    "secret_path": str(_SECRET_PATH),
    "algorithms": ["HS256"],
    "audience": "loom-api",
}
_POSTGRES = "postgresql+asyncpg://app@db/app"
_ROLES = ["auditor"]

_SOURCE = """\
from loom.core.audit import ReadAuditLog, audit_entry_for
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class Note(BaseModel):
    __tablename__ = "audit_interface_notes"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class Ping(UseCase[Note, str]):
    async def execute(self) -> str:
        return "pong"


class PingInterface(RestInterface[str]):
    prefix = "__PREFIX__"
    routes = (RestRoute(use_case=Ping, method="GET", path="/"),)
"""

_PRODUCT_AUDIT = """

class LedgerAudit(RestInterface[object]):
    prefix = "/history"
    routes = (RestRoute(use_case=ReadAuditLog.for_store("ledger"), method="GET", path="/"),)
"""

_AUTO_OVER_AUDIT = """

class AuditCrud(RestInterface[audit_entry_for("ledger")]):
    prefix = "/crud"
    auto = True
"""

_SHADOWING = """

class PingName(UseCase[Note, str]):
    async def execute(self, name: str) -> str:
        return name


class ByName(RestInterface[str]):
    prefix = "/audit"
    routes = (RestRoute(use_case=PingName, method="GET", path="/{name}/"),)
"""

_NAMED_LIKE_THE_AUDIT = """

audit_ledger = type("audit_ledger", (PingInterface,), {"prefix": "/elsewhere"})
"""


def _reading(*, route: bool = True, prefix: str | None = None) -> dict[str, Any]:
    reading: dict[str, Any] = {"enabled": True}
    if route:
        declared: dict[str, Any] = {"requires_roles": _ROLES}
        if prefix is not None:
            declared["prefix"] = prefix
        reading["route"] = declared
    return reading


def _guarded(name: str, reading: dict[str, Any] | None = None, url: str = _POSTGRES) -> Any:
    schema: dict[str, Any] = {"mode": "external", "name": name, "guard": f"{name}_guard"}
    if reading is not None:
        schema["audit"] = {"reading": reading}
    return {"backend": "sqlalchemy", "url": url, "schema": schema}


Write = Callable[..., str]


@pytest.fixture
def write(tmp_path: Path) -> Write:
    """Write a project serving ``PingInterface`` and *stores*; return its config path."""
    counter = iter(range(1000))

    def project(
        stores: Mapping[str, Any],
        *,
        prefix: str = "/ping",
        extra: str = "",
        auth: dict[str, Any] | None = None,
        interfaces: dict[str, Any] | None = None,
        functions: dict[str, Any] | None = None,
        entry: str | None = None,
    ) -> str:
        directory = tmp_path / f"p{next(counter)}"
        directory.mkdir()
        module = f"audit_interface_app_{tmp_path.name}_{directory.name}".replace("-", "_")
        source = _SOURCE.replace("__PREFIX__", prefix) + extra
        (directory / f"{module}.py").write_text(source, encoding="utf-8")
        rest: dict[str, Any] = {"auth": {"jwt": _JWT} if auth is None else auth}
        if interfaces is not None:
            rest["interfaces"] = interfaces
        config: dict[str, Any] = {
            "app": {
                "name": "audit-demo",
                "code_path": ".",
                "discovery": {
                    "mode": "interfaces",
                    "interfaces": {"modules": [module], "warn_recommended": False},
                },
                "rest": rest,
            },
            "stores": {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}},
            "persistence": {"cursors": {"signing": "ephemeral"}},
        }
        config["stores"].update(stores)
        if functions is not None:
            config["lambda"] = {"functions": functions}
        if entry is not None:
            config["app"]["entry"] = entry
        path = directory / "app.yaml"
        path.write_text(yaml.safe_dump(config).replace("__MODULE__", module), encoding="utf-8")
        return str(path)

    return project


def _source(path: str) -> CompositionSource:
    return load_source(
        path, config_dir=None, environment="test", code_path=None, resolvers=(), caller="test"
    )


def _named(path: str) -> dict[str, tuple[type[RestInterface[Any]], ...]]:
    """The interface name map ``served_routes`` hands a selector."""
    seen: dict[str, tuple[type[RestInterface[Any]], ...]] = {}

    def every(
        named: Mapping[str, tuple[type[RestInterface[Any]], ...]],
    ) -> Collection[type[RestInterface[Any]]]:
        seen.update(named)
        return [found for interfaces in named.values() for found in interfaces]

    served_routes(_source(path), every)
    return seen


def _routes(path: str) -> RouteSources:
    return served_routes(_source(path))[1]


def _http(*interfaces: str, **options: Any) -> dict[str, Any]:
    return {"trigger": {"source": "http", "interfaces": list(interfaces), **options}}


def _fastapi(path: str) -> object:
    return create_app(path, adapter=FastAPIAdapter())


def _lambda(path: str) -> object:
    return create_app(path, adapter=LambdaAdapter())


def _lambda_function(path: str) -> object:
    return create_app(path, adapter=LambdaAdapter(function="api"))


_RUNTIMES = pytest.mark.parametrize(
    "compose",
    [_fastapi, _lambda, _lambda_function],
    ids=["fastapi", "lambda", "lambda-function"],
)
_BOTH_RUNTIMES = pytest.mark.parametrize("compose", [_fastapi, _lambda], ids=["fastapi", "lambda"])
# The served function lists neither colliding interface: collisions are refused all the same.
_FUNCTIONS = {"api": _http("PingInterface")}
_NAME_TAKEN = "audit_ledger, generated by schema.audit.reading.route, collides"
_PREFIX_TAKEN = "audit_ledger mounts at '/audit/ledger', which PingInterface already mounts"


# ---------------------------------------------------------------------------
# FR-010: the name map, the prefix, and the route
# ---------------------------------------------------------------------------


def test_a_reading_route_adds_audit_store_to_the_name_map(write: Write) -> None:
    named = _named(write({"ledger": _guarded("ledger", _reading())}))

    assert "audit_ledger" in named
    assert named["audit_ledger"][0].prefix == "/audit/ledger"


def test_two_stores_serve_one_audit_route_each(write: Write) -> None:
    path = write({"a": _guarded("a", _reading()), "b": _guarded("b", _reading())})

    published = published_routes(_routes(path))

    assert ("GET", "/audit/a/") in published
    assert ("GET", "/audit/b/") in published


def test_the_route_prefix_overrides_the_default(write: Write) -> None:
    path = write({"ledger": _guarded("ledger", _reading(prefix="/ops/log"))})

    routes = _routes(path)

    assert ("GET", "/ops/log/") in published_routes(routes)
    assert routes.audit_prefixes == ("/ops/log",)


def test_the_generated_route_serves_get_only_with_the_audit_contract(write: Write) -> None:
    interface = _named(write({"ledger": _guarded("ledger", _reading())}))["audit_ledger"][0]

    (route,) = interface.routes
    assert (route.method, route.path) == ("GET", "/")
    assert route.use_case is ReadAuditLog.for_store("ledger")
    assert interface.requires_roles == tuple(_ROLES)
    assert route.filterable == AUDIT_FILTERS
    assert (route.sortable, route.max_limit, route.allow_pagination_override) == (
        ("at",),
        100,
        True,
    )


def test_no_reading_route_gives_no_audit_interface_nor_schema(write: Write) -> None:
    """M4, US1 #1: neither a store without ``reading`` nor one without ``route``."""
    path = write({"ledger": _guarded("ledger"), "books": _guarded("books", _reading(route=False))})

    named = _named(path)
    app = _fastapi(path)

    assert not [name for name in named if name.startswith("audit_")]
    assert isinstance(app, FastAPI)
    document = app.openapi()
    assert not [path for path in document["paths"] if path.startswith("/audit")]
    assert not [
        name for name in document.get("components", {}).get("schemas", {}) if "AuditEntry" in name
    ]


# ---------------------------------------------------------------------------
# N7 and r2 M5: what build_audit_interfaces reads
# ---------------------------------------------------------------------------


def test_reading_on_a_store_another_backend_serves_is_refused(write: Write) -> None:
    events = {
        "backend": "dynamodb",
        "region": "eu-west-1",
        "table": "events",
        "url": "http://localhost:8000",
        "schema": {"audit": {"reading": _reading()}},
    }
    source = _source(write({"events": events}))

    with pytest.raises(ConfigError, match=r"stores\.events.*schema\.audit\.reading"):
        build_audit_interfaces(source.catalog)


def test_only_the_reading_section_is_decoded(write: Write) -> None:
    """An unresolvable connection URL is never read."""
    source = _source(
        write({"ledger": _guarded("ledger", _reading(), url="${secrets:/nowhere/ledger-url}")})
    )

    built = build_audit_interfaces(source.catalog)

    assert list(built.interfaces) == ["audit_ledger"]


# ---------------------------------------------------------------------------
# M11, US1 #15, N9: collisions
# ---------------------------------------------------------------------------


def _yaml_audit_ledger() -> dict[str, Any]:
    route = {"use_case": "__MODULE__:Ping", "method": "GET", "path": "/"}
    return {"audit_ledger": {"prefix": "/yaml", "routes": [route]}}


@_RUNTIMES
@pytest.mark.parametrize(
    ("options", "message"),
    [
        pytest.param({"interfaces": _yaml_audit_ledger()}, _NAME_TAKEN, id="yaml-key"),
        pytest.param({"extra": _NAMED_LIKE_THE_AUDIT}, _NAME_TAKEN, id="python-class"),
        pytest.param({"prefix": "/audit/ledger"}, _PREFIX_TAKEN, id="prefix"),
        pytest.param({"prefix": "/audit/ledger/"}, _PREFIX_TAKEN, id="prefix-trailing-slash"),
    ],
)
def test_a_colliding_name_or_prefix_is_refused(
    write: Write, compose: Callable[[str], object], options: dict[str, Any], message: str
) -> None:
    path = write(
        {"ledger": _guarded("ledger", _reading())},
        functions=_FUNCTIONS,
        **options,
    )

    with pytest.raises(ConfigError, match=re.escape(message)):
        compose(path)


def test_a_templated_product_route_that_would_capture_the_audit_route_is_refused(
    write: Write,
) -> None:
    """S2: ``GET /audit/{name}/`` would answer ``GET /audit/ledger/`` under its own roles."""
    path = write({"ledger": _guarded("ledger", _reading())}, extra=_SHADOWING)

    with pytest.raises(ConfigError, match=r"/audit/\{name\}/.*/audit/ledger"):
        served_routes(_source(path))


def test_a_shorter_product_prefix_coexists_with_the_audit_prefix(write: Write) -> None:
    """N9: only equal prefixes collide."""
    path = write({"default": _guarded("app", _reading())}, prefix="/audit")

    published = published_routes(_routes(path))

    assert {("GET", "/audit/"), ("GET", "/audit/default/")} <= set(published)


# ---------------------------------------------------------------------------
# Option C: the audit model joins its store's models
# ---------------------------------------------------------------------------


def test_a_reading_route_composes_the_store_through_its_audit_model(write: Write) -> None:
    source = _source(write({"ledger": _guarded("ledger", _reading())}))

    application = compose_application(source, FastAPIAdapter())

    model = audit_entry_for("ledger")
    assert model in application.discovered.models
    assert source.catalog.models_by_store(application.discovered.models)[StoreKey("ledger")] == (
        model,
    )
    application.kernel.factory.verify()


def test_without_reading_no_audit_model_is_discovered(write: Write) -> None:
    source = _source(write({"ledger": _guarded("ledger")}))

    discovered, _ = served_routes(source)

    assert not [m for m in discovered.models if m.__name__.startswith("AuditEntry")]
    assert list(source.catalog.models_by_store(discovered.models)) == [StoreKey("default")]


def test_a_product_use_case_adds_the_audit_model_of_its_store(write: Write) -> None:
    source = _source(write({"ledger": _guarded("ledger", _reading(route=False))}))
    readings = build_audit_interfaces(source.catalog).readings
    use_case = ReadAuditLog.for_store("ledger")
    discovered = DiscoveryResult(models=(), use_cases=cast(Any, (use_case,)), interfaces=())

    folded = fold_audit_models(discovered, readings)

    assert folded.models == (audit_entry_for("ledger"),)


# ---------------------------------------------------------------------------
# Sites 6 and 8: auto-CRUD and OpenAPI
# ---------------------------------------------------------------------------


def test_an_auto_interface_over_the_audit_model_is_refused_naming_the_marker(
    write: Write,
) -> None:
    path = write({"ledger": _guarded("ledger", _reading(route=False))}, extra=_AUTO_OVER_AUDIT)

    with pytest.raises(RuntimeError, match="NotMaterialized"):
        served_routes(_source(path))


def test_two_stores_publish_one_schema_each(write: Write) -> None:
    app = _fastapi(write({"a": _guarded("a", _reading()), "b": _guarded("b", _reading())}))

    assert isinstance(app, FastAPI)
    schemas = app.openapi()["components"]["schemas"]
    assert {"AuditEntry_a", "AuditEntry_b"} <= set(schemas)


# ---------------------------------------------------------------------------
# FR-011, US1 #5 and #8, r3 N6: the authenticated prefix
# ---------------------------------------------------------------------------


@_BOTH_RUNTIMES
def test_a_reading_route_without_an_authenticator_is_refused(
    write: Write, compose: Callable[[str], object]
) -> None:
    path = write({"ledger": _guarded("ledger", _reading())}, auth={})

    with pytest.raises(ConfigError, match=r"/audit/ledger.*authenticated caller"):
        compose(path)


@_BOTH_RUNTIMES
@pytest.mark.parametrize("excluded", ["/audit/ledger", "/audit/ledger/", "/audit/ledger/x"])
def test_an_excluded_audit_path_is_refused(
    write: Write, compose: Callable[[str], object], excluded: str
) -> None:
    auth = {"jwt": {**_JWT, "exclude_paths": [excluded]}}
    path = write({"ledger": _guarded("ledger", _reading())}, auth=auth)

    with pytest.raises(ConfigError, match=re.escape(excluded)):
        compose(path)


@_BOTH_RUNTIMES
def test_a_jwt_without_an_audience_is_refused(
    write: Write, compose: Callable[[str], object]
) -> None:
    """Security review S5: as for the SQL endpoint, a token must be bound to this service."""
    jwt = {key: value for key, value in _JWT.items() if key != "audience"}
    path = write({"ledger": _guarded("ledger", _reading())}, auth={"jwt": jwt})

    with pytest.raises(ConfigError, match=r"/audit/ledger.*audience"):
        compose(path)


def test_the_audit_prefixes_travel_in_the_route_sources(write: Write) -> None:
    path = write({"a": _guarded("a", _reading()), "b": _guarded("b", _reading(prefix="/b"))})

    assert _routes(path).audit_prefixes == ("/audit/a", "/b")


# ---------------------------------------------------------------------------
# Lambda functions: a function folds only the audit reads its own interfaces reach
# ---------------------------------------------------------------------------

_DEFAULT_AUDIT = """

from loom.core.repository.abc import PaginationMode


class DefaultAudit(RestInterface[object]):
    prefix = "/history"
    routes = (
        RestRoute(
            use_case=ReadAuditLog.for_store("default"),
            method="GET",
            path="/",
            pagination_mode=PaginationMode.CURSOR,
        ),
    )
"""

_TWO_FUNCTIONS = {"a": _http("PingInterface"), "b": _http("DefaultAudit")}
_DEFAULT_REFUSAL = "for_store(default) requires stores.default.schema.audit.reading.enabled"


def _function_models(path: str, function: str) -> list[str]:
    recipe = LambdaAdapter(function=function).recipe(_source(path))
    return [model.__name__ for model in recipe.discovered.models]


def test_a_function_not_reaching_an_audit_read_does_not_fold_its_model(write: Write) -> None:
    """Another function's product audit read builds no audit store here."""
    default = _guarded("app", {"enabled": True})
    path = write({"default": default}, extra=_DEFAULT_AUDIT, functions=_TWO_FUNCTIONS)

    assert "AuditEntry_default" not in _function_models(path, "a")
    assert "AuditEntry_default" in _function_models(path, "b")


def test_another_function_reading_a_store_without_reading_does_not_fail_this_one(
    write: Write, tmp_path: Path
) -> None:
    """US2 #4 refuses the function that reads the store, and only that one."""
    path = write({}, extra=_DEFAULT_AUDIT, functions=_TWO_FUNCTIONS)

    handler = create_app(path, adapter=LambdaAdapter(function="a"))
    getattr(handler, "close")()  # noqa: B009
    with pytest.raises(ConfigError, match=re.escape(_DEFAULT_REFUSAL)):
        create_app(path, adapter=LambdaAdapter(function="b"))

    config = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    config["lambda"]["handler"] = "entry.handler"
    config["checks"] = {"silenced": ["loom.cursors.E001", "loom.rest.E003", "loom.rest.W001"]}
    Path(path).write_text(yaml.safe_dump(config), encoding="utf-8")
    manifest_a = _manifest(path, "a")
    manifest_b = _manifest(path, "b")
    assert manifest_a.exit_code == 0, manifest_a.output
    assert manifest_b.exit_code != 0
    assert _DEFAULT_REFUSAL in manifest_b.output


def _manifest(path: str, function: str) -> Any:
    from typer.testing import CliRunner

    from loom.cli.app import app as cli

    return CliRunner().invoke(
        cli, ["deploy", "manifest", "--env", "test", "--function", function, path]
    )
