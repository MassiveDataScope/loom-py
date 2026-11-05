"""``loom deploy manifest``: what each Lambda of ``lambda.functions`` needs deployed, as JSON."""

from __future__ import annotations

import copy
import json
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from loom.cli.app import app
from loom.core.config.secrets import SecretsManagerResolver
from loom.core.config.ssm import SsmResolver

pytestmark = [
    pytest.mark.timeout(30),
    pytest.mark.filterwarnings("ignore:Discovery mode 'interfaces' is convenient"),
]

runner = CliRunner()

_API_SOURCE = """\
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class Order(BaseModel):
    __tablename__ = "manifest_orders"
    id: int = ColumnField(primary_key=True, autoincrement=True)


class GetOrder(UseCase[Order, int]):
    async def execute(self, order_id: int) -> int:
        return order_id


class ListOrders(UseCase[Order, int]):
    async def execute(self) -> int:
        return 0


class OrdersInterface(RestInterface[Order]):
    prefix = "/orders"
    routes = (
        RestRoute(use_case=GetOrder, method="get", path="/{order_id}"),
        RestRoute(use_case=ListOrders, method="GET", path=""),
        RestRoute(use_case=ListOrders, method="DELETE", path=""),
    )


class AdminInterface(RestInterface[Order]):
    prefix = "/admin"
    routes = (RestRoute(use_case=ListOrders, method="POST", path="/purge"),)
"""

_QUEUE_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs"
_SENTINEL = "resolved-secret-value"

Project = Callable[..., Path]


def _config(package: str) -> dict[str, Any]:
    admin_auth = {"jwt": {"secret_path": "${secrets:/shop/admin-key}", "algorithms": ["HS256"]}}
    return {
        "app": {
            "name": "shop",
            "job_service": f"{package}.jobs:Backend",
            "discovery": {"mode": "modules", "modules": {"include": [f"{package}.api"]}},
            "rest": {
                "docs_url": None,
                "redoc_url": None,
                "openapi_url": None,
                "auth": {"jwt": {"secret_path": "${secrets:/shop/jwt.path}"}},
                "disable_routes": [{"method": "DELETE", "path": "/orders"}],
                "interfaces": {
                    "health": {
                        "prefix": "/status",
                        "routes": [
                            {"use_case": f"{package}.api:ListOrders", "method": "GET", "path": ""}
                        ],
                    }
                },
            },
        },
        "stores": {
            "default": {"backend": "sqlalchemy", "url": "${ssm: '/shop/db-url' }"},
            "reports": {"backend": "none"},
        },
        "sqs": {"queues": {"jobs": _QUEUE_URL}},
        "checks": {"silenced": ["loom.cursors.E001"], "modules": [f"{package}.checks"]},
        "lambda": {
            "handler": "entry.handler",
            "functions": {
                "public": {"http": ["OrdersInterface", "health"]},
                "admin": {"http": ["AdminInterface"], "auth": admin_auth},
                "worker": {"sqs": True, "queue": "jobs", "batch_size": 5},
            },
        },
    }


def _shared(name: str) -> dict[str, Any]:
    return {
        "code_path": "src",
        "variables": {"LOOM_ENV": "production", "LOOM__APP__ENTRY": name},
        "handler": "entry.handler",
        "queues": {"jobs": _QUEUE_URL},
        "ssm_parameters": ["/shop/db-url"],
        "stores": {"default": "sqlalchemy"},
    }


def _expected(package: str) -> dict[str, Any]:
    modules = [f"{package}.api", f"{package}.checks", f"{package}.jobs"]
    app_auth = {"jwt": {"secret_path": "${secrets:/shop/jwt.path}"}}
    return {
        "manifest_version": 1,
        "environment": "production",
        "functions": {
            "admin": {
                **_shared("admin"),
                "auth": {
                    "jwt": {"algorithms": ["HS256"], "secret_path": "${secrets:/shop/admin-key}"}
                },
                "event": {"type": "http", "routes": [{"method": "POST", "path": "/admin/purge"}]},
                "modules": modules,
                "secrets": ["/shop/admin-key", "/shop/jwt"],
            },
            "public": {
                **_shared("public"),
                "auth": app_auth,
                "event": {
                    "type": "http",
                    "routes": [
                        {"method": "GET", "path": "/orders"},
                        {"method": "GET", "path": "/orders/{order_id}"},
                        {"method": "GET", "path": "/status"},
                    ],
                },
                "modules": modules,
                "secrets": ["/shop/jwt"],
            },
            "worker": {
                **_shared("worker"),
                "auth": None,
                "event": {
                    "type": "sqs",
                    "queue": "jobs",
                    "url": _QUEUE_URL,
                    "batch_size": 5,
                    "batching_window_s": None,
                    "max_concurrency": None,
                    "report_batch_item_failures": True,
                },
                "modules": modules,
                "secrets": ["/shop/jwt"],
            },
        },
    }


@pytest.fixture
def package(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """A unique application package name, unloaded and off the import path afterwards."""
    name = f"manifest_shop_{uuid.uuid4().hex}"
    monkeypatch.setattr(sys, "path", list(sys.path))
    yield name
    for module in [module for module in sys.modules if module.startswith(name)]:
        del sys.modules[module]


@pytest.fixture
def project(tmp_path: Path, package: str) -> Project:
    """Write the application code and a ``base.yaml`` built from the config *edit* returns."""
    code = tmp_path / "src" / package
    code.mkdir(parents=True)
    (code / "__init__.py").write_text("", encoding="utf-8")
    (code / "api.py").write_text(_API_SOURCE, encoding="utf-8")
    (code / "checks.py").write_text("", encoding="utf-8")

    def write(edit: Callable[[dict[str, Any]], dict[str, Any]] = lambda config: config) -> Path:
        path = tmp_path / "base.yaml"
        path.write_text(yaml.safe_dump(edit(_config(package)), sort_keys=False), encoding="utf-8")
        return path

    return write


def _manifest(config: Path, *options: str) -> Any:
    return runner.invoke(app, ["deploy", "manifest", "--env", "production", *options, str(config)])


def test_the_manifest_describes_every_function(project: Project, package: str) -> None:
    result = _manifest(project())

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == _expected(package)


def test_the_same_configuration_written_in_another_order_gives_the_same_bytes(
    project: Project,
) -> None:
    def reversed_order(node: Any) -> Any:
        if isinstance(node, dict):
            return {key: reversed_order(node[key]) for key in reversed(list(node))}
        return node

    first = _manifest(project())
    second = _manifest(project(reversed_order))

    assert (first.exit_code, second.exit_code) == (0, 0), first.stderr + second.stderr
    assert first.stdout == second.stdout


def test_one_function_is_described_alone(project: Project, package: str) -> None:
    result = _manifest(project(), "--function", "worker")

    assert result.exit_code == 0, result.stderr
    manifest = json.loads(result.stdout)
    assert manifest["functions"] == {"worker": _expected(package)["functions"]["worker"]}


@pytest.mark.parametrize(
    ("discovery", "modules"),
    [
        (
            {"mode": "modules", "modules": {"include": ["shop.jobs", "shop.api"]}},
            ["shop.api", "shop.jobs"],
        ),
        ({"mode": "manifest", "manifest": {"module": "shop.manifest"}}, ["shop.manifest"]),
        ({"mode": "interfaces", "interfaces": {"modules": ["shop.api"]}}, ["shop.api"]),
        ({}, []),
    ],
    ids=["modules", "manifest", "interfaces", "default"],
)
def test_a_function_lists_the_modules_its_discovery_imports(
    project: Project, package: str, discovery: dict[str, Any], modules: list[str]
) -> None:
    def discovering(config: dict[str, Any]) -> dict[str, Any]:
        config["app"]["discovery"] = discovery
        return config

    result = _manifest(project(discovering), "--function", "worker")

    assert result.exit_code == 0, result.stderr
    extra = [f"{package}.checks", f"{package}.jobs"]
    assert json.loads(result.stdout)["functions"]["worker"]["modules"] == sorted(modules + extra)


def test_no_secret_or_parameter_is_resolved(
    project: Project, monkeypatch: pytest.MonkeyPatch
) -> None:
    resolved: list[str] = []

    def record(_resolver: object, key: str, _root_: object = None) -> str:
        resolved.append(key)
        return _SENTINEL

    monkeypatch.setattr(SecretsManagerResolver, "resolve", record)
    monkeypatch.setattr(SsmResolver, "resolve", record)

    result = _manifest(project())

    assert result.exit_code == 0, result.stderr
    assert resolved == []
    assert _SENTINEL not in result.output


def test_a_function_does_not_list_the_secrets_of_another_function(project: Project) -> None:
    manifest = json.loads(_manifest(project(), "--function", "public").stdout)

    assert "/shop/admin-key" not in manifest["functions"]["public"]["secrets"]


def test_a_failing_deploy_check_prints_its_findings_and_no_manifest(project: Project) -> None:
    def unsilenced(config: dict[str, Any]) -> dict[str, Any]:
        del config["checks"]
        return config

    result = _manifest(project(unsilenced))

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "loom.cursors.E001" in result.stderr


def _without_handler(config: dict[str, Any]) -> dict[str, Any]:
    del config["lambda"]["handler"]
    return config


def _without_functions(config: dict[str, Any]) -> dict[str, Any]:
    config["lambda"]["functions"] = {}
    return config


def _worker(**changes: Any) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def edit(config: dict[str, Any]) -> dict[str, Any]:
        edited = copy.deepcopy(config)
        worker = edited["lambda"]["functions"]["worker"]
        worker.update(changes)
        for key in [key for key, value in changes.items() if value is None]:
            del worker[key]
        return edited

    return edit


def _nested_secret(config: dict[str, Any]) -> dict[str, Any]:
    config["app"]["rest"]["auth"]["jwt"]["secret_path"] = "${secrets:${oc.env:JWT_SECRET}}"
    return config


@pytest.mark.parametrize(
    ("edit", "options", "message"),
    [
        (_without_handler, (), "lambda.handler is missing"),
        (_without_functions, (), "declares no function to deploy"),
        (_worker(queue=None), (), "worker.queue must name the queue it consumes"),
        (_worker(queue="unknown"), (), "worker.queue must name the queue it consumes"),
        (_nested_secret, (), "app.rest.auth.jwt.secret_path nests a placeholder"),
        (lambda config: config, ("--function", "billing"), "declares no function 'billing'"),
    ],
    ids=["no-handler", "no-function", "no-queue", "unknown-queue", "nested", "unknown-function"],
)
def test_a_manifest_that_cannot_be_complete_is_refused(
    project: Project,
    edit: Callable[[dict[str, Any]], dict[str, Any]],
    options: tuple[str, ...],
    message: str,
) -> None:
    result = _manifest(project(edit), *options)

    assert result.exit_code == 1
    assert result.stdout == ""
    assert message in result.stderr
