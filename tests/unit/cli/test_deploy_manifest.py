"""``loom deploy manifest``: what each Lambda of ``lambda.functions`` needs deployed, as JSON."""

from __future__ import annotations

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
from loom.core.command import Command
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case import Input
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


class InvoiceArrived(Command, frozen=True):
    number: str


class ImportInvoice(Job[None]):
    async def execute(self, invoice: InvoiceArrived = Input()) -> None:
        return None
"""

_QUEUE_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs"
_ALERTS_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-alerts"
_SENTINEL = "resolved-secret-value"

Project = Callable[..., Path]
Edit = Callable[[dict[str, Any]], dict[str, Any]]


def _config(package: str) -> dict[str, Any]:
    admin_auth = {
        "jwt": {
            "secret_path": "${secrets:/shop/admin-key}",
            "audience": "${ssm:/shop/admin-audience}",
            "algorithms": ["HS256"],
        }
    }
    return {
        "app": {
            "name": "shop",
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
            "default": {"backend": "sqlalchemy", "url": "${ ssm : '/shop/db-url' }"},
            "reports": {"backend": "none"},
        },
        "sqs": {"queues": {"jobs": _QUEUE_URL, "alerts": _ALERTS_URL}},
        "checks": {"silenced": ["loom.cursors.E001"]},
        "lambda": {
            "handler": "entry.handler",
            "functions": {
                "public": _trigger("http", interfaces=["OrdersInterface", "health"]),
                "admin": _trigger("http", interfaces=["AdminInterface"], auth=admin_auth),
                "worker": _trigger("jobs", queue="jobs", batch_size=5),
                "invoices_in": _trigger(
                    "eventbridge",
                    job="ImportInvoice",
                    identity={"subject": "svc:invoices", "roles": ["invoice_importer"]},
                    maximum_retry_attempts=1,
                ),
                "alerts": _trigger("sns", via="sqs", queue="alerts", job="ImportInvoice"),
            },
        },
    }


def _trigger(source: str, **declared: Any) -> dict[str, Any]:
    return {"trigger": {"source": source, **declared}}


_JOB_SECRETS = {"secrets": ["/shop/jwt"], "ssm_parameters": ["/shop/db-url"]}


def _expected() -> dict[str, Any]:
    app_auth = {"jwt": {"secret_path": "${secrets:/shop/jwt.path}"}}
    admin_auth = {
        "jwt": {
            "algorithms": ["HS256"],
            "audience": "${ssm:/shop/admin-audience}",
            "secret_path": "${secrets:/shop/admin-key}",
        }
    }
    return {
        "manifest_version": 2,
        "environment": "production",
        "handler": "entry.handler",
        "code_path": "src",
        "queues": {"alerts": _ALERTS_URL, "jobs": _QUEUE_URL},
        "functions": {
            "admin": {
                "source": "http",
                "via": None,
                "event": {
                    "routes": [{"method": "POST", "path": "/admin/purge"}],
                    "auth": admin_auth,
                },
                "secrets": ["/shop/admin-key", "/shop/jwt"],
                "ssm_parameters": ["/shop/admin-audience", "/shop/db-url"],
            },
            "alerts": {
                "source": "sns",
                "via": "sqs",
                "event": {
                    "queue": "alerts",
                    "batch_size": None,
                    "batching_window_s": None,
                    "max_concurrency": None,
                    "report_batch_item_failures": True,
                    "dead_letter_required": True,
                    "job": "ImportInvoice",
                    "identity": None,
                    "raw_message_delivery": False,
                },
                **_JOB_SECRETS,
            },
            "invoices_in": {
                "source": "eventbridge",
                "via": None,
                "event": {
                    "job": "ImportInvoice",
                    "identity": {
                        "subject": "svc:invoices",
                        "roles": ["invoice_importer"],
                        "attributes": {},
                    },
                    "on_failure": {"maximum_retry_attempts": 1, "maximum_event_age_s": 21600},
                },
                **_JOB_SECRETS,
            },
            "public": {
                "source": "http",
                "via": None,
                "event": {
                    "routes": [
                        {"method": "GET", "path": "/orders"},
                        {"method": "GET", "path": "/orders/{order_id}"},
                        {"method": "GET", "path": "/status"},
                    ],
                    "auth": app_auth,
                },
                "secrets": ["/shop/jwt"],
                "ssm_parameters": ["/shop/db-url"],
            },
            # The worker lists app.rest.auth's secret: it resolves the whole app section.
            "worker": {
                "source": "jobs",
                "via": None,
                "event": {
                    "queue": "jobs",
                    "batch_size": 5,
                    "batching_window_s": None,
                    "max_concurrency": None,
                    "report_batch_item_failures": True,
                    "dead_letter_required": True,
                },
                "secrets": ["/shop/jwt"],
                "ssm_parameters": ["/shop/db-url"],
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

    def write(edit: Edit = lambda config: config) -> Path:
        path = tmp_path / "base.yaml"
        path.write_text(yaml.safe_dump(edit(_config(package)), sort_keys=False), encoding="utf-8")
        return path

    return write


def _manifest(config: Path, *options: str) -> Any:
    return runner.invoke(app, ["deploy", "manifest", "--env", "production", *options, str(config)])


def test_the_manifest_describes_every_function(project: Project) -> None:
    result = _manifest(project())

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == _expected()


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


def test_one_function_is_described_alone(project: Project) -> None:
    result = _manifest(project(), "--function", "worker")

    assert result.exit_code == 0, result.stderr
    manifest = json.loads(result.stdout)
    assert manifest["functions"] == {"worker": _expected()["functions"]["worker"]}


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


def test_a_failing_deploy_check_prints_its_findings_and_no_manifest(project: Project) -> None:
    def unsilenced(config: dict[str, Any]) -> dict[str, Any]:
        del config["checks"]
        return config

    result = _manifest(project(unsilenced))

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "loom.cursors.E001" in result.stderr


_DELETE = object()


def _set(key: str, value: object) -> Edit:
    """An edit setting the dotted *key* to *value*, or deleting it for ``_DELETE``."""

    def edit(config: dict[str, Any]) -> dict[str, Any]:
        *parents, leaf = key.split(".")
        node = config
        for part in parents:
            node = node.setdefault(part, {})
        if value is _DELETE:
            del node[leaf]
        else:
            node[leaf] = value
        return config

    return edit


_JWT_SECRET = "app.rest.auth.jwt.secret_path"
_WORKER_ONLY = ("--function", "worker")


@pytest.mark.parametrize(
    ("edit", "options", "message"),
    [
        (_set("lambda.handler", _DELETE), (), "lambda.handler is missing"),
        (_set("lambda.handler", "${oc.env:HANDLER}"), (), "lambda.handler must be written"),
        (_set("lambda.handler", "entry"), (), "must be 'module.attribute'"),
        (_set("sqs.queues.jobs", "${ssm:/shop/url}"), (), "sqs.queues.jobs must be written"),
        (_set("lambda.functions", {}), (), "declares no function to deploy"),
        (
            _set("lambda.functions.worker.trigger.queue", _DELETE),
            (),
            "worker.trigger.queue must name the queue",
        ),
        (
            _set("lambda.functions.worker.trigger.queue", "unknown"),
            (),
            "worker.trigger.queue must name the queue",
        ),
        (
            _set("lambda.functions.alerts.trigger.queue", _DELETE),
            (),
            "alerts.trigger.queue must name the queue",
        ),
        (
            _set("lambda.functions.invoices_in.trigger.job", "MissingJob"),
            (),
            "'MissingJob', which is no discovered Job",
        ),
        (
            _set("lambda.functions.bad-name", _trigger("schedule", job="ImportInvoice")),
            (),
            "lambda.functions.bad-name must be named by a Python identifier",
        ),
        (
            _set("lambda.functions.función", _trigger("schedule", job="ImportInvoice")),
            (),
            "lambda.functions.función must be named by a Python identifier",
        ),
        (_set(_JWT_SECRET, "${secrets:${oc.env:JWT}}"), (), f"{_JWT_SECRET} nests a placeholder"),
        (
            _set(_JWT_SECRET, "${ secrets : /shop/${oc.env:JWT} }"),
            (),
            f"{_JWT_SECRET} nests a placeholder",
        ),
        (_set(_JWT_SECRET, "${secrets:/shop/*}"), (), "'/shop/*' holds a wildcard"),
        (_set(_JWT_SECRET, "${ssm:/shop/key?}"), (), "'/shop/key?' holds a wildcard"),
        (
            _set("app.rest.disable_routes", [{"method": "GET", "path": "/nowhere"}]),
            (),
            "matches no route declared by a Python interface",
        ),
        (
            _set("app.discovery.mode", "interfaces"),
            _WORKER_ONLY,
            "'interfaces' is not one a worker scans",
        ),
        (lambda config: config, ("--function", "billing"), "declares no function 'billing'"),
    ],
    ids=[
        "no-handler",
        "handler-placeholder",
        "handler-malformed",
        "queue-placeholder",
        "no-function",
        "no-queue",
        "unknown-queue",
        "no-queue-via-sqs",
        "undiscovered-job",
        "function-name",
        "non-ascii-function-name",
        "nested",
        "nested-spaced",
        "wildcard-secret",
        "wildcard-parameter",
        "unmatched-disabled-route",
        "non-worker-discovery",
        "unknown-function",
    ],
)
def test_a_manifest_that_cannot_be_complete_is_refused(
    project: Project, edit: Edit, options: tuple[str, ...], message: str
) -> None:
    result = _manifest(project(edit), *options)

    assert result.exit_code == 1
    assert result.stdout == ""
    assert message in result.stderr


@pytest.mark.parametrize(
    ("written", "named"),
    [
        ("${ secrets : /shop/spaced }", "/shop/spaced"),
        ("${secrets: '/shop/quoted' }", "/shop/quoted"),
        ('${ secrets:"/shop/json.key"}', "/shop/json"),
    ],
    ids=["spaces", "single-quotes", "double-quotes-json-tail"],
)
def test_a_placeholder_written_with_spaces_or_quotes_is_named(
    project: Project, written: str, named: str
) -> None:
    result = _manifest(project(_set(_JWT_SECRET, written)), "--function", "public")

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout)["functions"]["public"]["secrets"] == [named]


def test_no_variable_is_read(project: Project, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LEAK", _SENTINEL)
    leaking = _set(_JWT_SECRET, "${secrets:/shop/${oc.env:LEAK}}")

    result = _manifest(project(leaking))

    assert result.exit_code == 1
    assert _SENTINEL not in result.output


def test_the_findings_go_to_stderr_and_the_manifest_alone_to_stdout(project: Project) -> None:
    result = _manifest(project(_set("app.rest.docs_url", "/docs")))

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout)["manifest_version"] == 2
    assert "environment: production" in result.stderr
    assert "silenced: loom.cursors.E001" in result.stderr
    assert "WARNING loom.rest.W001" in result.stderr
