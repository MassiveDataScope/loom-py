"""One configuration, several Lambdas: ``app.entry`` and ``lambda.functions``."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi import FastAPI

from loom.aws_lambda import LambdaHTTPAdapter, LambdaHTTPHandler
from loom.composition import create_app
from loom.core.config import ConfigError
from loom.core.identity import Identity
from loom.rest.auth import RequestCredentials
from loom.rest.compiler import InterfaceCompilationError
from loom.rest.fastapi import FastAPIAdapter
from tests.unit.aws_lambda.http._clients import lambda_client
from tests.unit.aws_lambda.http._contract_app import bearer

pytestmark = pytest.mark.filterwarnings("ignore:No function of lambda.functions serves")

_TEST = "test"
_PUBLIC_AND_ADMIN = {
    "public_api": {"http": ["ItemInterface"]},
    "admin_api": {"http": ["OpsInterface"]},
}

Edit = Callable[[dict[str, Any], Path], None]


@pytest.fixture
def project(contract_project: Callable[..., str]) -> Callable[..., str]:
    """Write the contract project with *entry*, *functions* and any further edits."""

    def write(entry: str, functions: dict[str, Any], *edits: Edit) -> str:
        path = Path(contract_project())
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        config["app"]["entry"] = entry
        config["lambda"] = {"functions": functions}
        for edit in edits:
            edit(config, path.parent)
        path.write_text(yaml.safe_dump(config), encoding="utf-8")
        return str(path)

    return write


@pytest.fixture
def serve() -> Iterator[Callable[..., Any]]:
    """Compose from configuration alone; close every handler after the test."""
    closes: list[Callable[[], None]] = []

    def compose(path: str, **options: Any) -> Any:
        handler = create_app(path, **{"environment": _TEST, **options})
        close = getattr(handler, "close", None)
        if close is not None:
            closes.append(close)
        return handler

    yield compose
    for close in closes:
        close()


def _module_of(config: dict[str, Any]) -> str:
    return str(config["app"]["discovery"]["interfaces"]["modules"][0])


def _yaml_catalog(config: dict[str, Any], _root: Path) -> None:
    route = {"use_case": f"{_module_of(config)}:GetItem", "method": "GET", "path": "/{item_id}"}
    config["app"]["rest"]["interfaces"] = {"catalog": {"prefix": "/yaml", "routes": [route]}}


def test_an_http_function_serves_only_its_interfaces(
    project: Callable[..., str], serve: Callable[..., Any]
) -> None:
    call = lambda_client(serve(project("public_api", _PUBLIC_AND_ADMIN)))

    assert call("GET", "/items/12", headers=bearer("alice")).status == 200
    assert call("GET", "/ops/admin", headers=bearer("alice", "admin")).status == 404


def test_a_yaml_interface_is_named_by_its_key(
    project: Callable[..., str], serve: Callable[..., Any]
) -> None:
    path = project("public_api", {"public_api": {"http": ["catalog"]}}, _yaml_catalog)

    call = lambda_client(serve(path))

    assert call("GET", "/yaml/12", headers=bearer("alice")).status == 200
    assert call("GET", "/items/12", headers=bearer("alice")).status == 404


def test_the_entry_variable_selects_the_function_in_production(
    project: Callable[..., str], serve: Callable[..., Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def silence_cursor_check(config: dict[str, Any], _root: Path) -> None:
        config["checks"] = {"silenced": ["loom.cursors.E001"]}

    path = project("public_api", _PUBLIC_AND_ADMIN, silence_cursor_check)
    monkeypatch.setenv("LOOM__APP__ENTRY", "admin_api")

    call = lambda_client(serve(path, environment="production"))

    assert call("GET", "/items/12", headers=bearer("alice")).status == 404
    assert call("GET", "/ops/admin", headers=bearer("alice", "admin")).status == 200


class _HeaderAuthenticator:
    """Authenticates exactly the requests carrying ``x-test: ok``."""

    name = "test"
    provides_roles = True

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        if credentials.headers.get("x-test") != "ok":
            return None
        return Identity(subject="tester", mechanism=self.name)


_OK = {"x-test": "ok"}


@pytest.mark.parametrize(
    ("auth", "in_code", "headers", "status"),
    [
        pytest.param(None, False, {}, 401, id="application-jwt-applies"),
        pytest.param({}, False, {}, 200, id="function-without-authentication"),
        pytest.param({"custom": True}, True, _OK, 200, id="code-authenticates"),
        pytest.param({"custom": True}, True, {}, 401, id="code-refuses"),
    ],
)
def test_a_function_auth_replaces_the_application_auth(
    project: Callable[..., str],
    serve: Callable[..., Any],
    auth: dict[str, Any] | None,
    in_code: bool,
    headers: dict[str, str],
    status: int,
) -> None:
    function: dict[str, Any] = {"http": ["ItemInterface"]}
    if auth is not None:
        function["auth"] = auth
    path = project("public_api", {"public_api": function})
    authenticator = _HeaderAuthenticator() if in_code else None
    adapter = LambdaHTTPAdapter(function="public_api", authenticator=authenticator)

    reply = lambda_client(serve(path, adapter=adapter))("GET", "/items/12", headers=headers)

    assert reply.status == status


def test_the_entry_selects_the_function_of_an_adapter_built_in_code(
    project: Callable[..., str], serve: Callable[..., Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    functions = {
        "public_api": {"http": ["ItemInterface"], "auth": {"custom": True}},
        "admin_api": {"http": ["OpsInterface"]},
    }
    path = project("admin_api", functions)
    monkeypatch.setenv("LOOM__APP__ENTRY", "public_api")
    adapter = LambdaHTTPAdapter(authenticator=_HeaderAuthenticator())

    call = lambda_client(serve(path, adapter=adapter))

    assert call("GET", "/items/12", headers=_OK).status == 200
    assert call("GET", "/ops/admin", headers=_OK).status == 404


def _disable(method: str, route: str) -> Edit:
    def edit(config: dict[str, Any], _root: Path) -> None:
        config["app"]["rest"]["disable_routes"] = [{"method": method, "path": route}]

    return edit


def test_a_route_disabled_in_another_function_still_composes(
    project: Callable[..., str], serve: Callable[..., Any]
) -> None:
    path = project("public_api", _PUBLIC_AND_ADMIN, _disable("GET", "/ops/explode"))

    call = lambda_client(serve(path))

    assert call("GET", "/items/12", headers=bearer("alice")).status == 200


def test_a_disabled_route_no_function_declares_is_refused(
    project: Callable[..., str], serve: Callable[..., Any]
) -> None:
    path = project("public_api", _PUBLIC_AND_ADMIN, _disable("GET", "/ops/nowhere"))

    with pytest.raises(InterfaceCompilationError, match="matches no route"):
        serve(path)


def test_an_explicit_adapter_wins_over_the_entry(
    project: Callable[..., str], serve: Callable[..., Any]
) -> None:
    path = project("no_such_function", {})

    assert isinstance(serve(path, adapter=FastAPIAdapter()), FastAPI)


@pytest.mark.parametrize(
    ("entry", "built"), [("fastapi", FastAPI), ("lambda_http", LambdaHTTPHandler)]
)
def test_a_built_in_http_entry_selects_its_adapter(
    project: Callable[..., str], serve: Callable[..., Any], entry: str, built: type
) -> None:
    assert isinstance(serve(project(entry, {})), built)


def _manifest_discovery(config: dict[str, Any], root: Path) -> None:
    root.joinpath("lambda_manifest.py").write_text(
        f"from {_module_of(config)} import Item, ItemInterface\n"
        "MODELS = [Item]\nINTERFACES = [ItemInterface]\nJOBS = []\n",
        encoding="utf-8",
    )
    config["app"]["discovery"] = {"mode": "manifest", "manifest": {"module": "lambda_manifest"}}


@pytest.mark.parametrize("entry", ["worker", "lambda_sqs"])
def test_an_sqs_entry_serves_the_jobs(
    project: Callable[..., str], serve: Callable[..., Any], entry: str
) -> None:
    handler = serve(project(entry, {"worker": {"sqs": True}}, _manifest_discovery))

    assert handler({"Records": []}, None) == {"batchItemFailures": []}


def test_an_interface_no_function_serves_is_warned_about(
    project: Callable[..., str], serve: Callable[..., Any]
) -> None:
    path = project("public_api", {"public_api": {"http": ["ItemInterface"]}})

    with pytest.warns(UserWarning, match="serves OpsInterface"):
        serve(path)


def _second_item_interface(config: dict[str, Any], root: Path) -> None:
    root.joinpath("other_contract.py").write_text(
        "from loom.rest.model import RestInterface\n"
        "class ItemInterface(RestInterface[object]):\n"
        "    prefix = '/other'\n"
        "    routes = ()\n",
        encoding="utf-8",
    )
    config["app"]["discovery"]["interfaces"]["modules"].append("other_contract")


_ITEM_ONLY = {"public_api": {"http": ["ItemInterface"]}}


@pytest.mark.parametrize(
    ("entry", "functions", "edits", "function", "message"),
    [
        pytest.param(
            "public_api",
            {"public_api": {"http": ["MissingInterface"]}},
            (),
            None,
            "'MissingInterface': no discovered RestInterface",
            id="unknown-interface",
        ),
        pytest.param(
            "public_api",
            _ITEM_ONLY,
            (_second_item_interface,),
            None,
            "'ItemInterface', which is ambiguous",
            id="ambiguous-interface",
        ),
        pytest.param(
            "public_api",
            {"public_api": {"http": ["ItemInterface"]}, "other": {"http": ["ItemInterface"]}},
            (),
            None,
            r"'ItemInterface' is served by both lambda\.functions\.(other|public_api) and",
            id="interface-in-two-functions",
        ),
        pytest.param(
            "public_api",
            {"public_api": {"http": ["ItemInterface"], "sqs": True}},
            (),
            None,
            "exactly one of",
            id="http-and-sqs",
        ),
        pytest.param("public_api", {"public_api": {}}, (), None, "exactly one of", id="neither"),
        pytest.param(
            "worker",
            {"worker": {"sqs": True, "auth": {}}},
            (),
            None,
            "applies to an http function only",
            id="auth-on-sqs",
        ),
        pytest.param(
            "missing",
            _ITEM_ONLY,
            (),
            None,
            r"declares no function 'missing' \(declared: public_api\)",
            id="undeclared-entry",
        ),
        pytest.param(
            "lambda_http",
            {"worker": {"sqs": True}},
            (),
            "worker",
            "is an sqs function, not an http one",
            id="http-adapter-on-sqs-function",
        ),
        pytest.param(
            "lambda_http",
            _ITEM_ONLY,
            (),
            "typo",
            r"declares no function 'typo'",
            id="http-adapter-on-undeclared-function",
        ),
    ],
)
def test_a_misdeclared_function_fails_at_startup(
    project: Callable[..., str],
    serve: Callable[..., Any],
    entry: str,
    functions: dict[str, Any],
    edits: tuple[Edit, ...],
    function: str | None,
    message: str,
) -> None:
    path = project(entry, functions, *edits)
    options = {} if function is None else {"adapter": LambdaHTTPAdapter(function=function)}

    with pytest.raises(ConfigError, match=message):
        serve(path, **options)


def test_without_an_entry_create_app_needs_an_adapter(
    contract_project: Callable[..., str], serve: Callable[..., Any]
) -> None:
    path = contract_project()

    with pytest.raises(ConfigError, match="adapter= or app.entry"):
        serve(path)
