"""An application ``LoomError`` may declare the HTTP answer the mapper sends."""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Mapping
from enum import StrEnum
from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from loom.composition import create_app
from loom.core.errors import (
    LoomError,
    NotFound,
    PostCommitError,
    RateLimited,
    RuleViolation,
    SystemError,
)
from loom.core.errors.codes import ErrorCode
from loom.core.tracing import get_trace_id
from loom.rest.errors import HttpErrorMapper
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.serverless.aws.lambda_ import LambdaAdapter
from loom.serverless.aws.lambda_.triggers.http import LambdaHTTPHandler
from loom.testing import LambdaTestClient

_LEAKY = "postgresql://db.internal:5432/orders"
_QUERY_MESSAGE = "upstream refused the sale quote"
_FAILED_QUERY = "query/tasar-venta"
_FIXED_500 = "An unexpected error occurred"
_FIXED_503 = "The service is temporarily unavailable"
_TRACE = "parity-trace-0001"
_NON_IDENTIFIER = re.compile(r"\W", re.ASCII)


class _QueryCode(StrEnum):
    BAD_GATEWAY = "bad_gateway"


def _detail(exc: HTTPException) -> dict[str, Any]:
    assert isinstance(exc.detail, dict)
    return exc.detail


class QueryFailed(LoomError):
    """The pricing query did not answer; the client may see which query."""

    http_status = HTTPStatus.BAD_GATEWAY
    public_message = True

    def __init__(self, message: str = _QUERY_MESSAGE, *, failed: str = _FAILED_QUERY) -> None:
        super().__init__(message, code=_QueryCode.BAD_GATEWAY)
        self.failed = failed

    def public_detail(self) -> Mapping[str, Any]:
        return {"failed": self.failed}


class _HiddenUpstream(LoomError):
    """A 502 whose message names a host and must stay off the wire."""

    http_status = HTTPStatus.BAD_GATEWAY

    def __init__(self) -> None:
        super().__init__(_LEAKY, code=_QueryCode.BAD_GATEWAY)

    def public_detail(self) -> Mapping[str, Any]:
        return {"failed": _FAILED_QUERY}


def test_a_query_failure_answers_502_with_exactly_its_public_fields() -> None:
    """The real case: 502, the error's own text, and the failed query name."""
    exc = HttpErrorMapper().to_http(QueryFailed())

    assert exc.status_code == HTTPStatus.BAD_GATEWAY
    assert _detail(exc) == {
        "code": "bad_gateway",
        "message": _QUERY_MESSAGE,
        "failed": _FAILED_QUERY,
        "trace_id": get_trace_id(),
    }


def test_a_5xx_keeps_the_fixed_message_when_public_message_is_off() -> None:
    """Extra fields are public; the message is not, unless the error opts in."""
    exc = HttpErrorMapper().to_http(_HiddenUpstream())

    assert exc.status_code == HTTPStatus.BAD_GATEWAY
    assert _detail(exc) == {
        "code": "bad_gateway",
        "message": _FIXED_500,
        "failed": _FAILED_QUERY,
        "trace_id": get_trace_id(),
    }


def test_a_5xx_shows_its_own_message_when_public_message_is_on() -> None:
    error = _HiddenUpstream()
    error.public_message = True

    detail = _detail(HttpErrorMapper().to_http(error))

    assert detail["message"] == _LEAKY


def test_a_public_field_the_mapper_did_not_set_is_kept() -> None:
    """``entity`` is loom's only when this response already carries it."""

    class _NamesTheQuery(SystemError):
        def __init__(self) -> None:
            super().__init__("down")

        def public_detail(self) -> Mapping[str, Any]:
            return {"entity": "Query"}

    detail = _detail(HttpErrorMapper().to_http(_NamesTheQuery()))

    assert detail == {
        "code": "system_error",
        "message": _FIXED_500,
        "trace_id": get_trace_id(),
        "entity": "Query",
    }


class _OverridesMessage(LoomError):
    """A 5xx that tries to replace the message loom writes."""

    public_message = True

    def __init__(self) -> None:
        super().__init__("leaked-message-host", code=ErrorCode.SYSTEM_ERROR)

    def public_detail(self) -> Mapping[str, Any]:
        return {"message": "leaked-field-value"}


class _OverridesEntity(NotFound):
    """A not-found that tries to replace the entity loom writes."""

    public_message = True

    def public_detail(self) -> Mapping[str, Any]:
        return {"entity": "leaked-field-value"}


def _assert_internal_fallback(exc: HTTPException, error: LoomError) -> None:
    assert exc.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert exc.headers is None
    assert _detail(exc) == {
        "code": error.code,
        "message": _FIXED_500,
        "trace_id": get_trace_id(),
    }


def _assert_misdeclaration_logged(
    caplog: pytest.LogCaptureFixture, class_name: str, problem: str, *hidden: str
) -> None:
    records = [record for record in caplog.records if record.name == "loom.rest.errors"]
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None
    assert class_name in record.getMessage()
    assert problem in record.getMessage()
    logged = record.getMessage()
    if record.exc_info is not None:
        logged = f"{logged} {record.exc_info[1]}"
    for secret in hidden:
        assert secret not in logged


@pytest.mark.parametrize(
    "error",
    [_OverridesMessage(), _OverridesEntity("User", id=1)],
    ids=["message", "entity"],
)
def test_a_public_detail_that_overrides_a_mapper_field_answers_500(
    error: LoomError, caplog: pytest.LogCaptureFixture
) -> None:
    """A colliding key is logged and dropped; the client gets the fixed 500."""
    with caplog.at_level(logging.ERROR, logger="loom.rest.errors"):
        exc = HttpErrorMapper().to_http(error)

    _assert_internal_fallback(exc, error)
    _assert_misdeclaration_logged(
        caplog,
        type(error).__name__,
        "public_detail",
        "leaked-field-value",
        "leaked-message-host",
    )


def test_a_bare_status_int_is_accepted() -> None:
    """``http_status = 502`` type-checks as ``int`` and is ``HTTPStatus.BAD_GATEWAY``."""

    class _Declared(LoomError):
        http_status = 502

        def __init__(self) -> None:
            super().__init__("no", code=ErrorCode.SYSTEM_ERROR)

    exc = HttpErrorMapper().to_http(_Declared())

    assert exc.status_code is HTTPStatus.BAD_GATEWAY
    assert _detail(exc) == {
        "code": ErrorCode.SYSTEM_ERROR,
        "message": _FIXED_500,
        "trace_id": get_trace_id(),
    }


def test_a_bare_401_is_the_unauthorized_status() -> None:
    """Normalising matters: the challenge compares with ``is``, not equality."""

    class _Declared(LoomError):
        http_status = 401

        def __init__(self) -> None:
            super().__init__("sign in", code=ErrorCode.SYSTEM_ERROR)

    exc = HttpErrorMapper().to_http(_Declared())

    assert exc.status_code is HTTPStatus.UNAUTHORIZED
    assert exc.headers == {"WWW-Authenticate": "Bearer"}
    assert _detail(exc)["message"] == "sign in"


@pytest.mark.parametrize("status", [400, 451, 511])
def test_the_registered_4xx_and_5xx_bounds_are_accepted(status: int) -> None:
    class _Declared(LoomError):
        def __init__(self) -> None:
            super().__init__("bound", code=ErrorCode.SYSTEM_ERROR)

    error = _Declared()
    error.http_status = status

    assert HttpErrorMapper().to_http(error).status_code is HTTPStatus(status)


def test_a_truthy_non_bool_public_message_keeps_the_fixed_5xx_message() -> None:
    class _Declared(LoomError):
        http_status = HTTPStatus.BAD_GATEWAY

        def __init__(self) -> None:
            super().__init__("secret-host", code=ErrorCode.SYSTEM_ERROR)

    error = _Declared()
    flag: Any = "no"
    error.public_message = flag

    assert _detail(HttpErrorMapper().to_http(error))["message"] == _FIXED_500


class _RaisingDetail(LoomError):
    def __init__(self) -> None:
        super().__init__("secret-raise-host", code=ErrorCode.SYSTEM_ERROR)

    def public_detail(self) -> Mapping[str, Any]:
        raise RuntimeError("secret-raise-value")


class _NoneDetail(LoomError):
    def __init__(self) -> None:
        super().__init__("secret-none-host", code=ErrorCode.SYSTEM_ERROR)

    def public_detail(self) -> Mapping[str, Any]:
        result: Any = None
        return result


@pytest.mark.parametrize(
    "error", [_RaisingDetail(), _NoneDetail()], ids=["raises", "not-a-mapping"]
)
def test_a_broken_public_detail_answers_the_internal_error(
    error: LoomError, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.ERROR, logger="loom.rest.errors"):
        exc = HttpErrorMapper().to_http(error)

    _assert_internal_fallback(exc, error)
    _assert_misdeclaration_logged(
        caplog, type(error).__name__, "public_detail", "secret-raise-value"
    )


@pytest.mark.parametrize(
    "status",
    [200, 308, 499, 600, 700, True, "502", 502.0],
    ids=["200", "308", "499", "600", "700", "bool", "str", "float"],
)
def test_an_invalid_http_status_answers_the_internal_error(
    status: object, caplog: pytest.LogCaptureFixture
) -> None:
    class _Declared(LoomError):
        public_message = True

        def __init__(self) -> None:
            super().__init__("secret-status-host", code=ErrorCode.SYSTEM_ERROR)

        def public_detail(self) -> Mapping[str, Any]:
            return {"failed": "secret-status-field"}

    error = _Declared()
    declared: Any = status
    error.http_status = declared
    with caplog.at_level(logging.ERROR, logger="loom.rest.errors"):
        exc = HttpErrorMapper().to_http(error)

    _assert_internal_fallback(exc, error)
    _assert_misdeclaration_logged(
        caplog, type(error).__name__, "http_status", "secret-status-host", "secret-status-field"
    )


@pytest.mark.parametrize(
    ("error", "status", "message", "extra", "headers"),
    [
        (
            NotFound("User", id=1),
            404,
            "User with id=1 not found",
            {"entity": "User", "id": 1},
            None,
        ),
        (
            RuleViolation("email", "invalid"),
            422,
            "invalid",
            {"field": "email"},
            None,
        ),
        (RateLimited(1), 429, "Too many requests.", {}, {"Retry-After": "1"}),
        (SystemError("secret-host"), 500, _FIXED_500, {}, None),
        (
            SystemError("secret-host", code=ErrorCode.SERVICE_UNAVAILABLE),
            503,
            _FIXED_503,
            {},
            None,
        ),
        (
            PostCommitError(committed=True, failures=(ConnectionError("broker down"),)),
            500,
            _FIXED_500,
            {"committed": True},
            None,
        ),
    ],
    ids=["not-found", "rule", "rate-limited", "500", "503", "post-commit"],
)
def test_an_error_without_an_http_declaration_keeps_its_answer(
    error: LoomError,
    status: int,
    message: str,
    extra: dict[str, Any],
    headers: dict[str, str] | None,
) -> None:
    exc = HttpErrorMapper().to_http(error)

    assert (exc.status_code, exc.headers) == (status, headers)
    assert _detail(exc) == {
        "code": error.code,
        "message": message,
        "trace_id": get_trace_id(),
        **extra,
    }


def test_an_instance_may_declare_the_status_and_the_public_message() -> None:
    error = SystemError(_QUERY_MESSAGE, code=ErrorCode.SYSTEM_ERROR)
    error.http_status = HTTPStatus.BAD_GATEWAY
    error.public_message = True

    exc = HttpErrorMapper().to_http(error)

    assert exc.status_code == HTTPStatus.BAD_GATEWAY
    assert _detail(exc)["message"] == _QUERY_MESSAGE


_APP_SOURCE = '''\
"""Application that answers a failed pricing query with its own HTTP declaration."""

from __future__ import annotations

from enum import StrEnum
from http import HTTPStatus
from typing import Any

from loom.core.errors import LoomError
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class Record(BaseModel):
    __tablename__ = "declared_http_records"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class QueryCode(StrEnum):
    BAD_GATEWAY = "bad_gateway"


class QueryFailed(LoomError):
    http_status = HTTPStatus.BAD_GATEWAY
    public_message = True

    def __init__(self) -> None:
        super().__init__("upstream refused the sale quote", code=QueryCode.BAD_GATEWAY)

    def public_detail(self) -> dict[str, str]:
        return {"failed": "query/tasar-venta"}


class TasarVenta(UseCase[Record, str]):
    async def execute(self) -> str:
        raise QueryFailed()


class QueryInterface(RestInterface[str]):
    prefix = "/queries"
    routes = (RestRoute(use_case=TasarVenta, method="GET", path="/tasar-venta"),)
'''


def _write_project(
    directory: Path, *, source: str = _APP_SOURCE, slug: str = "declared_http"
) -> tuple[str, str]:
    module = f"loom_{slug}_{_NON_IDENTIFIER.sub('_', directory.name)}"
    (directory / f"{module}.py").write_text(source, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "declared-http",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [module], "warn_recommended": False},
            },
        },
        "stores": {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    path = directory / "app.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(path), module


def test_fastapi_and_lambda_answer_a_declared_error_the_same(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both transports serve the use case through the same mapper."""
    config, module = _write_project(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path.resolve()))
    fastapi_app = create_app(config, adapter=FastAPIAdapter(), environment="test")
    handler = create_app(config, adapter=LambdaAdapter(), environment="test")
    assert isinstance(fastapi_app, FastAPI)
    assert isinstance(handler, LambdaHTTPHandler)
    expected = {
        "detail": {
            "trace_id": _TRACE,
            "code": "bad_gateway",
            "message": _QUERY_MESSAGE,
            "failed": _FAILED_QUERY,
        }
    }
    try:
        with TestClient(fastapi_app) as fastapi_client, LambdaTestClient(handler) as lambda_client:
            headers = {"x-request-id": _TRACE}
            fastapi_reply = fastapi_client.get("/queries/tasar-venta", headers=headers)
            lambda_reply = lambda_client.get("/queries/tasar-venta", headers=headers)
        assert (fastapi_reply.status_code, fastapi_reply.json()) == (502, expected)
        assert (lambda_reply.status_code, lambda_reply.json()) == (502, expected)
    finally:
        handler.close()
        sys.modules.pop(module, None)


_MISDECLARED_APP_SOURCE = '''\
"""Application whose declared errors are not publishable."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from loom.core.errors import LoomError
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class Record(BaseModel):
    __tablename__ = "misdeclared_http_records"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class QueryCode(StrEnum):
    BAD_GATEWAY = "bad_gateway"


class CollidingQuery(LoomError):
    public_message = True

    def __init__(self) -> None:
        super().__init__("postgresql://db.internal:5432/orders", code=QueryCode.BAD_GATEWAY)

    def public_detail(self) -> dict[str, str]:
        return {"message": "LEAKED-COLLISION"}


class BadStatusQuery(LoomError):
    http_status = 200
    public_message = True

    def __init__(self) -> None:
        super().__init__("postgresql://db.internal:5432/orders", code=QueryCode.BAD_GATEWAY)

    def public_detail(self) -> dict[str, str]:
        return {"failed": "LEAKED-STATUS"}


class RaisingDetailQuery(LoomError):
    def __init__(self) -> None:
        super().__init__("postgresql://db.internal:5432/orders", code=QueryCode.BAD_GATEWAY)

    def public_detail(self) -> dict[str, str]:
        raise RuntimeError("LEAKED-RAISE")


class Collide(UseCase[Record, str]):
    async def execute(self) -> str:
        raise CollidingQuery()


class RaisingDetail(UseCase[Record, str]):
    async def execute(self) -> str:
        raise RaisingDetailQuery()


class BadStatus(UseCase[Record, str]):
    async def execute(self) -> str:
        raise BadStatusQuery()


class QueryInterface(RestInterface[str]):
    prefix = "/queries"
    routes = (
        RestRoute(use_case=Collide, method="GET", path="/collide"),
        RestRoute(use_case=BadStatus, method="GET", path="/bad-status"),
        RestRoute(use_case=RaisingDetail, method="GET", path="/raising-detail"),
    )
'''

_MISDECLARED = (
    ("/queries/collide", "LEAKED-COLLISION", "CollidingQuery", "public_detail"),
    ("/queries/bad-status", "LEAKED-STATUS", "BadStatusQuery", "http_status"),
    ("/queries/raising-detail", "LEAKED-RAISE", "RaisingDetailQuery", "public_detail"),
)


def test_fastapi_and_lambda_answer_a_misdeclared_error_as_json_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A bad declaration stays JSON, on both transports, and is logged once."""
    config, module = _write_project(
        tmp_path, source=_MISDECLARED_APP_SOURCE, slug="misdeclared_http"
    )
    monkeypatch.syspath_prepend(str(tmp_path.resolve()))
    fastapi_app = create_app(config, adapter=FastAPIAdapter(), environment="test")
    handler = create_app(config, adapter=LambdaAdapter(), environment="test")
    assert isinstance(fastapi_app, FastAPI)
    assert isinstance(handler, LambdaHTTPHandler)
    expected = {
        "detail": {
            "trace_id": _TRACE,
            "code": "bad_gateway",
            "message": _FIXED_500,
        }
    }
    try:
        with (
            TestClient(fastapi_app) as fastapi_client,
            LambdaTestClient(handler) as lambda_client,
            caplog.at_level(logging.ERROR, logger="loom.rest.errors"),
        ):
            for client in (fastapi_client, lambda_client):
                for path, leak, class_name, problem in _MISDECLARED:
                    caplog.clear()
                    reply = client.get(path, headers={"x-request-id": _TRACE})
                    assert reply.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
                    assert reply.headers["content-type"].startswith("application/json")
                    assert reply.json() == expected
                    assert leak not in reply.text
                    assert _LEAKY not in reply.text
                    _assert_misdeclaration_logged(caplog, class_name, problem, leak, _LEAKY)
    finally:
        handler.close()
        sys.modules.pop(module, None)
