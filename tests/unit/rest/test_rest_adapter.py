from __future__ import annotations

from http import HTTPStatus
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitError
from loom.core.errors import (
    Conflict,
    Forbidden,
    LoomError,
    NotFound,
    RuleViolation,
    RuleViolations,
    Unauthenticated,
)
from loom.core.errors.codes import ErrorCode
from loom.core.model import BoundaryValidationError
from loom.core.repository.abc.query import CursorResult, PageResult
from loom.core.tracing import get_trace_id
from loom.core.transport.adapter import AdapterRequest, LoomAdapter
from loom.core.use_case.markers import Input
from loom.core.use_case.use_case import UseCase
from loom.etl import Format, UnsupportedFormatError
from loom.rest.auth import AuthenticationUnavailable
from loom.rest.errors import ErrorField, HttpErrorMapper
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.rest_adapter import LoomRestAdapter

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class _ResultStruct(msgspec.Struct):
    id: int
    name: str


class _SimpleUseCase(UseCase[Any, str]):
    async def execute(self) -> str:
        return "ok"


# ---------------------------------------------------------------------------
# AdapterRequest
# ---------------------------------------------------------------------------


class TestAdapterRequest:
    def test_params_stored(self) -> None:
        req = AdapterRequest(params={"user_id": 1})
        assert req.params == {"user_id": 1}

    def test_payload_default_none(self) -> None:
        req = AdapterRequest(params={})
        assert req.payload is None

    def test_payload_stored(self) -> None:
        req = AdapterRequest(params={}, payload={"email": "x@corp.com"})
        assert req.payload == {"email": "x@corp.com"}

    def test_dependencies_default_empty(self) -> None:
        req = AdapterRequest(params={})
        assert req.dependencies == {}

    def test_dependencies_stored(self) -> None:
        class Repo:
            pass

        repo = Repo()
        req = AdapterRequest(params={}, dependencies={Repo: repo})
        assert req.dependencies[Repo] is repo


# ---------------------------------------------------------------------------
# LoomAdapter Protocol (duck-typing conformance)
# ---------------------------------------------------------------------------


class TestLoomAdapterProtocol:
    def test_rest_adapter_satisfies_loom_adapter(self) -> None:
        executor = MagicMock(spec=["execute"])
        adapter = LoomRestAdapter(executor)
        assert callable(adapter.handle)

    def test_loom_adapter_is_protocol(self) -> None:
        class _CustomAdapter:
            async def handle(
                self,
                use_case: UseCase[Any, Any],
                request: AdapterRequest,
            ) -> Any:
                return None

        adapter = _CustomAdapter()
        assert isinstance(adapter, LoomAdapter)


# ---------------------------------------------------------------------------
# HttpErrorMapper
# ---------------------------------------------------------------------------


def _detail(exc: HTTPException) -> dict[str, Any]:
    """Return the structured error body the mapper always attaches."""
    assert isinstance(exc.detail, dict)
    return cast(dict[str, Any], exc.detail)


class TestHttpErrorMapper:
    def _mapper(self) -> HttpErrorMapper:
        return HttpErrorMapper()

    def test_not_found_maps_to_404(self) -> None:
        exc = self._mapper().to_http(NotFound("User", id=1))
        assert exc.status_code == 404

    def test_forbidden_maps_to_403(self) -> None:
        exc = self._mapper().to_http(Forbidden())
        assert exc.status_code == 403

    def test_unauthenticated_maps_to_401(self) -> None:
        exc = self._mapper().to_http(Unauthenticated())
        assert exc.status_code == 401

    def test_unauthenticated_carries_the_bearer_challenge(self) -> None:
        """RFC 9110 §11.6.1: a 401 without a challenge is not a valid response."""
        exc = self._mapper().to_http(Unauthenticated())
        assert exc.headers == {"WWW-Authenticate": "Bearer"}

    def test_non_401_errors_carry_no_challenge(self) -> None:
        """Only a 401 challenges the caller; a 403 is final."""
        exc = self._mapper().to_http(Forbidden())
        assert exc.headers is None

    def test_authentication_unavailable_maps_to_503_without_a_challenge(self) -> None:
        """An outage is not a credential fault, so the 503 challenges no one."""
        exc = self._mapper().to_http(AuthenticationUnavailable("issuer keys unreachable"))
        assert (exc.status_code, _detail(exc)["code"], exc.headers) == (
            503,
            ErrorCode.SERVICE_UNAVAILABLE,
            None,
        )

    def test_conflict_maps_to_409(self) -> None:
        exc = self._mapper().to_http(Conflict("duplicate"))
        assert exc.status_code == 409

    def test_post_commit_error_maps_to_500_marked_committed(self) -> None:
        """FR-003: the client learns the write stood even though the request failed."""
        error = PostCommitError(committed=True, failures=(ConnectionError("broker down"),))
        exc = self._mapper().to_http(error)
        assert exc.status_code == 500
        assert _detail(exc)["code"] == ErrorCode.POST_COMMIT_FAILURE
        assert _detail(exc)["committed"] is True

    def test_a_post_commit_error_without_a_commit_is_marked_not_committed(self) -> None:
        """A1: a read-only execution committed nothing, so the client may retry."""
        error = PostCommitError(committed=False, failures=(ConnectionError("broker down"),))
        exc = self._mapper().to_http(error)
        assert exc.status_code == 500
        assert _detail(exc)["committed"] is False

    def test_an_unsupported_format_maps_to_400(self) -> None:
        """A declaration mistake in an ETL exposed over REST is a caller error."""
        exc = self._mapper().to_http(UnsupportedFormatError(Format.XLSX, (Format.CSV,)))
        assert exc.status_code == 400

    def test_rule_violations_maps_to_422(self) -> None:
        violations = RuleViolations([RuleViolation("email", "invalid")])
        exc = self._mapper().to_http(violations)
        assert exc.status_code == 422

    def test_rule_violations_detail_includes_violations(self) -> None:
        violations = RuleViolations(
            [RuleViolation("email", "invalid"), RuleViolation("name", "too short")]
        )
        detail = _detail(self._mapper().to_http(violations))
        assert "violations" in detail
        assert len(detail["violations"]) == 2
        assert detail["violations"][0] == {"field": "email", "message": "invalid"}

    def test_rule_violation_body_carries_its_field(self) -> None:
        """Two 422s from different fields are told apart by key, not by message text."""
        exc = self._mapper().to_http(RuleViolation("tenant", "malformed tenant header"))
        assert exc.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert _detail(exc) == {
            ErrorField.CODE: ErrorCode.RULE_VIOLATION,
            ErrorField.MESSAGE: "malformed tenant header",
            ErrorField.FIELD: "tenant",
            ErrorField.TRACE_ID: get_trace_id(),
        }

    @pytest.mark.parametrize(
        "error",
        [
            NotFound("User", id=1),
            Forbidden(),
            Unauthenticated(),
            Conflict("duplicate"),
            RuleViolations([RuleViolation("email", "invalid")]),
            BoundaryValidationError("bad body", (("fullName", "Expected `str`"),)),
            PostCommitError(committed=True, failures=(ConnectionError("broker down"),)),
        ],
        ids=lambda error: type(error).__name__,
    )
    def test_no_other_error_body_gains_a_top_level_field(self, error: LoomError) -> None:
        assert ErrorField.FIELD not in _detail(self._mapper().to_http(error))

    def test_boundary_validation_maps_to_422_with_violations(self) -> None:
        error = BoundaryValidationError("bad body", (("fullName", "Expected `str`, got `int`"),))
        exc = self._mapper().to_http(error)
        detail = _detail(exc)
        assert exc.status_code == 422
        assert detail["code"] == ErrorCode.BOUNDARY_VALIDATION
        assert detail["violations"] == [
            {"field": "fullName", "message": "Expected `str`, got `int`"}
        ]

    def test_unknown_code_defaults_to_500(self) -> None:
        class _CustomError(LoomError):
            pass

        err = _CustomError("oops", code="unknown_custom")
        exc = self._mapper().to_http(err)
        assert exc.status_code == 500

    def test_detail_contains_code_and_message(self) -> None:
        detail = _detail(self._mapper().to_http(NotFound("Order", id=42)))
        assert detail["code"] == ErrorCode.NOT_FOUND
        assert "42" in detail["message"]


# ---------------------------------------------------------------------------
# LoomRestAdapter — success paths
# ---------------------------------------------------------------------------


class TestLoomRestAdapterSuccess:
    def _make_executor(self, return_value: Any) -> Any:
        executor = MagicMock()
        executor.execute = AsyncMock(return_value=return_value)
        return executor

    async def test_returns_plain_result(self) -> None:
        adapter = LoomRestAdapter(self._make_executor("ok"))
        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert result == "ok"

    async def test_struct_result_serialized_to_builtins(self) -> None:
        struct = _ResultStruct(id=1, name="Alice")
        adapter = LoomRestAdapter(self._make_executor(struct))
        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert result == {"id": 1, "name": "Alice"}

    async def test_page_result_envelope_serialized_in_camel_case(self) -> None:
        page = PageResult(
            items=(_ResultStruct(id=1, name="Alice"),),
            total_count=1,
            page=1,
            limit=50,
            has_next=False,
        )
        adapter = LoomRestAdapter(self._make_executor(page))

        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))

        assert result["totalCount"] == 1
        assert result["hasNext"] is False
        assert "total_count" not in result
        assert "has_next" not in result
        assert result["items"][0]["id"] == 1

    async def test_cursor_result_envelope_serialized_in_camel_case(self) -> None:
        cursor = CursorResult(
            items=(_ResultStruct(id=1, name="Alice"),),
            next_cursor="opaque-token",
            has_next=True,
        )
        adapter = LoomRestAdapter(self._make_executor(cursor))

        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))

        assert result["nextCursor"] == "opaque-token"
        assert result["hasNext"] is True
        assert "next_cursor" not in result
        assert "has_next" not in result

    async def test_non_struct_result_returned_as_is(self) -> None:
        payload = {"key": "value"}
        adapter = LoomRestAdapter(self._make_executor(payload))
        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert result is payload

    async def test_params_forwarded_to_executor(self) -> None:
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={"user_id": 7})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["params"] == {"user_id": 7}

    async def test_payload_forwarded_to_executor(self) -> None:
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={}, payload={"email": "x@corp.com"})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["payload"] == {"email": "x@corp.com"}

    async def test_empty_dependencies_passed_as_none(self) -> None:
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["dependencies"] is None

    async def test_non_empty_dependencies_forwarded(self) -> None:
        class Repo:
            pass

        repo = Repo()
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={}, dependencies={Repo: repo})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["dependencies"] == {Repo: repo}


# ---------------------------------------------------------------------------
# LoomRestAdapter — error mapping
# ---------------------------------------------------------------------------


class TestLoomRestAdapterErrors:
    def _make_failing_executor(self, error: Exception) -> Any:
        executor = MagicMock()
        executor.execute = AsyncMock(side_effect=error)
        return executor

    async def test_loom_error_mapped_to_http_exception(self) -> None:
        executor = self._make_failing_executor(NotFound("Item", id=99))
        adapter = LoomRestAdapter(executor)
        use_case = _SimpleUseCase()
        request = AdapterRequest(params={})
        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(use_case, request)
        assert exc_info.value.status_code == 404

    async def test_rule_violations_mapped_to_422(self) -> None:
        err = RuleViolations([RuleViolation("email", "bad")])
        executor = self._make_failing_executor(err)
        adapter = LoomRestAdapter(executor)
        use_case = _SimpleUseCase()
        request = AdapterRequest(params={})
        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(use_case, request)
        assert exc_info.value.status_code == 422

    async def test_rule_violation_mapped_to_422_with_its_field(self) -> None:
        executor = self._make_failing_executor(RuleViolation("tenant", "bad"))
        adapter = LoomRestAdapter(executor)
        request = AdapterRequest(params={})
        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(_SimpleUseCase(), request)
        assert exc_info.value.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert _detail(exc_info.value)[ErrorField.FIELD] == "tenant"

    async def test_non_loom_error_not_caught(self) -> None:
        executor = self._make_failing_executor(RuntimeError("unexpected"))
        adapter = LoomRestAdapter(executor)
        use_case = _SimpleUseCase()
        request = AdapterRequest(params={})
        with pytest.raises(RuntimeError, match="unexpected"):
            await adapter.handle(use_case, request)

    async def test_custom_error_mapper_used(self) -> None:
        class _AlwaysTeapot(HttpErrorMapper):
            def to_http(self, error: LoomError) -> HTTPException:
                return HTTPException(status_code=418, detail="teapot")

        executor = self._make_failing_executor(NotFound("X", id=1))
        adapter = LoomRestAdapter(executor, error_mapper=_AlwaysTeapot())
        use_case = _SimpleUseCase()
        request = AdapterRequest(params={})
        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(use_case, request)
        assert exc_info.value.status_code == 418


# ---------------------------------------------------------------------------
# LoomRestAdapter — boundary validation, driven by a real RuntimeExecutor
# ---------------------------------------------------------------------------


class _CreateItemCmd(Command, frozen=True):
    full_name: str


class _CreateItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, cmd: _CreateItemCmd = Input()) -> dict[str, Any]:
        return {"full_name": cmd.full_name}


def _real_executor(*use_case_types: type[UseCase[Any, Any]]) -> RuntimeExecutor:
    uc_compiler = UseCaseCompiler()
    for use_case_type in use_case_types:
        uc_compiler.compile(use_case_type)
    return RuntimeExecutor(uc_compiler)


class TestLoomRestAdapterBoundaryValidation:
    """A bad body drives the same 422 through the adapter and a real executor."""

    async def test_a_bad_field_maps_to_422_with_violations(self) -> None:
        adapter = LoomRestAdapter(_real_executor(_CreateItemUseCase))
        request = AdapterRequest(params={}, payload={"fullName": 12})
        use_case = _CreateItemUseCase()

        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(use_case, request)

        assert exc_info.value.status_code == 422
        detail = _detail(exc_info.value)
        assert detail["code"] == ErrorCode.BOUNDARY_VALIDATION
        assert detail["violations"][0]["field"] == "fullName"


# ---------------------------------------------------------------------------
# RuleViolation on the wire — no database, repository or row-level security
# ---------------------------------------------------------------------------


class _RejectTenantUseCase(UseCase[Any, str]):
    async def execute(self) -> str:
        raise RuleViolation("tenant", "no tenant is selectable")


def _app_without_database() -> FastAPI:
    """A bare app: no container, no database, no ``schema.mode``, no RLS guard."""
    adapter = LoomRestAdapter(_real_executor(_RejectTenantUseCase))
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/tenant")
    async def tenant() -> Any:
        return await adapter.handle(_RejectTenantUseCase(), AdapterRequest(params={}))

    return app


def test_a_rule_violation_answers_its_field_without_any_database() -> None:
    response = TestClient(_app_without_database()).get("/tenant")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    detail = response.json()["detail"]
    assert detail[ErrorField.CODE] == ErrorCode.RULE_VIOLATION
    assert detail[ErrorField.MESSAGE] == "no tenant is selectable"
    assert detail[ErrorField.FIELD] == "tenant"
    assert ErrorField.TRACE_ID in detail
