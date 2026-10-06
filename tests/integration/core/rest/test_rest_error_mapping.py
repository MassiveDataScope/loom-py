"""Integration tests — REST layer error mapping.

Verifies that every ``LoomError`` subclass is mapped to the correct HTTP
status code and that the response body contains the expected fields, without
requiring a database or external service.

Infrastructure:
    - ``HttpTestHarness`` + ``InMemoryRepository`` (zero-dependency)
    - ``force_error`` / ``simulate_system_error`` to inject failures

Response shape: FastAPI wraps HTTPException detail under the ``detail`` key.
"""

from __future__ import annotations

from http import HTTPStatus

import pytest

from loom.core.errors import Conflict, Forbidden, NotFound
from loom.testing import HttpTestHarness, InMemoryRepository
from tests.integration.fake_repo.product.interface import ProductRestInterface
from tests.integration.fake_repo.product.model import Product


@pytest.fixture
def client_with_empty_repo():  # type: ignore[no-untyped-def]
    harness = HttpTestHarness()
    harness.inject_repo(Product, InMemoryRepository(Product))
    return harness.build_app(interfaces=[ProductRestInterface])


def _error_detail(resp):  # type: ignore[no-untyped-def]
    """Return the structured detail payload from an error response."""
    return resp.json()["detail"]


# ---------------------------------------------------------------------------
# Validation errors (RuleViolations → 422)
# ---------------------------------------------------------------------------


class TestValidationErrors:
    def test_blank_name_returns_422_with_violations(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.post("/products/", json={"name": "   ", "price": 10.0})

        assert resp.status_code == 422
        detail = _error_detail(resp)
        assert detail["code"] == "rule_violations"
        assert isinstance(detail["violations"], list)
        fields = [v["field"] for v in detail["violations"]]
        assert "name" in fields

    def test_negative_price_returns_422_with_violations(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.post("/products/", json={"name": "Widget", "price": -5.0})

        assert resp.status_code == 422
        detail = _error_detail(resp)
        assert detail["code"] == "rule_violations"
        assert any(v["field"] == "price" for v in detail["violations"])

    def test_multiple_violations_returned_together(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.post("/products/", json={"name": "  ", "price": -1.0})

        assert resp.status_code == 422
        assert len(_error_detail(resp)["violations"]) >= 2

    def test_violation_shape_has_field_and_message(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.post("/products/", json={"name": "   ", "price": 10.0})

        violation = _error_detail(resp)["violations"][0]
        assert isinstance(violation["field"], str)
        assert isinstance(violation["message"], str)

    def test_patch_empty_payload_returns_422(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.patch("/products/99", json={})

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Malformed body (not JSON → 400)
# ---------------------------------------------------------------------------

_MALFORMED_BODY_MESSAGE = "Request body is not valid JSON"


class TestMalformedBody:
    @pytest.mark.parametrize(
        "body",
        [b"{not json", b"", b"\xff\xfe", b'{"name": "\xff", "price": 1.0}'],
        ids=["syntax", "empty", "not_utf8", "not_utf8_in_string"],
    )
    def test_malformed_body_returns_400_with_error_body(
        self,
        client_with_empty_repo,  # type: ignore[no-untyped-def]
        body: bytes,
    ) -> None:
        resp = client_with_empty_repo.post(
            "/products/", content=body, headers={"content-type": "application/json"}
        )

        assert resp.status_code == HTTPStatus.BAD_REQUEST
        detail = _error_detail(resp)
        assert detail.keys() == {"code", "message", "trace_id"}
        assert (detail["code"], detail["message"]) == ("bad_request", _MALFORMED_BODY_MESSAGE)

    def test_malformed_body_is_not_echoed(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.patch(
            "/products/1",
            content=b'{"name": "echo-marker',
            headers={"content-type": "application/json"},
        )

        assert resp.status_code == HTTPStatus.BAD_REQUEST
        assert "echo-marker" not in resp.text

    def test_well_formed_body_of_wrong_type_stays_422(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.post("/products/", json={"name": "Widget", "price": "free"})

        assert resp.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert _error_detail(resp)["code"] == "boundary_validation"


# ---------------------------------------------------------------------------
# Not found (NotFound → 404)
# ---------------------------------------------------------------------------


class TestNotFoundErrors:
    def test_forced_not_found_returns_404(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "get_by_id", NotFound("Product", id=99))
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.get("/products/99")

        assert resp.status_code == 404
        detail = _error_detail(resp)
        assert detail["code"] == "not_found"
        assert detail["entity"] == "Product"
        assert detail["id"] == 99

    def test_not_found_body_has_message(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "get_by_id", NotFound("Product", id=1))
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.get("/products/1")

        detail = _error_detail(resp)
        assert "Product" in detail["message"]


# ---------------------------------------------------------------------------
# Conflict (Conflict → 409)
# ---------------------------------------------------------------------------


class TestConflictErrors:
    def test_forced_conflict_returns_409(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "create", Conflict("Product already exists"))
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert resp.status_code == 409
        detail = _error_detail(resp)
        assert detail["code"] == "conflict"
        assert "message" in detail


# ---------------------------------------------------------------------------
# Forbidden (Forbidden → 403)
# ---------------------------------------------------------------------------


class TestForbiddenErrors:
    def test_forced_forbidden_returns_403(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "update", Forbidden("not allowed"))
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.patch("/products/1", json={"price": 5.0})

        assert resp.status_code == 403
        assert _error_detail(resp)["code"] == "forbidden"


# ---------------------------------------------------------------------------
# System error (unhandled exception → 500)
# ---------------------------------------------------------------------------


class TestSystemErrors:
    def test_simulated_system_error_returns_500(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.simulate_system_error(Product, "create")
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert resp.status_code == 500

    def test_unhandled_runtime_error_returns_500(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "create", RuntimeError("unexpected crash"))
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# Successful operations
# ---------------------------------------------------------------------------


class TestSuccessfulOperations:
    def test_create_returns_201_with_id(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.post("/products/", json={"name": "Gadget", "price": 25.0})

        assert resp.status_code == 201
        body = resp.json()
        assert body["id"] == 1
        assert body["name"] == "Gadget"

    def test_create_normalizes_name(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.post(
            "/products/", json={"name": "  keyboard  ", "price": 50.0}
        )

        assert resp.status_code == 201
        assert resp.json()["name"] == "keyboard"

    def test_get_missing_returns_none_not_404(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.get("/products/999")

        assert resp.status_code == 200
        assert resp.json() is None

    def test_delete_missing_entity_returns_false(self, client_with_empty_repo) -> None:  # type: ignore[no-untyped-def]
        resp = client_with_empty_repo.delete("/products/999")

        assert resp.status_code == 200
        assert resp.json() is False


# ---------------------------------------------------------------------------
# Error response always includes code and message
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error,method,path,http_method,payload,expected_status",
    [
        (NotFound("Product", id=1), "get_by_id", "/products/1", "GET", None, 404),
        (Conflict("dup"), "create", "/products/", "POST", {"name": "W", "price": 1.0}, 409),
        (Forbidden("no"), "update", "/products/1", "PATCH", {"price": 1.0}, 403),
    ],
)
def test_error_detail_always_has_code_and_message(
    error: Exception,
    method: str,
    path: str,
    http_method: str,
    payload: dict | None,
    expected_status: int,
) -> None:
    harness = HttpTestHarness()
    harness.inject_repo(Product, InMemoryRepository(Product))
    harness.force_error(Product, method, error)
    client = harness.build_app(interfaces=[ProductRestInterface])

    if http_method == "GET":
        resp = client.get(path)
    elif http_method == "POST":
        resp = client.post(path, json=payload)
    else:
        resp = client.patch(path, json=payload)

    assert resp.status_code == expected_status
    detail = _error_detail(resp)
    assert "code" in detail
    assert "message" in detail
