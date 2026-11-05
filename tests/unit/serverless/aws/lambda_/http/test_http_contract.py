"""The same application answers the same over FastAPI and over Lambda (spec 004, US8, FR-023a).

Every test runs against both adapters, composed from one configuration and one
codebase that only ``LOOM__APP__ENTRY`` switches (scenario 9). Lambda is driven
with API Gateway HTTP API events through the real handler. The expected status,
body and headers are pinned, so a difference is a failure on one side, never a
drift both share.
"""

from __future__ import annotations

import json

import pytest

from tests.unit.serverless.aws.lambda_.http._clients import TRACE_ID, Client
from tests.unit.serverless.aws.lambda_.http._contract_app import (
    MAX_BODY_BYTES,
    ORIGIN,
    PUBLIC_PATH,
    bearer,
)

pytestmark = pytest.mark.timeout(60)


_AS_ALICE = "alice's token"
"""Stands for the ``alice`` fixture in a parametrize table, which is built at import."""


@pytest.fixture
def alice() -> dict[str, str]:
    """A token minted when the test runs: one minted at import expires in a long suite."""
    return bearer("alice")


@pytest.fixture
def admin() -> dict[str, str]:
    return bearer("root", "admin")


_JSON = "application/json"
_UNAUTHENTICATED = {
    "detail": {
        "code": "unauthenticated",
        "message": "Authentication required: missing or invalid credentials.",
        "trace_id": TRACE_ID,
    }
}


def test_a_valid_request_answers_200_with_the_use_case_result(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("GET", "/items/7", headers=alice)

    assert (reply.status, reply.json()) == (200, {"id": 7, "type": "int"})
    assert reply.headers["content-type"] == _JSON


def test_a_body_that_is_not_json_answers_400(served: Client, alice: dict[str, str]) -> None:
    reply = served("POST", "/items/", headers=alice, body=b"{not json")

    assert (reply.status, reply.json()) == (
        400,
        {
            "detail": {
                "trace_id": TRACE_ID,
                "code": "bad_request",
                "message": "Request body is not valid JSON",
            }
        },
    )


def test_a_query_parameter_the_route_refuses_answers_400(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("GET", "/items/7?profile=full", headers=alice)

    assert (reply.status, reply.json()) == (
        400,
        {
            "detail": {
                "code": "bad_request",
                "message": "Query parameter 'profile' is not allowed for this route.",
                "trace_id": TRACE_ID,
            }
        },
    )


def test_a_request_without_a_token_answers_401_with_a_challenge(served: Client) -> None:
    reply = served("GET", "/items/7")

    assert (reply.status, reply.json()) == (401, _UNAUTHENTICATED)
    assert reply.headers["www-authenticate"] == "Bearer"


def test_a_token_signed_with_another_key_answers_401(served: Client, alice: dict[str, str]) -> None:
    forged = {"authorization": alice["authorization"][:-4] + "AAAA"}

    reply = served("GET", "/items/7", headers=forged)

    assert (reply.status, reply.json()) == (401, _UNAUTHENTICATED)


def test_an_excluded_path_is_served_without_a_token(served: Client) -> None:
    reply = served("POST", PUBLIC_PATH)

    assert (reply.status, reply.json()) == (200, {"subject": ""})


def test_a_caller_without_the_route_role_answers_403(served: Client, alice: dict[str, str]) -> None:
    reply = served("GET", "/ops/admin", headers=alice)

    assert (reply.status, reply.json()) == (
        403,
        {
            "detail": {
                "trace_id": TRACE_ID,
                "code": "forbidden",
                "message": "You are not authorized to access this route.",
            }
        },
    )


def test_a_caller_holding_the_route_role_is_served(served: Client, admin: dict[str, str]) -> None:
    reply = served("GET", "/ops/admin", headers=admin)

    assert (reply.status, reply.json()) == (200, "granted")


def test_an_unknown_route_answers_fastapis_404(served: Client, alice: dict[str, str]) -> None:
    reply = served("GET", "/nowhere", headers=alice)

    assert (reply.status, reply.json()) == (404, {"detail": "Not Found"})


def test_a_method_the_route_does_not_declare_answers_405_with_allow(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("POST", "/items/7", headers=alice)

    assert (reply.status, reply.json()) == (405, {"detail": "Method Not Allowed"})
    assert reply.headers["allow"] == "GET"


def test_head_on_a_get_route_is_not_served(served: Client, alice: dict[str, str]) -> None:
    reply = served("HEAD", "/items/7", headers=alice)

    assert (reply.status, reply.headers["allow"]) == (405, "GET")


def test_a_body_over_the_cap_answers_413_before_authentication(served: Client) -> None:
    oversized = json.dumps({"name": "x" * MAX_BODY_BYTES}).encode()

    reply = served("POST", "/items/", body=oversized)

    assert (reply.status, reply.json()) == (
        413,
        {
            "detail": {
                "code": "payload_too_large",
                "message": "Request body exceeds the maximum accepted size "
                f"({MAX_BODY_BYTES} bytes)",
                "trace_id": TRACE_ID,
            }
        },
    )


def test_a_body_missing_a_field_answers_422(served: Client, alice: dict[str, str]) -> None:
    reply = served("POST", "/items/", headers=alice, body=b'{"title": "x"}')

    assert (reply.status, reply.json()) == (
        422,
        {
            "detail": {
                "trace_id": TRACE_ID,
                "code": "boundary_validation",
                "message": "Object missing required field `name`",
                "violations": [
                    {"field": "name", "message": "Object missing required field `name`"}
                ],
            }
        },
    )


def test_a_path_parameter_of_the_wrong_type_answers_422_not_404(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("GET", "/items/seven", headers=alice)

    assert (reply.status, reply.json()) == (
        422,
        {
            "detail": {
                "code": "rule_violations",
                "message": "Request validation failed",
                "trace_id": TRACE_ID,
                "violations": [
                    {
                        "field": "path.item_id",
                        "message": "Input should be a valid integer, "
                        "unable to parse string as an integer",
                    }
                ],
            }
        },
    )


def test_a_path_parameter_is_validated_before_the_route_role(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("GET", "/ops/admin/high", headers=alice)

    assert (reply.status, reply.json()["detail"]["violations"][0]["field"]) == (422, "path.level")


def test_a_loom_error_answers_its_mapped_status(served: Client, alice: dict[str, str]) -> None:
    reply = served("GET", "/items/404", headers=alice)

    assert (reply.status, reply.json()) == (
        404,
        {
            "detail": {
                "trace_id": TRACE_ID,
                "code": "not_found",
                "message": "Item with id=404 not found",
                "entity": "Item",
                "id": 404,
            }
        },
    )


def test_an_unexpected_failure_answers_500_without_its_details(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("GET", "/ops/explode", headers=alice)

    assert (reply.status, reply.json()) == (
        500,
        {"code": "internal_error", "message": "An unexpected error occurred", "trace_id": TRACE_ID},
    )


def test_a_cors_preflight_is_answered_without_a_token(served: Client) -> None:
    preflight = {"origin": ORIGIN, "access-control-request-method": "POST"}

    reply = served("OPTIONS", "/items/", headers=preflight)

    assert (reply.status, reply.body) == (200, b"OK")
    assert (
        reply.headers["access-control-allow-origin"],
        reply.headers["access-control-allow-methods"],
        reply.headers["access-control-max-age"],
    ) == (ORIGIN, "GET, POST", "600")


def test_a_cross_origin_response_carries_the_cors_headers(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("GET", "/items/7", headers={"origin": ORIGIN, **alice})

    assert (
        reply.headers["access-control-allow-origin"],
        reply.headers["access-control-expose-headers"],
        reply.headers["vary"],
    ) == (ORIGIN, "x-request-id", "Origin")


def test_a_preflight_from_another_origin_is_refused(served: Client) -> None:
    preflight = {"origin": "https://evil.example", "access-control-request-method": "GET"}

    reply = served("OPTIONS", "/items/7", headers=preflight)

    assert (reply.status, reply.body) == (400, b"Disallowed CORS origin")


def test_the_callers_trace_id_is_echoed_on_the_response(
    served: Client, alice: dict[str, str]
) -> None:
    reply = served("GET", "/items/7", headers=alice)

    assert reply.headers["x-request-id"] == TRACE_ID


def test_an_unsafe_trace_id_is_replaced_by_one_the_body_shares(served: Client) -> None:
    reply = served("GET", "/items/7", headers={"x-request-id": "<script>"})

    assert reply.headers["x-request-id"] not in {"", "<script>"}
    assert reply.json()["detail"]["trace_id"] == reply.headers["x-request-id"]


@pytest.mark.parametrize(
    ("method", "path", "headers", "body"),
    [
        ("GET", "/items/7", _AS_ALICE, b""),
        ("GET", "/items/7?profile=full", _AS_ALICE, b""),
        ("GET", "/items/seven", _AS_ALICE, b""),
        ("GET", "/items/404", _AS_ALICE, b""),
        ("POST", "/items/7", _AS_ALICE, b""),
        ("GET", "/nowhere", _AS_ALICE, b""),
        ("POST", "/items/", _AS_ALICE, b'{"name": "lamp"}'),
        ("POST", "/items/", _AS_ALICE, b"[1]"),
        ("POST", "/items/", _AS_ALICE, b'{"title": "x"}'),
        ("GET", "/items/7", {}, b""),
        ("POST", PUBLIC_PATH, {}, b""),
        ("POST", "/items/", {}, b"x" * (MAX_BODY_BYTES + 1)),
        ("GET", "/ops/explode", _AS_ALICE, b""),
        ("GET", "/ops/admin", _AS_ALICE, b""),
        ("OPTIONS", "/items/7", {"origin": ORIGIN, "access-control-request-method": "GET"}, b""),
    ],
    ids=[
        "200",
        "400-query",
        "422-path",
        "404-loom",
        "405",
        "404-route",
        "200-post",
        "422-not-an-object",
        "422-missing-field",
        "401",
        "excluded-path",
        "413",
        "500",
        "403",
        "preflight",
    ],
)
def test_both_adapters_send_the_same_reply(
    fastapi_served: Client,
    lambda_served: Client,
    method: str,
    path: str,
    headers: dict[str, str] | str,
    body: bytes,
    alice: dict[str, str],
) -> None:
    sent = alice if headers == _AS_ALICE else headers
    assert isinstance(sent, dict)
    over_fastapi = fastapi_served(method, path, headers=sent, body=body)

    over_lambda = lambda_served(method, path, headers=sent, body=body)

    assert over_lambda == over_fastapi
