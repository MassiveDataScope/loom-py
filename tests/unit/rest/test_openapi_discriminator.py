"""OpenAPI publication of discriminated (tagged) unions.

A discriminator ``mapping`` value is a schema reference like ``$ref``, so it
must point at a component that exists in the published document.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated, Any, Literal

import msgspec
import pydantic
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.command import Command
from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.markers import Input
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi.app import _register_openapi_components
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute

_COMPONENT_PREFIX = "#/components/schemas/"


class Card(msgspec.Struct, frozen=True, tag="card"):
    last4: str


class Transfer(msgspec.Struct, frozen=True, tag="transfer"):
    iban: str


class Payment(msgspec.Struct, frozen=True):
    method: Card | Transfer
    fallback: Card | Transfer | None = None


class PayOrder(Command, frozen=True):
    method: Card | Transfer
    fallback: Card | Transfer | None = None


class CardModel(pydantic.BaseModel):
    kind: Literal["card"]
    last4: str


class TransferModel(pydantic.BaseModel):
    kind: Literal["transfer"]
    iban: str


class PaymentModel(pydantic.BaseModel):
    method: Annotated[CardModel | TransferModel, pydantic.Field(discriminator="kind")]
    fallback: Annotated[CardModel | TransferModel, pydantic.Field(discriminator="kind")] | None = (
        None
    )


class PayOrderUseCase(UseCase[Any, Payment]):
    async def execute(self, cmd: PayOrder = Input()) -> Payment:
        return Payment(method=cmd.method, fallback=cmd.fallback)


class ReadPayment(UseCase[Any, Payment]):
    async def execute(self, payment_id: str) -> Payment:
        return Payment(method=Card(last4=payment_id))


class ReadPaymentModel(UseCase[Any, PaymentModel]):
    async def execute(self, payment_id: str) -> PaymentModel:
        return PaymentModel(method=CardModel(kind="card", last4=payment_id))


class Payments(RestInterface[Any]):
    prefix = "/payments"
    routes = (
        RestRoute(use_case=PayOrderUseCase, method="POST", path="/"),
        RestRoute(use_case=ReadPayment, method="GET", path="/{payment_id}"),
        RestRoute(use_case=ReadPaymentModel, method="GET", path="/{payment_id}/model"),
    )


def _openapi() -> dict[str, Any]:
    compiler = UseCaseCompiler()
    factory = UseCaseFactory(LoomContainer())
    for route in Payments.routes:
        compiler.compile(route.use_case)
        factory.register(route.use_case)
    app = FastAPI()
    registry = bind_interfaces(
        app,
        RestInterfaceCompiler(compiler).compile(Payments),
        factory,
        RuntimeExecutor(compiler),
        ObservabilityRuntime.noop(),
    )
    _register_openapi_components(app, registry)
    document: dict[str, Any] = TestClient(app).get("/openapi.json").json()
    return document


def _references(node: Any) -> Iterator[str]:
    """Yield every ``$ref`` value and every discriminator mapping value in *node*."""
    if isinstance(node, list):
        for item in node:
            yield from _references(item)
        return
    if not isinstance(node, dict):
        return
    for key, value in node.items():
        if key == "$ref" and isinstance(value, str):
            yield value
        elif key == "discriminator" and isinstance(value, dict):
            yield from value.get("mapping", {}).values()
        yield from _references(value)


def _resolves(document: dict[str, Any], reference: str) -> bool:
    if not reference.startswith("#/"):
        return False
    node: Any = document
    for token in reference.removeprefix("#/").split("/"):
        if not isinstance(node, dict) or token not in node:
            return False
        node = node[token]
    return True


def _body_schema(document: dict[str, Any], operation: tuple[str, str]) -> dict[str, Any]:
    """Return the request body schema of a POST, else the 200 schema, dereferenced once."""
    path, method = operation
    entry = document["paths"][path][method]
    body = entry["requestBody"] if method == "post" else entry["responses"]["200"]
    schema: dict[str, Any] = body["content"]["application/json"]["schema"]
    ref = schema.get("$ref")
    if ref is None:
        return schema
    return dict(document["components"]["schemas"][ref.removeprefix(_COMPONENT_PREFIX)])


def _mapping(document: dict[str, Any], operation: tuple[str, str], field: str) -> dict[str, str]:
    prop = _body_schema(document, operation)["properties"][field]
    if "discriminator" in prop:
        return dict(prop["discriminator"]["mapping"])
    (union,) = [option for option in prop["anyOf"] if "discriminator" in option]
    return dict(union["discriminator"]["mapping"])


_OPERATIONS = pytest.mark.parametrize(
    "operation",
    [
        pytest.param(("/payments/", "post"), id="msgspec-request"),
        pytest.param(("/payments/{payment_id}", "get"), id="msgspec-response"),
        pytest.param(("/payments/{payment_id}/model", "get"), id="pydantic-response"),
    ],
)


@_OPERATIONS
@pytest.mark.parametrize("field", ["method", "fallback"])
def test_each_tag_of_a_union_maps_to_an_existing_component(
    operation: tuple[str, str], field: str
) -> None:
    document = _openapi()

    mapping = _mapping(document, operation, field)

    assert set(mapping) == {"card", "transfer"}
    assert all(target.startswith(_COMPONENT_PREFIX) for target in mapping.values())
    assert all(_resolves(document, target) for target in mapping.values())


def test_every_reference_in_the_document_resolves() -> None:
    document = _openapi()

    references = list(_references(document))

    assert references
    assert [ref for ref in references if not _resolves(document, ref)] == []
