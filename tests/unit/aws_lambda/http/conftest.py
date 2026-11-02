"""One contract application served by FastAPI and by Lambda, behind the same client interface."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from loom.aws_lambda import LambdaHTTPAdapter, LambdaHTTPHandler
from loom.composition import create_app
from loom.rest.fastapi import FastAPIAdapter
from tests.helpers.composition import application_openings
from tests.unit.aws_lambda.http._clients import (
    Client,
    LambdaProject,
    fastapi_client,
    lambda_client,
)
from tests.unit.aws_lambda.http._contract_app import write_contract_project

__all__ = ["application_openings"]


@pytest.fixture(scope="module")
def contract_project(tmp_path_factory: pytest.TempPathFactory) -> Callable[..., str]:
    """Write the contract project under a fresh module name; return its config path."""

    def write(**options: Any) -> str:
        return write_contract_project(tmp_path_factory.mktemp("contract"), **options)

    return write


@pytest.fixture(scope="module")
def fastapi_served(contract_project: Callable[..., str]) -> Iterator[Client]:
    app = create_app(contract_project(), adapter=FastAPIAdapter(), environment="test")
    with TestClient(app) as test_client:
        yield fastapi_client(test_client)


@pytest.fixture(scope="module")
def lambda_served(contract_project: Callable[..., str]) -> Iterator[Client]:
    handler = create_app(contract_project(), adapter=LambdaHTTPAdapter(), environment="test")
    yield lambda_client(handler)
    handler.close()


@pytest.fixture(params=["fastapi_served", "lambda_served"], ids=["fastapi", "lambda"])
def served(request: pytest.FixtureRequest) -> Client:
    """The contract application, served by each adapter in turn."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def open_lambda(contract_project: Callable[..., str]) -> Iterator[Callable[..., LambdaProject]]:
    """Open contract projects on Lambda handlers of their own, closed after the test."""
    opened: list[LambdaHTTPHandler] = []

    def open_project(**options: Any) -> LambdaProject:
        config = contract_project(**options)
        handler = create_app(config, adapter=LambdaHTTPAdapter(), environment="test")
        opened.append(handler)
        return LambdaProject(config, handler)

    yield open_project
    for handler in opened:
        handler.close()


@pytest.fixture
def lambda_project(open_lambda: Callable[..., LambdaProject]) -> LambdaProject:
    """The contract project on a Lambda handler of its own, closed after the test."""
    return open_lambda()
