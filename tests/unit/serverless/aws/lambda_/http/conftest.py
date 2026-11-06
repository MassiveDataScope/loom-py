"""One contract application served by FastAPI and by Lambda, behind the same client interface."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.composition import create_app
from loom.serverless.aws.lambda_ import LambdaAdapter, LambdaHTTPHandler
from tests.helpers.composition import application_openings
from tests.unit.serverless.aws.lambda_.http._clients import (
    Client,
    LambdaProject,
    driven_by,
    lambda_client,
)
from tests.unit.serverless.aws.lambda_.http._contract_app import write_contract_project

__all__ = ["application_openings"]


@pytest.fixture(scope="module")
def contract_project(tmp_path_factory: pytest.TempPathFactory) -> Callable[..., str]:
    """Write the contract project under a fresh module name; return its config path."""

    def write(**options: Any) -> str:
        return write_contract_project(tmp_path_factory.mktemp("contract"), **options)

    return write


@pytest.fixture(scope="module")
def contract_config(contract_project: Callable[..., str]) -> str:
    """One configuration and one codebase, served by both adapters."""
    return contract_project()


def _served_as(entry: str, config: str) -> object:
    """Compose *config* as a deployment does: only ``LOOM__APP__ENTRY`` picks the adapter."""
    with pytest.MonkeyPatch.context() as env:
        env.setenv("LOOM__APP__ENTRY", entry)
        return create_app(config, environment="test")


@pytest.fixture(scope="module")
def fastapi_served(contract_config: str) -> Iterator[Client]:
    app = _served_as("fastapi", contract_config)
    assert isinstance(app, FastAPI)
    with TestClient(app) as test_client:
        yield driven_by(test_client)


@pytest.fixture(scope="module")
def lambda_served(contract_config: str) -> Iterator[Client]:
    handler = _served_as("lambda", contract_config)
    assert isinstance(handler, LambdaHTTPHandler)
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
        handler = create_app(config, adapter=LambdaAdapter(), environment="test")
        opened.append(handler)
        return LambdaProject(config, handler)

    yield open_project
    for handler in opened:
        handler.close()


@pytest.fixture
def lambda_project(open_lambda: Callable[..., LambdaProject]) -> LambdaProject:
    """The contract project on a Lambda handler of its own, closed after the test."""
    return open_lambda()
