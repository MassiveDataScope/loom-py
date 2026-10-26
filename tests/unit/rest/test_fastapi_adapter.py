"""The HTTP entry point is the FastAPI adapter mounted on the shared composition."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from loom.composition import create_app as compose
from loom.rest.fastapi import create_app
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory


def _routes(app: FastAPI) -> set[tuple[str, frozenset[str]]]:
    return {
        (route.path, frozenset(route.methods))
        for route in app.router.routes
        if isinstance(route, APIRoute)
    }


def test_the_shortcut_and_the_adapter_serve_the_same_application(
    fixture_project: ProjectFactory,
) -> None:
    shortcut = create_app(fixture_project("shortcut"), environment="test")
    composed = compose(fixture_project("adapter"), adapter=FastAPIAdapter(), environment="test")

    assert _routes(shortcut) == _routes(composed)
    assert shortcut.openapi()["paths"].keys() == composed.openapi()["paths"].keys()
    with TestClient(shortcut) as first, TestClient(composed) as second:
        assert first.get("/health").json() == second.get("/health").json()
        assert first.get("/ping/").json() == second.get("/ping/").json() == "pong"


def test_the_lifespan_opens_the_application_once_and_closes_it(
    fixture_project: ProjectFactory, application_openings: list[str]
) -> None:
    app = create_app(fixture_project(), environment="test")

    assert application_openings == []
    with TestClient(app) as client:
        client.get("/ping/")
        client.get("/ping/")
        assert application_openings == ["open"]

    assert application_openings == ["open", "close"]


def test_bind_before_recipe_is_refused() -> None:
    with pytest.raises(RuntimeError, match="call recipe first"):
        FastAPIAdapter().bind(object())  # type: ignore[arg-type]
