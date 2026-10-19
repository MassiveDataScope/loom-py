"""What ``create_app`` refuses to build, and what it warns about.

The application factory is the last place where an unsafe shape can still be
turned into a startup error rather than a production incident: an exclusion
that opens a business route, a schema published anonymously, or a CORS policy
that hands credentials to every origin.
"""

from __future__ import annotations

import tempfile
import warnings
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI

from loom.core.config.errors import ConfigError
from loom.rest.fastapi.auto import create_app
from tests.unit.rest._fixture_app import write_project

_SECRET_PATH = str(Path(tempfile.mkdtemp()) / "hs.key")
Path(_SECRET_PATH).write_text("unit-test-secret")

_JWT_SECTION: dict[str, Any] = {
    "secret_path": _SECRET_PATH,
    "algorithms": ["HS256"],
    "audience": "loom-api",
}
_OPENAPI = "/openapi.json"


def _auth(**overrides: Any) -> dict[str, Any]:
    section: dict[str, Any] = {"auth": {"jwt": dict(_JWT_SECTION)}}
    section.update(overrides)
    return section


def _route_paths(app: FastAPI) -> set[str]:
    return {str(getattr(route, "path", "")) for route in app.routes}


# ---------------------------------------------------------------------------
# Exclusions vs. templated routes
# ---------------------------------------------------------------------------


def test_a_catch_all_route_capturing_an_exclusion_aborts_startup(tmp_path: Path) -> None:
    """``GET /{tenant}`` serves ``/openapi.json``: the exclusion opens a business route."""
    config_path = write_project(tmp_path, rest=_auth(), prefix="/", route_path="/{tenant}")
    with pytest.raises(ConfigError, match="business route"):
        create_app(config_path)


def test_a_prefixed_interface_leaves_the_exclusions_alone(tmp_path: Path) -> None:
    """The ordinary shape — routes under a prefix — captures nothing."""
    config_path = write_project(tmp_path, rest=_auth())
    with pytest.warns(UserWarning):
        app = create_app(config_path)
    assert "/ping/" in _route_paths(app)


def test_an_unauthenticated_app_has_nothing_to_exclude(tmp_path: Path) -> None:
    """Without authentication there is no exclusion list to get wrong."""
    config_path = write_project(tmp_path, prefix="/", route_path="/{tenant}")
    app = create_app(config_path)
    assert "/{tenant}" in _route_paths(app)


# ---------------------------------------------------------------------------
# Schema exposure
# ---------------------------------------------------------------------------


def test_the_openapi_url_can_be_switched_off(tmp_path: Path) -> None:
    """An operator disabling the docs must also be able to withhold the schema."""
    config_path = write_project(
        tmp_path,
        rest={"docs_url": None, "redoc_url": None, "openapi_url": None},
    )
    app = create_app(config_path)
    assert _OPENAPI not in _route_paths(app)


def test_an_authenticated_app_warns_about_its_anonymous_schema(tmp_path: Path) -> None:
    """The schema maps every route and field: publishing it anonymously is a slip."""
    config_path = write_project(tmp_path, rest=_auth())
    with pytest.warns(UserWarning, match="schema"):
        create_app(config_path)


def test_withholding_the_schema_silences_the_warning(tmp_path: Path) -> None:
    """The warning must be actionable: following it removes it."""
    config_path = write_project(
        tmp_path,
        rest=_auth(docs_url=None, redoc_url=None, openapi_url=None),
    )
    with warnings.catch_warnings(record=True) as raised:
        warnings.simplefilter("always")
        create_app(config_path)

    assert not [w for w in raised if "schema" in str(w.message)]


def test_moving_the_docs_moves_the_exclusion(tmp_path: Path) -> None:
    """Default exclusions follow the effective paths, not a hardcoded tuple."""
    config_path = write_project(
        tmp_path,
        rest=_auth(docs_url="/internal/docs", redoc_url=None, openapi_url=None),
    )
    with pytest.warns(UserWarning, match="/internal/docs"):
        create_app(config_path)


# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------


def test_a_wildcard_cors_with_credentials_aborts_startup(tmp_path: Path) -> None:
    """The combination Starlette silently turns into 'any origin, with cookies'."""
    config_path = write_project(
        tmp_path,
        rest={"cors": {"allow_origins": ["*"], "allow_credentials": True}},
    )
    with pytest.raises(ConfigError, match="allow_credentials"):
        create_app(config_path)


def test_a_named_origin_with_credentials_starts(tmp_path: Path) -> None:
    """Listing the origins is the safe way to allow credentialed calls."""
    config_path = write_project(
        tmp_path,
        rest={
            "cors": {
                "allow_origins": ["https://app.example.com"],
                "allow_credentials": True,
            }
        },
    )
    assert create_app(config_path) is not None
