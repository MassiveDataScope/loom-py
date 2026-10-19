"""``GET /health``: backend readiness, authentication exclusion and route collisions.

The route is the one place an orchestrator asks whether the application can
serve; every test here pins one way it could lie or disappear: a backend that
cannot reach its store, a mechanism that would demand credentials for it, a
catch-all route that would capture it, or an interface that would replace it.
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import loom.core.plugins.entrypoints as entrypoints_module
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.identity import Identity
from loom.core.model import BaseModel
from loom.core.persistence import NoneBackend, PersistenceWiring
from loom.rest.auth import RequestCredentials
from loom.rest.compiler import InterfaceCompilationError
from loom.rest.fastapi import health as health_module
from loom.rest.fastapi.auto import create_app
from tests.unit.rest._fixture_app import write_project

_HEALTH = "/health"
_PING = "/ping/"
_NO_DOCS: dict[str, Any] = {"docs_url": None, "redoc_url": None, "openapi_url": None}

_SECRET_PATH = str(Path(tempfile.mkdtemp()) / "hs.key")
Path(_SECRET_PATH).write_text("unit-test-secret")
_JWT_SECTION: dict[str, Any] = {
    "secret_path": _SECRET_PATH,
    "algorithms": ["HS256"],
    "audience": "loom-api",
}


def _jwt_rest(**jwt_overrides: Any) -> dict[str, Any]:
    return {"auth": {"jwt": {**_JWT_SECTION, **jwt_overrides}}, **_NO_DOCS}


def _get(app: FastAPI, path: str) -> tuple[int, Any]:
    """Return the status code and JSON body of ``GET path`` inside the app lifespan."""
    with TestClient(app) as client:
        response = client.get(path)
        return response.status_code, response.json()


class _AlwaysRefuses:
    """Authenticator that never accepts anyone: a served route is an excluded one."""

    name = "never"
    provides_roles = False

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        del credentials
        return None


class _ProbedBackend:
    """Backend delegating to ``none`` with a readiness result set by the test."""

    name: ClassVar[str] = "probed"
    ready: ClassVar[bool | None] = None

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
        wiring = NoneBackend().build(ctx, models)
        readiness = None if self.ready is None else self._probe
        return PersistenceWiring(
            uow_factory=wiring.uow_factory,
            repo_registration_module=wiring.repo_registration_module,
            lifespan_init=wiring.lifespan_init,
            default_repository_type=wiring.default_repository_type,
            prepare_models=wiring.prepare_models,
            readiness=readiness,
        )

    async def _probe(self) -> bool:
        return bool(self.ready)


class _ProbedEntryPoint:
    name = _ProbedBackend.name
    group = "loom.persistence.backends"

    def load(self) -> type[_ProbedBackend]:
        return _ProbedBackend


class _ProbedEntryPoints:
    def select(self, *, group: str) -> tuple[_ProbedEntryPoint, ...]:
        return (_ProbedEntryPoint(),) if group == _ProbedEntryPoint.group else ()


@pytest.fixture
def probed_backend(monkeypatch: pytest.MonkeyPatch) -> type[_ProbedBackend]:
    monkeypatch.setattr(entrypoints_module, "entry_points", lambda: _ProbedEntryPoints())
    monkeypatch.setattr(_ProbedBackend, "ready", None)
    return _ProbedBackend


def _probed_app(tmp_path: Path) -> FastAPI:
    return create_app(write_project(tmp_path, persistence={"backend": "probed"}, database=None))


class TestHealthResponse:
    def test_a_reachable_sqlalchemy_backend_is_ok(self, tmp_path: Path) -> None:
        status, body = _get(create_app(write_project(tmp_path)), _HEALTH)

        assert status == 200
        assert body == {"status": "ok", "backends": {"sqlalchemy": True}}

    def test_a_backend_that_is_not_ready_degrades_the_application(
        self, tmp_path: Path, probed_backend: type[_ProbedBackend]
    ) -> None:
        probed_backend.ready = False

        status, body = _get(_probed_app(tmp_path), _HEALTH)

        assert status == 503
        assert body == {"status": "degraded", "backends": {"probed": False}}

    def test_the_none_backend_reports_no_backends(self, tmp_path: Path) -> None:
        config_path = write_project(tmp_path, persistence={"backend": "none"}, database=None)

        status, body = _get(create_app(config_path), _HEALTH)

        assert status == 200
        assert body == {"status": "ok", "backends": {}}


class TestHealthAuthentication:
    def test_health_is_served_without_credentials_under_jwt(self, tmp_path: Path) -> None:
        app = create_app(write_project(tmp_path, rest=_jwt_rest()))

        with TestClient(app) as client:
            assert client.get(_HEALTH).status_code == 200
            assert client.get(_PING).status_code == 401

    def test_health_is_served_without_credentials_under_a_custom_authenticator(
        self, tmp_path: Path
    ) -> None:
        app = create_app(write_project(tmp_path, rest=_NO_DOCS), authenticator=_AlwaysRefuses())

        with TestClient(app) as client:
            assert client.get(_HEALTH).status_code == 200
            assert client.get(_PING).status_code == 401

    def test_an_explicit_exclusion_list_is_honoured_verbatim(self, tmp_path: Path) -> None:
        """An operator's own list must name ``/health`` to keep it anonymous."""
        app = create_app(write_project(tmp_path, rest=_jwt_rest(exclude_paths=["/docs"])))

        status, _body = _get(app, _HEALTH)

        assert status == 401


class TestHealthMounting:
    def test_health_answers_before_a_catch_all_interface(self, tmp_path: Path) -> None:
        """``GET /{tenant}`` is registered after the route, so it never captures it."""
        config_path = write_project(tmp_path, prefix="/", route_path="/{tenant}")

        _status, body = _get(create_app(config_path), _HEALTH)

        assert body == {"status": "ok", "backends": {"sqlalchemy": True}}

    def test_a_catch_all_route_capturing_health_aborts_an_authenticated_startup(
        self, tmp_path: Path
    ) -> None:
        config_path = write_project(tmp_path, rest=_jwt_rest(), prefix="/", route_path="/{tenant}")

        with pytest.raises(ConfigError, match=r"'/health'.*business route"):
            create_app(config_path)

    def test_an_interface_declaring_health_is_a_compilation_error(self, tmp_path: Path) -> None:
        config_path = write_project(tmp_path, prefix=_HEALTH, route_path="")

        with pytest.raises(InterfaceCompilationError, match=_HEALTH):
            create_app(config_path)

    def test_a_trailing_slash_is_not_excluded_from_auth(self, tmp_path: Path) -> None:
        """Only the literal ``/health`` is anonymous; its slash variant needs credentials."""
        app = create_app(write_project(tmp_path, rest=_jwt_rest()))

        with TestClient(app) as client:
            assert client.get(f"{_HEALTH}/", follow_redirects=False).status_code == 401


class _GatedProbe:
    """Readiness probe that blocks on an event and counts its invocations."""

    def __init__(self, *, ready: bool = True) -> None:
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self._ready = ready

    async def __call__(self) -> bool:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return self._ready


class _CountingProbe:
    """Readiness probe answering at once and counting its invocations."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> bool:
        self.calls += 1
        return True


async def _sleeping_probe() -> bool:
    await asyncio.sleep(1)
    return True


def _frozen_clock(monkeypatch: pytest.MonkeyPatch, start: float = 100.0) -> list[float]:
    """Pin the module clock to a mutable cell and return it."""
    now = [start]
    monkeypatch.setattr(health_module, "monotonic", lambda: now[0])
    return now


class TestCachedProbe:
    async def test_concurrent_calls_share_one_probe(self) -> None:
        probe = _GatedProbe()
        cached = health_module._CachedProbe(probe)

        tasks = [asyncio.create_task(cached()) for _ in range(3)]
        await probe.entered.wait()
        probe.release.set()
        results = await asyncio.gather(*tasks)

        assert results == [True, True, True]
        assert probe.calls == 1

    async def test_a_call_within_the_ttl_does_not_probe_again(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = _frozen_clock(monkeypatch)
        probe = _CountingProbe()
        cached = health_module._CachedProbe(probe)

        await cached()
        now[0] += health_module.PROBE_TTL_SECONDS / 2
        await cached()

        assert probe.calls == 1

    async def test_a_call_after_the_ttl_probes_again(self, monkeypatch: pytest.MonkeyPatch) -> None:
        now = _frozen_clock(monkeypatch)
        probe = _CountingProbe()
        cached = health_module._CachedProbe(probe)

        await cached()
        now[0] += health_module.PROBE_TTL_SECONDS
        await cached()

        assert probe.calls == 2


class TestHealthProbeBudget:
    def test_a_probe_exceeding_the_timeout_degrades_the_application(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(health_module, "PROBE_TIMEOUT_SECONDS", 0.01)
        app = FastAPI()
        health_module.mount_health(app, "slow", _sleeping_probe)

        status, body = _get(app, _HEALTH)

        assert status == 503
        assert body == {"status": "degraded", "backends": {"slow": False}}
