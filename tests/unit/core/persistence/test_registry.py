"""Persistence backend registry: resolution by name and the ``none`` backend."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

import loom.core.plugins.entrypoints as entrypoints_module
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.persistence import (
    NoneBackend,
    PersistenceBackend,
    PersistenceWiring,
    resolve_backend,
)
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.plugins.entrypoints import DuplicateEntryPointError

_GROUP = "loom.persistence.backends"
_SRC = Path(__file__).resolve().parents[4] / "src"


class _FakeDist:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    def __init__(self, name: str, dist_name: str, target: object) -> None:
        self.name = name
        self.group = _GROUP
        self.dist = _FakeDist(dist_name)
        self._target = target

    def load(self) -> object:
        return self._target


class _FakeEntryPoints:
    def __init__(self, entries: tuple[_FakeEntryPoint, ...]) -> None:
        self._entries = entries

    def select(self, *, group: str) -> tuple[_FakeEntryPoint, ...]:
        return tuple(ep for ep in self._entries if ep.group == group)


class _FakeBackend:
    name: ClassVar[str] = "fake"

    def build(
        self, ctx: ConfigContext, models: object, runtime: BackendRuntime
    ) -> PersistenceWiring:
        return NoneBackend().build(ctx, (), runtime)


def _install(monkeypatch: pytest.MonkeyPatch, entries: tuple[_FakeEntryPoint, ...]) -> None:
    monkeypatch.setattr(entrypoints_module, "entry_points", lambda: _FakeEntryPoints(entries))


class TestResolveBackend:
    def test_unknown_name_raises_config_error_listing_registered_names(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(
            monkeypatch,
            (
                _FakeEntryPoint("zeta", "loom-zeta", _FakeBackend),
                _FakeEntryPoint("alpha", "loom-alpha", _FakeBackend),
            ),
        )

        with pytest.raises(ConfigError) as excinfo:
            resolve_backend("missing")

        message = str(excinfo.value)
        assert "'missing'" in message
        assert "alpha, zeta" in message

    def test_unknown_name_with_empty_group_reports_no_backends_registered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(ConfigError, match="No persistence backends are registered"):
            resolve_backend("missing")

    def test_injected_entry_point_resolves_to_an_instance_of_its_class(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, (_FakeEntryPoint("fake", "loom-fake", _FakeBackend),))

        backend = resolve_backend("fake")

        assert isinstance(backend, _FakeBackend)
        assert backend.name == "fake"

    def test_duplicate_names_fail_closed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(
            monkeypatch,
            (
                _FakeEntryPoint("fake", "loom-fake-a", _FakeBackend),
                _FakeEntryPoint("fake", "loom-fake-b", _FakeBackend),
            ),
        )

        with pytest.raises(DuplicateEntryPointError):
            resolve_backend("fake")

    def test_none_backend_is_registered_by_loom(self) -> None:
        backend = resolve_backend("none")

        assert isinstance(backend, NoneBackend)


class TestNoneBackend:
    def _wiring(self) -> PersistenceWiring:
        backend: PersistenceBackend = NoneBackend()
        return backend.build(ConfigContext.from_dict({}), (), BackendRuntime(SERVER_PROFILE))

    def test_build_has_no_unit_of_work_and_no_default_repository_type(self) -> None:
        wiring = self._wiring()

        assert wiring.uow_factory is None
        assert wiring.default_repository_type is None
        assert wiring.readiness is None

    def test_registration_module_registers_nothing(self) -> None:
        container = MagicMock(spec=LoomContainer)

        self._wiring().repo_registration_module(container)

        assert not container.method_calls


def test_persistence_package_imports_without_sqlalchemy_or_boto3() -> None:
    script = (
        "import sys\n"
        "sys.modules['sqlalchemy'] = None\n"
        "sys.modules['boto3'] = None\n"
        "import loom.core.persistence\n"
        "assert not [m for m in sys.modules if m.startswith('loom.core.repository')], "
        "sorted(m for m in sys.modules if m.startswith('loom.core.repository'))\n"
    )
    env = {**os.environ, "PYTHONPATH": str(_SRC)}
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False, env=env
    )

    assert result.returncode == 0, result.stderr


class _BrokenEntryPoint(_FakeEntryPoint):
    def load(self) -> object:
        raise ModuleNotFoundError("No module named 'sqlalchemy'", name="sqlalchemy")


def test_backend_whose_module_import_fails_reports_the_extra_to_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, (_BrokenEntryPoint("sqlalchemy", "loom-kernel", _FakeBackend),))

    with pytest.raises(ConfigError) as excinfo:
        resolve_backend("sqlalchemy")

    message = str(excinfo.value)
    assert "'sqlalchemy'" in message
    assert "loom-kernel[sqlalchemy]" in message
    assert isinstance(excinfo.value.__cause__, ModuleNotFoundError)
