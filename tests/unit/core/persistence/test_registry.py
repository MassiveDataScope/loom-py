"""Store backend registry: resolution by name and the ``none`` backend."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

import loom.core.plugins.entrypoints as entrypoints_module
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.plugins.entrypoints import (
    DuplicateEntryPointError,
    MissingDependency,
    select_entry_point,
)
from loom.core.stores import (
    NullStorePlugin,
    StorePlugin,
    StoreWiring,
    resolve_store_plugin,
)
from loom.core.stores.profile import SERVER_PROFILE, StorePluginContext
from tests.helpers.persistence import build_backend

_GROUP = "loom.stores.plugins"
_SRC = Path(__file__).resolve().parents[4] / "src"


class AcmePersistenceDist:
    def __init__(self, name: str, requires: tuple[str, ...] = ()) -> None:
        self.name = name
        self.requires = requires


class AcmePersistenceEntryPoint:
    def __init__(
        self, name: str, dist_name: str, target: object, requires: tuple[str, ...] = ()
    ) -> None:
        self.name = name
        self.group = _GROUP
        self.dist = AcmePersistenceDist(dist_name, requires)
        self._target = target

    def load(self) -> object:
        return self._target


class AcmePersistenceEntryPoints:
    def __init__(self, entries: tuple[AcmePersistenceEntryPoint, ...]) -> None:
        self._entries = entries

    def select(self, *, group: str) -> tuple[AcmePersistenceEntryPoint, ...]:
        return tuple(ep for ep in self._entries if ep.group == group)


class _FakeBackend:
    name: ClassVar[str] = "fake"
    repository_type: ClassVar[type | None] = None

    def build(self, ctx: ConfigContext, models: object, runtime: StorePluginContext) -> StoreWiring:
        return NullStorePlugin().build(ctx, (), runtime)


def _install(
    monkeypatch: pytest.MonkeyPatch, entries: tuple[AcmePersistenceEntryPoint, ...]
) -> None:
    monkeypatch.setattr(
        entrypoints_module, "entry_points", lambda: AcmePersistenceEntryPoints(entries)
    )


class TestResolveBackend:
    def test_unknown_name_raises_config_error_listing_registered_names(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(
            monkeypatch,
            (
                AcmePersistenceEntryPoint("zeta", "loom-zeta", _FakeBackend),
                AcmePersistenceEntryPoint("alpha", "loom-alpha", _FakeBackend),
            ),
        )

        with pytest.raises(ConfigError) as excinfo:
            resolve_store_plugin("missing")

        message = str(excinfo.value)
        assert "'missing'" in message
        assert "alpha, zeta" in message

    def test_unknown_name_with_empty_group_reports_no_backends_registered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(ConfigError, match="No store plugins are registered"):
            resolve_store_plugin("missing")

    def test_injected_entry_point_resolves_to_an_instance_of_its_class(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, (AcmePersistenceEntryPoint("fake", "loom-fake", _FakeBackend),))

        backend = resolve_store_plugin("fake")

        assert isinstance(backend, _FakeBackend)
        assert backend.name == "fake"

    def test_duplicate_names_fail_closed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(
            monkeypatch,
            (
                AcmePersistenceEntryPoint("fake", "loom-fake-a", _FakeBackend),
                AcmePersistenceEntryPoint("fake", "loom-fake-b", _FakeBackend),
            ),
        )

        with pytest.raises(DuplicateEntryPointError):
            resolve_store_plugin("fake")

    def test_none_backend_is_registered_by_loom(self) -> None:
        backend = resolve_store_plugin("none")

        assert isinstance(backend, NullStorePlugin)


class _UntouchedContainer:
    """A container that fails the test as soon as anything reads or calls it."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the registration module touched container.{name}")


class TestNullStorePlugin:
    def _wiring(self) -> StoreWiring:
        backend: StorePlugin = NullStorePlugin()
        return build_backend(backend, ConfigContext.from_dict({}), (), SERVER_PROFILE)

    def test_build_has_no_unit_of_work_and_no_default_repository_type(self) -> None:
        wiring = self._wiring()

        assert wiring.uow_factory is None
        assert NullStorePlugin.repository_type is None
        assert wiring.readiness is None

    def test_registration_module_registers_nothing(self) -> None:
        self._wiring().repo_registration_module(cast(LoomContainer, _UntouchedContainer()))


class TestPersistencePackageImport:
    def test_persistence_package_imports_without_sqlalchemy_or_boto3(self) -> None:
        script = (
            "import sys\n"
            "sys.modules['sqlalchemy'] = None\n"
            "sys.modules['boto3'] = None\n"
            "import loom.core.stores\n"
            "adapters = ('loom.stores',)\n"
            "leaked = sorted(m for m in sys.modules if m.startswith(adapters))\n"
            "assert not leaked, leaked\n"
        )
        env = {**os.environ, "PYTHONPATH": str(_SRC)}
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=False, env=env
        )

        assert result.returncode == 0, result.stderr


class _BrokenEntryPoint(AcmePersistenceEntryPoint):
    def __init__(self, name: str, missing: str, requires: tuple[str, ...] = ()) -> None:
        super().__init__(name, "loom-kernel", _FakeBackend, requires)
        self._missing = missing

    def load(self) -> object:
        raise ModuleNotFoundError(f"No module named {self._missing!r}", name=self._missing)


_KERNEL_REQUIRES = (
    "pymongo>=4.18.3,<5.0; extra == 'mongo'",
    "boto3>=1.35; extra == 'aws'",
    "fastapi>=0.115; extra == 'rest'",
    "fastapi>=0.115; extra == 'lambda'",
    "Typing-Extensions>=4; extra == 'typing'",
    "msgspec>=0.18",
)


class TestBrokenBackendImport:
    @pytest.mark.parametrize(
        ("missing", "installs"),
        [
            ("pymongo", "loom-kernel[mongo]"),
            ("pymongo.errors", "loom-kernel[mongo]"),
            ("boto3", "loom-kernel[aws]"),
            ("typing_extensions", "loom-kernel[typing]"),
            ("fastapi", "loom-kernel[rest] or loom-kernel[lambda]"),
        ],
    )
    def test_a_missing_third_party_module_names_the_path_the_module_and_the_extras(
        self,
        monkeypatch: pytest.MonkeyPatch,
        missing: str,
        installs: str,
    ) -> None:
        _install(monkeypatch, (_BrokenEntryPoint("audit", missing, _KERNEL_REQUIRES),))

        with pytest.raises(ConfigError) as excinfo:
            resolve_store_plugin("audit", path="stores.audit.backend")

        root = missing.partition(".")[0]
        assert str(excinfo.value) == (
            f"stores.audit.backend: store plugin 'audit' needs {root!r}, "
            f"which is not installed: install {installs}"
        )
        assert isinstance(excinfo.value.__cause__, ModuleNotFoundError)

    def test_a_module_no_extra_provides_names_only_the_module(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, (_BrokenEntryPoint("audit", "acme_driver", _KERNEL_REQUIRES),))

        with pytest.raises(ConfigError) as excinfo:
            resolve_store_plugin("audit", path="stores.audit.backend")

        assert str(excinfo.value) == (
            "stores.audit.backend: store plugin 'audit' needs 'acme_driver', which is not installed"
        )

    @pytest.mark.parametrize("missing", ["loom.stores.mongo.plugin", "loom"])
    def test_a_missing_loom_module_propagates_unchanged(
        self, monkeypatch: pytest.MonkeyPatch, missing: str
    ) -> None:
        _install(monkeypatch, (_BrokenEntryPoint("audit", missing, _KERNEL_REQUIRES),))

        with pytest.raises(ModuleNotFoundError) as excinfo:
            resolve_store_plugin("audit", path="stores.audit.backend")

        assert excinfo.value.name == missing


class TestShippedBackendExtras:
    @pytest.mark.parametrize(
        ("backend", "module", "extra"),
        [("mongo", "pymongo", "mongo"), ("dynamodb", "boto3", "aws")],
    )
    def test_the_distribution_metadata_names_the_extra_of_a_builtin_backend(
        self, backend: str, module: str, extra: str
    ) -> None:
        entry = select_entry_point(_GROUP, backend, on_duplicate="error")
        assert entry is not None

        missing = MissingDependency(module=module, entry=entry)

        assert extra in missing.extras
        assert f"loom-kernel[{extra}]" in missing.describe()
