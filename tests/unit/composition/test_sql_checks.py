"""The ClickHouse startup probe is a check: it runs before forking, not in every child.

:meth:`Application.check` opens temporary clients, runs the driver checks
and the fail-closed sentinel-role probe, and closes them, so a prefork
parent's ``worker_init`` fails before forking. A child opening with
``check=False`` creates its clients without probing again.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.composition import Application, CompositionSource, Recipe, compose_application
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.persistence.profile import WORKER_PROFILE
from loom.core.sql.clickhouse import registry as clickhouse_registry
from tests.helpers.composition import composition_source
from tests.unit.core.sql._fakes import (
    DEFAULT_ROLES,
    DEFAULT_URL,
    FakeClickHouseClient,
    RecordingClientFactory,
)


class _Bare:
    def recipe(self, source: CompositionSource) -> Recipe:
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(models=(), use_cases=(), interfaces=()),
            backend=source.backend(default="none"),
        )

    def bind(self, application: Application) -> Application:
        return application


def _application(tmp_path: Path) -> Application:
    connection = {
        "backend": "clickhouse",
        "url": DEFAULT_URL,
        "allowed_roles": list(DEFAULT_ROLES),
        "default_role": DEFAULT_ROLES[0],
    }
    source = composition_source(
        ConfigContext.from_dict(
            {"app": {"name": "sql"}, "sql": {"connections": {"analytics": connection}}}
        ),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    return compose_application(source, _Bare())


def _factory(
    monkeypatch: pytest.MonkeyPatch, *clients: FakeClickHouseClient
) -> RecordingClientFactory:
    factory = RecordingClientFactory(*clients)
    monkeypatch.setattr(clickhouse_registry, "create_driver_client", factory)
    return factory


def _probes(factory: RecordingClientFactory) -> int:
    return sum(
        1
        for client in factory.created
        for call in client.queries
        if call.settings is not None and "role" in call.settings
    )


async def test_a_failing_role_probe_fails_the_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = _factory(monkeypatch, FakeClickHouseClient(ignore_role=True))
    application = _application(tmp_path)

    with pytest.raises(ConfigError, match="sentinel role"):
        await application.check()

    assert [client.closed for client in factory.created] == [True]


async def test_the_check_probes_and_leaves_no_client_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = _factory(monkeypatch)
    application = _application(tmp_path)

    await application.check()

    assert _probes(factory) == 1
    assert [client.closed for client in factory.created] == [True]


async def test_opening_without_check_after_a_check_does_not_probe_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = _factory(monkeypatch)
    application = _application(tmp_path)
    await application.check()

    async with application.running(check=False):
        assert application.sql.registry is not None
        assert application.sql.registry.executor("analytics") is not None

    assert (_probes(factory), len(factory.created)) == (1, 2)


async def test_opening_with_check_probes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = _factory(monkeypatch)
    application = _application(tmp_path)

    async with application.running():
        pass

    assert _probes(factory) == 1


async def test_opening_unchecked_still_probes_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _factory(monkeypatch, FakeClickHouseClient(ignore_role=True))
    application = _application(tmp_path)

    with pytest.raises(ConfigError, match="sentinel role"):
        async with application.running(check=False):
            pass
