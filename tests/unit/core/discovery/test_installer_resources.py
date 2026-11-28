"""An installer's resources open, close, check and answer readiness as loom's own do."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from loom.celery.runtime import CeleryRuntime
from loom.composition.application import Application
from loom.composition.project import Project
from loom.core.config import ConfigError
from tests.helpers import acme_session_pool
from tests.helpers.package_writer import PackageWriter

_INSTALLER = """\
from loom.core.di.installer import ConfiguredInstaller
from loom.core.resources import Resource
from tests.helpers.acme_session_pool import PoolSettings, SessionPool


class {name}Pool(SessionPool):
    pass


class Acme{name}Installer(ConfiguredInstaller[PoolSettings]):
    section = "acme.{name}"
    settings_type = PoolSettings

    def install(self, container):
        container.register_instance({name}Pool, {name}Pool("{resource}", self.settings))

    def resources(self, container):
        pool = container.resolve({name}Pool)
        return (
            Resource(
                name="{resource}",
                manager=pool.lifespan,
                check=pool.check,
                readiness=pool.is_ready,
                readiness_key="{key}",
            ),
        )
"""
_CONFIG = (
    "app:\n  name: acme\n  job_service: inline\n  packages: [acme_resources_pkg]\n"
    "stores:\n  default:\n    backend: sqlalchemy\n    url: sqlite+aiosqlite:///\n"
    "celery:\n  broker_url: memory://\n  result_backend: cache+memory://\n"
)


@pytest.fixture(autouse=True)
def events() -> Iterator[list[str]]:
    acme_session_pool.EVENTS.clear()
    yield acme_session_pool.EVENTS
    acme_session_pool.EVENTS.clear()


@pytest.fixture
def compose(write_package: PackageWriter, tmp_path: Path) -> Callable[..., Application]:
    """Compose a project of two installers, ``AcmeFirstInstaller`` then ``AcmeSecondInstaller``.

    Each pool's settings come from the keyword of its name; the second pool's
    resource name and readiness key can be chosen.
    """

    def build(
        first: str = "",
        second: str = "",
        *,
        second_name: str = "second",
        second_key: str = "second",
    ) -> Application:
        source = _INSTALLER.format(name="First", resource="first", key="first")
        source += _INSTALLER.format(name="Second", resource=second_name, key=second_key)
        write_package("acme_resources_pkg", {"installers.py": source})
        config = tmp_path / "app.yaml"
        config.write_text(f"{_CONFIG}acme:\n  First: {{{first}}}\n  Second: {{{second}}}\n")
        return Project.load(str(config), environment="test").compose(CeleryRuntime())

    return build


class TestInstallerResourceLifecycle:
    @pytest.mark.asyncio
    async def test_resources_open_after_the_stores_and_before_the_timer_and_close_in_reverse(
        self, compose: Callable[..., Application], events: list[str]
    ) -> None:
        application = compose()

        async with application.running(check=False):
            opened = list(events)

        labels = [resource.label for resource in application.resources]
        assert labels[-4:] == [
            "store:default",
            "application:first",
            "application:second",
            "timer:jobs",
        ]
        assert opened == ["open:first", "open:second"]
        assert events == ["open:first", "open:second", "close:second", "close:first"]

    @pytest.mark.asyncio
    async def test_a_failure_while_opening_closes_what_already_opened_and_surfaces(
        self, compose: Callable[..., Application], events: list[str]
    ) -> None:
        application = compose(second="fail_open: true")

        with pytest.raises(RuntimeError, match="open second"):
            async with application.running(check=False):
                pytest.fail("the application must not open")

        assert events == ["open:first", "open:second", "close:first"]

    @pytest.mark.asyncio
    async def test_two_failing_closes_are_gathered_and_every_resource_still_closes(
        self, compose: Callable[..., Application], events: list[str]
    ) -> None:
        application = compose(first="fail_close: true", second="fail_close: true")

        with pytest.raises(BaseExceptionGroup) as raised:
            async with application.running(check=False):
                pass

        assert sorted(str(error) for error in raised.value.exceptions) == [
            "close first",
            "close second",
        ]
        assert events == ["open:first", "open:second", "close:second", "close:first"]


class TestInstallerResourceChecks:
    @pytest.mark.asyncio
    async def test_the_check_runs_right_before_the_resource_opens(
        self, compose: Callable[..., Application], events: list[str]
    ) -> None:
        application = compose()

        async with application.running():
            pass

        assert events[:4] == ["check:first", "open:first", "check:second", "open:second"]

    @pytest.mark.asyncio
    async def test_application_check_runs_the_checks_and_opens_nothing(
        self, compose: Callable[..., Application], events: list[str]
    ) -> None:
        application = compose()

        await application.check()

        assert events == ["check:first", "check:second"]


class TestInstallerResourceReadiness:
    @pytest.mark.asyncio
    async def test_readiness_is_reported_under_the_readiness_key(
        self, compose: Callable[..., Application]
    ) -> None:
        probes = compose(second_key="reports").readiness_probes()

        assert {"first", "reports"} <= probes.keys()
        assert await probes["reports"]() is True


class TestInstallerResourceNames:
    @pytest.mark.parametrize(
        ("second_name", "second_key", "what"),
        [("first", "second", "resource 'application:first'"), ("second", "first", "readiness key")],
        ids=["same-name", "same-readiness-key"],
    )
    def test_a_duplicate_is_refused_naming_both_installers(
        self, compose: Callable[..., Application], second_name: str, second_key: str, what: str
    ) -> None:
        with pytest.raises(ConfigError, match=what) as refused:
            compose(second_name=second_name, second_key=second_key)

        assert "AcmeSecondInstaller" in str(refused.value)
        assert "AcmeFirstInstaller" in str(refused.value)
