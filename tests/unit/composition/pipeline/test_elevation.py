"""The composition binds its elevation sink before start-up builds anything, executor included."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.composition.pipeline import registration
from loom.core.authz.elevation import ElevationSink
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from tests.unit.core.authz._elevation_doubles import FakeSink


class AcmeSinkUser:
    """An ``APPLICATION`` service that takes the elevation sink when it is built."""

    def __init__(self, sink: ElevationSink) -> None:
        self.sink = sink


def _binds_sink_user(container: LoomContainer) -> None:
    container.register(
        AcmeSinkUser,
        lambda: AcmeSinkUser(container.resolve(ElevationSink)),
        scope=Scope.APPLICATION,
    )


class TestElevationSink:
    @pytest.fixture
    def sink(self, monkeypatch: pytest.MonkeyPatch) -> FakeSink:
        """The sink the composition builds from its stores."""
        built = FakeSink()
        monkeypatch.setattr(registration, "_elevation_sink", lambda wirings: built)
        return built

    def test_the_executors_sink_is_the_one_bound(
        self, overridden: Callable[..., Application], sink: FakeSink
    ) -> None:
        kernel = overridden().kernel

        assert kernel.container.resolve(ElevationSink) is sink
        assert kernel.executor._elevation_sink is sink  # noqa: SLF001

    def test_the_sink_is_bound_before_the_application_services_are_built(
        self, overridden: Callable[..., Application], sink: FakeSink
    ) -> None:
        kernel = overridden(modules=(_binds_sink_user,)).kernel

        assert kernel.container.resolve(AcmeSinkUser).sink is sink
