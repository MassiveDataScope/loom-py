"""Seal instantiates every ``APPLICATION`` binding, as ``validate()`` does, reachable or not."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope
from tests.unit.core.di.graph import acme


class AcmeCountedSettings:
    built = 0

    def __init__(self) -> None:
        AcmeCountedSettings.built += 1


class AcmeMetrics:
    built = 0

    def __init__(self) -> None:
        AcmeMetrics.built += 1


class AcmeSettingsReport:
    def __init__(self, settings: AcmeCountedSettings) -> None:
        self.settings = settings


@pytest.fixture(autouse=True)
def _reset() -> None:
    AcmeCountedSettings.built = AcmeMetrics.built = 0


@pytest.fixture
def container() -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeCountedSettings, AcmeCountedSettings, scope=Scope.APPLICATION)
    container.register(AcmeMetrics, AcmeMetrics, scope=Scope.APPLICATION)
    return container


class TestSealInstantiatesApplicationBindings:
    def test_seal_builds_reachable_and_unreachable_singletons_once(
        self, container: LoomContainer
    ) -> None:
        container.seal(acme.analysed(container, AcmeSettingsReport))
        container.resolve(AcmeCountedSettings)
        container.resolve(AcmeMetrics)

        assert (AcmeCountedSettings.built, AcmeMetrics.built) == (1, 1)

    def test_seal_without_an_analysis_builds_every_singleton(
        self, container: LoomContainer
    ) -> None:
        container.seal()

        assert (AcmeCountedSettings.built, AcmeMetrics.built) == (1, 1)

    def test_validate_instantiates_every_application_binding(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeSettingsReport, AcmeSettingsReport)

        container.validate()

        assert (AcmeCountedSettings.built, AcmeMetrics.built) == (1, 1)

    def test_a_failing_singleton_fails_seal_as_validate_words_it(
        self, container: LoomContainer
    ) -> None:
        def offline() -> AcmeMetrics:
            raise RuntimeError("acme metrics offline")

        container.register(AcmeMetrics, offline, scope=Scope.APPLICATION)

        with pytest.raises(ResolutionError) as sealed:
            container.seal(acme.analysed(container, AcmeSettingsReport))
        with pytest.raises(ResolutionError) as validated:
            container.validate()

        assert str(sealed.value) == str(validated.value)
        assert "Failed to instantiate APPLICATION-scope binding for AcmeMetrics" in str(
            sealed.value
        )


class AcmeCountedSession:
    built = 0

    def __init__(self) -> None:
        AcmeCountedSession.built += 1


class AcmeSessionLedger:
    def __init__(self, session: AcmeCountedSession) -> None:
        self.session = session


class TestSealCompilesPerExecutionBindings:
    def test_seal_compiles_reachable_per_execution_bindings_without_building_them(self) -> None:
        AcmeCountedSession.built = 0
        container = LoomContainer()
        container.register(AcmeCountedSession, AcmeCountedSession)

        container.seal(acme.analysed(container, AcmeSessionLedger))

        assert AcmeCountedSession.built == 0
        assert container._compiled.keys() >= {AcmeCountedSession}  # noqa: SLF001 - compiled, not built
