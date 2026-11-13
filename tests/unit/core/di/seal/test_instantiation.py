"""Seal instantiates every ``APPLICATION`` binding, as ``validate()`` does, reachable or not."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope
from tests.unit.core.di.graph import acme


class AcmeSettings:
    built = 0

    def __init__(self) -> None:
        AcmeSettings.built += 1


class AcmeMetrics:
    built = 0

    def __init__(self) -> None:
        AcmeMetrics.built += 1


class AcmeReport:
    def __init__(self, settings: AcmeSettings) -> None:
        self.settings = settings


@pytest.fixture(autouse=True)
def _reset() -> None:
    AcmeSettings.built = AcmeMetrics.built = 0


def _bound() -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeSettings, AcmeSettings, scope=Scope.APPLICATION)
    container.register(AcmeMetrics, AcmeMetrics, scope=Scope.APPLICATION)
    return container


def test_seal_builds_reachable_and_unreachable_singletons_once() -> None:
    container = _bound()

    container.seal(acme.analysed(container, AcmeReport))
    container.resolve(AcmeSettings)
    container.resolve(AcmeMetrics)

    assert (AcmeSettings.built, AcmeMetrics.built) == (1, 1)


def test_seal_without_an_analysis_builds_every_singleton() -> None:
    container = _bound()

    container.seal()

    assert (AcmeSettings.built, AcmeMetrics.built) == (1, 1)


def test_validate_instantiates_every_application_binding() -> None:
    container = _bound()
    container.register(AcmeReport, AcmeReport)

    container.validate()

    assert (AcmeSettings.built, AcmeMetrics.built) == (1, 1)


def test_a_failing_singleton_fails_seal_as_validate_words_it() -> None:
    def offline() -> AcmeMetrics:
        raise RuntimeError("acme metrics offline")

    container = _bound()
    container.register(AcmeMetrics, offline, scope=Scope.APPLICATION)

    with pytest.raises(ResolutionError) as sealed:
        container.seal(acme.analysed(container, AcmeReport))
    with pytest.raises(ResolutionError) as validated:
        container.validate()

    assert str(sealed.value) == str(validated.value)
    assert "Failed to instantiate APPLICATION-scope binding for AcmeMetrics" in str(sealed.value)


class AcmeSession:
    built = 0

    def __init__(self) -> None:
        AcmeSession.built += 1


class AcmeLedger:
    def __init__(self, session: AcmeSession) -> None:
        self.session = session


def test_seal_compiles_reachable_per_execution_bindings_without_building_them() -> None:
    AcmeSession.built = 0
    container = LoomContainer()
    container.register(AcmeSession, AcmeSession)

    container.seal(acme.analysed(container, AcmeLedger))

    assert AcmeSession.built == 0
    assert container._compiled.keys() >= {AcmeSession}  # noqa: SLF001 - compiled, not built
