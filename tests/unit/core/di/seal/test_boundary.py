"""A sealed container is complete: any change raises ``ContainerSealed`` and changes nothing."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

from loom.core.di import ContainerSealed, ResolutionError
from loom.core.di.compiler import COLD_PATH
from loom.core.di.container import LoomContainer
from loom.core.di.introspection import Constructor, plain_key
from tests.unit.core.di.seal.doubles import AcmeLedger, AcmeOtherLedger


def _nothing_implicit(cls: type[Any]) -> Constructor:
    return ()


_REGISTERS: dict[str, Callable[[LoomContainer], None]] = {
    "register": lambda container: container.register(AcmeLedger, AcmeOtherLedger),
    "register_default": lambda container: container.register_default(AcmeLedger, AcmeOtherLedger),
    "register_instance": lambda container: container.register_instance(
        AcmeLedger, AcmeOtherLedger()
    ),
    "register_repo": lambda container: container.register_repo(AcmeLedger, AcmeOtherLedger),
}


class TestSealedContainer:
    @pytest.mark.parametrize("name", list(_REGISTERS), ids=list(_REGISTERS))
    def test_each_register_raises_naming_the_key_and_how_to_reconfigure(
        self, sealed: LoomContainer, name: str
    ) -> None:
        with pytest.raises(ContainerSealed) as caught:
            _REGISTERS[name](sealed)

        assert str(caught.value).startswith("AcmeLedger cannot change: the container is sealed")
        assert "build a new container to reconfigure" in str(caught.value)

    @pytest.mark.parametrize("name", list(_REGISTERS), ids=list(_REGISTERS))
    def test_a_refused_register_changes_nothing(self, sealed: LoomContainer, name: str) -> None:
        ledger, token = sealed.resolve(AcmeLedger), sealed.bindings()[AcmeLedger]

        with pytest.raises(ContainerSealed):
            _REGISTERS[name](sealed)

        assert sealed.bindings()[AcmeLedger] is token
        assert sealed.resolve(AcmeLedger) is ledger
        assert not sealed.has_repo_mapping(AcmeLedger)

    def test_is_a_resolution_error(self, sealed: LoomContainer) -> None:
        with pytest.raises(ResolutionError):
            sealed.register(AcmeLedger, AcmeOtherLedger)

    def test_reading_constructors_a_new_way_is_refused(self, sealed: LoomContainer) -> None:
        with pytest.raises(ContainerSealed, match="build a new container to reconfigure"):
            sealed.read_with(classify_key=plain_key, implicit=_nothing_implicit)

    def test_asking_for_the_way_it_already_reads_returns_its_graph(self) -> None:
        container = LoomContainer()
        graph = container.read_with(classify_key=plain_key, implicit=_nothing_implicit)
        container.seal()

        assert container.read_with(classify_key=plain_key, implicit=_nothing_implicit) is graph


class TestRegisterDefault:
    def test_a_default_waiting_for_the_lock_keeps_a_binding_made_meanwhile(self) -> None:
        """The check and the binding are one locked step: the late default binds nothing."""
        container = LoomContainer()
        with COLD_PATH:
            waiting = threading.Thread(
                target=container.register_default, args=(AcmeLedger, AcmeOtherLedger)
            )
            waiting.start()
            time.sleep(0.05)  # let the thread reach the lock
            container.register(AcmeLedger, AcmeLedger)
        waiting.join(timeout=5)

        assert type(container.resolve(AcmeLedger)) is AcmeLedger

    def test_after_seal_raises_even_when_the_key_is_bound(self, sealed: LoomContainer) -> None:
        with pytest.raises(ContainerSealed):
            sealed.register_default(AcmeLedger, AcmeLedger)
