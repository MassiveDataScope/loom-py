"""A sealed container is complete: any change raises ``ContainerSealed`` and changes nothing."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from loom.core.di import ContainerSealed, ResolutionError
from loom.core.di.compiler import STRUCTURE_LOCK
from loom.core.di.container import LoomContainer
from loom.core.execution import ExecutionKinds, Kind
from loom.core.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from tests.unit.core.di.seal.doubles import AcmeBoundLedger, AcmeLateLedger

_REGISTERS: dict[str, Callable[[LoomContainer], None]] = {
    "register": lambda container: container.register(AcmeBoundLedger, AcmeLateLedger),
    "register_default": lambda container: container.register_default(
        AcmeBoundLedger, AcmeLateLedger
    ),
    "register_instance": lambda container: container.register_instance(
        AcmeBoundLedger, AcmeLateLedger()
    ),
    "register_repo": lambda container: container.register_repo(AcmeBoundLedger, AcmeLateLedger),
}


class TestSealedContainer:
    @pytest.mark.parametrize("name", list(_REGISTERS), ids=list(_REGISTERS))
    def test_each_register_raises_naming_the_key_and_how_to_reconfigure(
        self, sealed: LoomContainer, name: str
    ) -> None:
        with pytest.raises(ContainerSealed) as caught:
            _REGISTERS[name](sealed)

        assert str(caught.value).startswith(
            "AcmeBoundLedger cannot change: the container is sealed"
        )
        assert "build a new container to reconfigure" in str(caught.value)

    @pytest.mark.parametrize("name", list(_REGISTERS), ids=list(_REGISTERS))
    def test_a_refused_register_changes_nothing(self, sealed: LoomContainer, name: str) -> None:
        ledger, token = sealed.resolve(AcmeBoundLedger), sealed.bindings()[AcmeBoundLedger]

        with pytest.raises(ContainerSealed):
            _REGISTERS[name](sealed)

        assert sealed.bindings()[AcmeBoundLedger] is token
        assert sealed.resolve(AcmeBoundLedger) is ledger
        assert not sealed.has_repo_mapping(AcmeBoundLedger)

    def test_is_a_resolution_error(self, sealed: LoomContainer) -> None:
        with pytest.raises(ResolutionError):
            sealed.register(AcmeBoundLedger, AcmeLateLedger)

    def test_a_factory_over_a_sealed_bare_container_is_refused(self, sealed: LoomContainer) -> None:
        with pytest.raises(ContainerSealed, match="build a new container to reconfigure"):
            UseCaseFactory(sealed)

    def test_a_factory_over_a_sealed_container_reading_use_cases_shares_its_graph(self) -> None:
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        container.seal()

        assert UseCaseFactory(container).graph is container.graph


_ACME = Kind("acme")


class _AcmeMarker:
    pass


class _AcmeResolver:
    def resolve(self, declaration: object, annotation: object) -> object:
        return declaration


_KIND_CHANGES: dict[str, Callable[[ExecutionKinds], None]] = {
    "declare_type": lambda kinds: kinds.declare_type(_AcmeMarker, _ACME),
    "declare_origin": lambda kinds: kinds.declare_origin(list, _ACME),
    "refuse": lambda kinds: kinds.refuse(_AcmeMarker, "acme declares an input"),
    "bind": lambda kinds: kinds.bind(_ACME, _AcmeResolver()),
}


class TestSealedKinds:
    @pytest.mark.parametrize("name", list(_KIND_CHANGES), ids=list(_KIND_CHANGES))
    def test_each_change_raises_saying_how_to_fix_it(
        self, sealed: LoomContainer, name: str
    ) -> None:
        with pytest.raises(ContainerSealed) as caught:
            _KIND_CHANGES[name](sealed.kinds)

        assert "the container is sealed" in str(caught.value)
        assert "before the container is sealed" in str(caught.value)

    @pytest.mark.parametrize("name", list(_KIND_CHANGES), ids=list(_KIND_CHANGES))
    def test_a_refused_change_changes_nothing(self, sealed: LoomContainer, name: str) -> None:
        with pytest.raises(ContainerSealed):
            _KIND_CHANGES[name](sealed.kinds)

        assert sealed.kinds.kind_for(_AcmeMarker(), list[int]) is None
        assert sealed.kinds.refusal_for(_AcmeMarker()) is None
        assert not sealed.kinds.is_bound(_ACME)

    def test_sealing_again_is_allowed(self, sealed: LoomContainer) -> None:
        sealed.seal()

        with pytest.raises(ContainerSealed):
            sealed.kinds.bind(_ACME, _AcmeResolver())

    def test_a_declaration_made_before_seal_still_resolves(self) -> None:
        container = LoomContainer()
        container.kinds.declare_type(_AcmeMarker, _ACME)
        container.kinds.bind(_ACME, _AcmeResolver())
        container.seal()

        marker = _AcmeMarker()
        assert container.kinds.resolve(_ACME, marker, object) is marker


class TestSealRegisterDefault:
    def test_a_default_waiting_for_the_lock_keeps_a_binding_made_meanwhile(self) -> None:
        """The check and the binding are one locked step: the late default binds nothing."""
        container = LoomContainer()
        with STRUCTURE_LOCK:
            waiting = threading.Thread(
                target=container.register_default, args=(AcmeBoundLedger, AcmeLateLedger)
            )
            waiting.start()
            time.sleep(0.05)  # let the thread reach the lock
            container.register(AcmeBoundLedger, AcmeBoundLedger)
        waiting.join(timeout=5)

        assert type(container.resolve(AcmeBoundLedger)) is AcmeBoundLedger

    def test_after_seal_raises_even_when_the_key_is_bound(self, sealed: LoomContainer) -> None:
        with pytest.raises(ContainerSealed):
            sealed.register_default(AcmeBoundLedger, AcmeBoundLedger)
