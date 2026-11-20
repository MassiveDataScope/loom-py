"""C-B2: an unreachable singleton analysis would refuse, or one declaring a kind, is not built."""

from __future__ import annotations

import pytest

from loom.core.di import ContainerSealed
from loom.core.di.container import LoomContainer
from loom.core.di.errors import DependencyWarning
from loom.core.di.graph import DependencyGraphError
from loom.core.di.scope import Scope
from loom.core.identity import ANONYMOUS, Identity
from tests.unit.core.di.doubles import AcmeSession, AcmeTakesInput
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.acme import AcmeCall, AcmeCaller


class AcmeCaptive:
    def __init__(self, session: AcmeSession) -> None:
        self.session = session


class AcmeCallerCache:
    def __init__(self, caller: object = AcmeCaller()) -> None:
        self.caller = caller


class AcmeTarget:
    pass


class AcmeCalls:
    def __init__(self, target: AcmeCall[AcmeTarget]) -> None:
        self.target = target


class AcmeLoopFirst:
    def __init__(self, second: AcmeLoopSecond) -> None:
        self.second = second


class AcmeLoopSecond:
    def __init__(self, first: AcmeLoopFirst) -> None:
        self.first = first


class AcmeRoot:
    """Reaches nothing."""


class AcmeResolver:
    def resolve(self, declaration: object, annotation: object) -> Identity:
        return ANONYMOUS


def _bound(*keys: type) -> LoomContainer:
    container = acme.container()
    container.kinds.register(acme.CALLER, AcmeResolver())
    container.kinds.register(acme.CALL, AcmeResolver())
    container.register(AcmeSession, AcmeSession)
    if AcmeLoopFirst in keys:
        container.register(AcmeLoopSecond, AcmeLoopSecond, scope=Scope.APPLICATION)
    for key in keys:
        container.register(key, key, scope=Scope.APPLICATION)
    return container


class TestSealSkipsUnreachableSingletons:
    @pytest.mark.parametrize(
        ("key", "reason"),
        [
            (
                AcmeCaptive,
                "AcmeCaptive -> session: AcmeSession: an APPLICATION binding cannot hold",
            ),
            (AcmeCallerCache, "AcmeCallerCache: caller: an APPLICATION binding cannot hold"),
            (AcmeTakesInput, "AcmeTakesInput: payload: AcmeInput() belongs to execute()"),
            (AcmeLoopFirst, "AcmeLoopFirst -> second: AcmeLoopSecond -> first: AcmeLoopFirst: dep"),
            (AcmeCalls, "AcmeCalls: target: declares a 'acme-call' dependency no root reaches"),
        ],
    )
    def test_an_unreachable_refusable_singleton_is_skipped_and_fails_at_first_resolve(
        self, key: type, reason: str
    ) -> None:
        container = _bound(key)

        with pytest.warns(DependencyWarning) as caught:
            container.seal(acme.analysed(container, AcmeRoot))
        with pytest.raises(DependencyGraphError) as failed:
            container.resolve(key)

        assert any(
            str(warning.message).startswith(f"{key.__qualname__} is not built")
            for warning in caught
        )
        assert any(reason in str(warning.message) for warning in caught)
        assert reason in str(failed.value)

    def test_a_skipped_singleton_cannot_be_registered_again_after_seal(self) -> None:
        container = _bound(AcmeCaptive)
        with pytest.warns(DependencyWarning):
            container.seal(acme.analysed(container, AcmeRoot))

        with pytest.raises(ContainerSealed, match="AcmeCaptive"):
            container.register(AcmeCaptive, AcmeRoot, scope=Scope.APPLICATION)

        with pytest.raises(DependencyGraphError):
            container.resolve(AcmeCaptive)
