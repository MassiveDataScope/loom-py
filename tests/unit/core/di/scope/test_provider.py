"""Zero-argument classes and providers are built as today: called with no argument."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope
from loom.core.identity import ANONYMOUS


class AcmePort:
    pass


class AcmeDefaults(AcmePort):
    def __init__(self, region: str = "eu", retries: int = 3) -> None:
        self.region, self.retries = region, retries


class AcmeVariadic(AcmePort):
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.args, self.kwargs = args, kwargs


@pytest.mark.parametrize("impl", [AcmeDefaults, AcmeVariadic])
def test_a_class_needing_no_argument_is_built_with_none(impl: type[AcmePort]) -> None:
    container = LoomContainer()
    container.register(AcmePort, impl)

    built = container.resolve(AcmePort)

    assert type(built) is impl
    assert vars(built) in ({"region": "eu", "retries": 3}, {"args": (), "kwargs": {}})


@pytest.mark.parametrize("scope", list(Scope))
def test_a_provider_is_called_with_no_argument(scope: Scope) -> None:
    calls: list[int] = []

    def provide() -> AcmePort:
        calls.append(1)
        return AcmePort()

    container = LoomContainer()
    container.register(AcmePort, provide, scope=scope)

    with ExecutionScope(ANONYMOUS):
        container.resolve(AcmePort)
        container.resolve(AcmePort)

    assert calls == [1]


def test_a_registered_instance_is_served_as_is_inside_and_outside_an_execution() -> None:
    port = AcmePort()
    container = LoomContainer()
    container.register_instance(AcmePort, port)

    with ExecutionScope(ANONYMOUS):
        inside = container.resolve(AcmePort)

    assert inside is port is container.resolve(AcmePort)
