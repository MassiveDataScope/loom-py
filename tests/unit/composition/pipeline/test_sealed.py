"""A started application's container is sealed: a later ``register*`` warns and changes one key.

The registration still replaces that key's binding and drops only that key's
built instance, so no singleton splits: whatever was built at start-up keeps
being served. Reading a sealed container never warns.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.core.di import ContainerSealedWarning
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.service import JobService
from tests.unit.composition.pipeline.components import AcmeRecord, AcmeUnregistered
from tests.unit.composition.pipeline.snapshot import application_singletons


class AcmeOtherJobs:
    """A job service bound after start-up."""


_REGISTERS: dict[str, Callable[[LoomContainer], None]] = {
    "register": lambda container: container.register(AcmeUnregistered, AcmeUnregistered),
    "register_default": lambda container: container.register_default(
        AcmeUnregistered, AcmeUnregistered
    ),
    "register_instance": lambda container: container.register_instance(
        AcmeUnregistered, AcmeUnregistered()
    ),
    "register_repo": lambda container: container.register_repo(AcmeUnregistered, AcmeRecord),
}


@pytest.mark.parametrize("name", list(_REGISTERS))
def test_each_register_after_seal_warns_naming_the_key_at_its_caller(name: str) -> None:
    container = LoomContainer()
    container.seal()

    with pytest.warns(ContainerSealedWarning, match="AcmeUnregistered registered into") as caught:
        _REGISTERS[name](container)

    assert [warning.filename for warning in caught] == [__file__]


@pytest.mark.parametrize("name", list(_REGISTERS))
def test_a_register_before_seal_does_not_warn(name: str) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", ContainerSealedWarning)
        _REGISTERS[name](LoomContainer())


def test_reading_a_sealed_container_does_not_warn() -> None:
    container = LoomContainer()
    container.register_instance(AcmeUnregistered, AcmeUnregistered())
    container.register_repo(AcmeRecord, AcmeUnregistered)
    container.seal()

    with warnings.catch_warnings():
        warnings.simplefilter("error", ContainerSealedWarning)
        container.resolve(AcmeUnregistered)
        container.resolve_repo(AcmeRecord)
        container.validate()


def test_a_register_after_seal_drops_only_its_own_instance() -> None:
    container = LoomContainer()
    container.register(AcmeUnregistered, AcmeUnregistered, scope=Scope.APPLICATION)
    container.register(JobService, AcmeOtherJobs, scope=Scope.APPLICATION)
    service, jobs = container.resolve(AcmeUnregistered), container.resolve(JobService)
    container.seal()

    with pytest.warns(ContainerSealedWarning):
        container.register(AcmeUnregistered, AcmeUnregistered, scope=Scope.APPLICATION)

    assert container.resolve(AcmeUnregistered) is not service
    assert container.resolve(JobService) is jobs


def test_no_singleton_splits_when_the_application_container_is_registered_into(
    overridden: Callable[..., Application],
) -> None:
    application = overridden()
    container = application.kernel.container
    before = application_singletons(container)

    with pytest.warns(ContainerSealedWarning, match="JobService"):
        container.register(JobService, AcmeOtherJobs, scope=Scope.APPLICATION)

    after = application_singletons(container)
    assert container.resolve(RuntimeExecutor) is application.kernel.executor
    assert isinstance(after.pop(JobService), AcmeOtherJobs)
    assert all(after[key] is instance for key, instance in before.items() if key is not JobService)
