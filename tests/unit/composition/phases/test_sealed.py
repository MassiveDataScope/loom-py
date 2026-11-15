"""A started application's container is sealed: a later ``register*`` raises and changes nothing.

Every ``register*`` on a sealed container, its message and the fact that it
leaves the bindings untouched are pinned in ``tests/unit/core/di/seal/test_boundary.py``.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.core.di import ContainerSealed
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.service import JobService
from tests.unit.composition.phases.components import AcmeRecord, AcmeUnregistered
from tests.unit.composition.phases.snapshot import application_singletons


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


class TestContainerBeforeSeal:
    @pytest.mark.parametrize("name", list(_REGISTERS), ids=list(_REGISTERS))
    def test_a_register_is_accepted(self, name: str) -> None:
        _REGISTERS[name](LoomContainer())


class TestSealedContainer:
    def test_reading_it_is_accepted(self) -> None:
        container = LoomContainer()
        container.register_instance(AcmeUnregistered, AcmeUnregistered())
        container.register_repo(AcmeRecord, AcmeUnregistered)
        container.seal()

        container.resolve(AcmeUnregistered)
        container.resolve_repo(AcmeRecord)
        container.validate()


class TestStartedApplicationContainer:
    def test_a_register_is_refused_and_no_singleton_changes(
        self, overridden: Callable[..., Application]
    ) -> None:
        application = overridden()
        container = application.kernel.container
        before = application_singletons(container)

        with pytest.raises(ContainerSealed, match="JobService"):
            container.register(JobService, AcmeOtherJobs, scope=Scope.APPLICATION)

        after = application_singletons(container)
        assert container.resolve(RuntimeExecutor) is application.kernel.executor
        assert not isinstance(after[JobService], AcmeOtherJobs)
        assert all(after[key] is instance for key, instance in before.items())

    def test_binding_an_agent_resolver_is_refused(
        self, overridden: Callable[..., Application]
    ) -> None:
        executor = overridden().kernel.executor
        resolver = _AcmeAgents()

        with pytest.raises(ContainerSealed, match="before the container is sealed"):
            executor.bind_agent_resolver(resolver)


class _AcmeAgents:
    def resolve(self, declaration: object, annotation: object) -> object:
        return declaration
