"""Nothing binds after the registration phase, and start-up builds against the kernel's own."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.composition import Application, pipeline
from loom.composition.pipeline.registration import Registered
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.registry import UseCaseRegistry
from tests.unit.composition.pipeline import paths


class AcmeKernelUser:
    """An ``APPLICATION`` service that takes the kernel's collaborators when it is built."""

    def __init__(
        self, executor: RuntimeExecutor, invoker: ApplicationInvoker, registry: UseCaseRegistry
    ) -> None:
        self.executor = executor
        self.invoker = invoker
        self.registry = registry


def _binds_kernel_user(container: LoomContainer) -> None:
    container.register(
        AcmeKernelUser,
        lambda: AcmeKernelUser(
            container.resolve(RuntimeExecutor),
            container.resolve(ApplicationInvoker),
            container.resolve(UseCaseRegistry),
        ),
        scope=Scope.APPLICATION,
    )


@pytest.mark.parametrize("name", list(paths.COMPOSED))
def test_nothing_binds_after_the_registration_phase(
    name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registered: list[Registered] = []
    bound: dict[BindingKey, object] = {}
    register = pipeline.register

    def snapshotting(*args: Any, **kwargs: Any) -> Registered:
        result = register(*args, **kwargs)
        registered.append(result)
        bound.update(result.kernel.container.bindings())
        return result

    monkeypatch.setattr(pipeline, "register", snapshotting)

    container = paths.COMPOSED[name](paths.Composition(), tmp_path, monkeypatch)

    (result,) = registered
    assert container is result.kernel.container
    after = container.bindings()
    assert after.keys() == bound.keys()
    assert all(after[key] is binding for key, binding in bound.items())


def test_an_application_service_built_at_start_up_gets_the_kernels_collaborators(
    overridden: Callable[..., Application],
) -> None:
    kernel = overridden(modules=(_binds_kernel_user,)).kernel

    user = kernel.container.resolve(AcmeKernelUser)
    assert user.executor is kernel.executor
    assert user.invoker is kernel.app
    assert user.registry is kernel.registry
