"""``create_kernel`` called on its own builds one executor and leaves its container open.

What it binds is recorded in ``snapshots/bindings``.
"""

from __future__ import annotations

from loom.composition import AppConfig
from loom.core.bootstrap import create_kernel
from loom.core.engine.executor import UseCaseExecutor
from tests.unit.composition.phases.components import AcmeRecord, AcmeUnregistered


class TestStandaloneKernel:
    def test_builds_one_executor(self, executors: list[UseCaseExecutor]) -> None:
        kernel = create_kernel(config=AppConfig(name="acme"), use_cases=[])

        assert executors == [kernel.executor]
        assert kernel.container.resolve(UseCaseExecutor) is kernel.executor

    def test_does_not_seal_its_container(self) -> None:
        kernel = create_kernel(config=AppConfig(name="acme"), use_cases=[])

        kernel.container.register(AcmeUnregistered, AcmeUnregistered)
        kernel.container.register_repo(AcmeRecord, AcmeUnregistered)
