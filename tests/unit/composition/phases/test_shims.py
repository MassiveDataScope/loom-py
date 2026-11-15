"""``bootstrap_app`` is a deprecated shim; ``create_kernel`` is not, and builds one executor.

What both bind is recorded in ``snapshots/bindings``; here, the warning, the
executors they construct and that neither seals its container.
"""

from __future__ import annotations

import warnings

import pytest

from loom.composition import AppConfig
from loom.core.bootstrap import bootstrap_app, create_kernel
from loom.core.engine.executor import RuntimeExecutor
from tests.unit.composition.phases.components import AcmePing, AcmeRecord, AcmeUnregistered


class TestCompositionShims:
    def test_bootstrap_app_warns_its_caller_to_use_create_app(
        self,
        executors: list[RuntimeExecutor],
    ) -> None:
        with pytest.warns(DeprecationWarning, match=r"loom\.composition\.create_app") as caught:
            result = bootstrap_app(config=AppConfig(name="acme"), use_cases=[AcmePing])

        (warning,) = caught
        assert warning.filename == __file__
        assert executors == [result.container.resolve(RuntimeExecutor)]

    def test_create_kernel_builds_one_executor_without_a_deprecation_warning(
        self,
        executors: list[RuntimeExecutor],
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            kernel = create_kernel(config=AppConfig(name="acme"), use_cases=[])

        assert executors == [kernel.executor]
        assert kernel.container.resolve(RuntimeExecutor) is kernel.executor

    def test_neither_seals_its_container(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            bootstrapped = bootstrap_app(config=AppConfig(name="acme"), use_cases=[AcmePing])
        kernel = create_kernel(config=AppConfig(name="acme"), use_cases=[])

        for container in (bootstrapped.container, kernel.container):
            container.register(AcmeUnregistered, AcmeUnregistered)
            container.register_repo(AcmeRecord, AcmeUnregistered)
