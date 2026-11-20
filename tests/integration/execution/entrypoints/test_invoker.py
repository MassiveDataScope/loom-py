"""The application invoker builds the use case inside the execution it opens."""

from __future__ import annotations

from typing import cast

from loom.core.engine.executor import UseCaseExecutor
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry

from .doubles import ALICE, AcmeProbe


class TestAppInvoker:
    async def test_builds_inside_an_execution_carrying_its_identity(
        self, factory: UseCaseFactory, executor: UseCaseExecutor
    ) -> None:
        invoker = AppInvoker(factory, executor, cast(UseCaseRegistry, None), identity=ALICE)

        probe = await invoker.invoke(AcmeProbe)

        assert probe.session.caller is ALICE
        assert probe.session.execution is probe.ran_in
