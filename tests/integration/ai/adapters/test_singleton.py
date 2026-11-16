"""A singleton adapter holding ``Mcp()`` and ``Agent()`` is built at seal, serving every caller."""

from __future__ import annotations

from loom.composition import Project
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.identity import Identity
from loom.serverless.aws.lambda_ import LambdaAdapter
from tests.helpers.ai import CountingEngineProvider
from tests.integration.ai.adapters.doubles import (
    AcmeAdvice,
    AcmeAnalystAdvice,
    AcmeGatewayQuotes,
    AcmeMarket,
    AcmeQuotes,
)
from tests.integration.ai.conftest import RecordingMcpSession

_ALICE = Identity(subject="acme-alice", mechanism="test")
_BOB = Identity(subject="acme-bob", mechanism="test")


def _singletons(container: LoomContainer) -> None:
    container.register(AcmeQuotes, AcmeGatewayQuotes, scope=Scope.APPLICATION)
    container.register(AcmeAdvice, AcmeAnalystAdvice, scope=Scope.APPLICATION)


class TestASingletonAdapterHoldingHandles:
    async def test_is_built_at_seal_and_serves_each_execution(
        self,
        project: Project,
        engines: CountingEngineProvider,
        session: RecordingMcpSession,
    ) -> None:
        del engines
        application = project.compose(
            LambdaAdapter(use_cases=(AcmeMarket,)), modules=(_singletons,)
        )
        container, runtime = application.kernel.container, application.ai.runtime
        quotes = container.resolve(AcmeQuotes)
        assert runtime is not None

        async with runtime:
            for caller in (_ALICE, _BOB):
                await application.kernel.executor.run(AcmeMarket, identity=caller)

        assert container.resolve(AcmeQuotes) is quotes
        assert [name for name, _ in session.calls] == ["quote", "quote"]
