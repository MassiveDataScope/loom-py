"""H3: a server or agent reached only through an adapter is opened, verified and compiled.

The Lambda function ``worker`` hosts agents without their endpoints, so it
compiles only the agents a declaration names: ``auditor``, which nothing
names, is never compiled. Removing the binding is ``test_fake.py``.
"""

from __future__ import annotations

from tests.integration.ai.adapters.conftest import Compose
from tests.integration.ai.adapters.doubles import AGENT, SERVER, AcmeFakeAdvice, AcmeFakeQuotes
from tests.integration.ai.conftest import CountingMcpClientFactory, RecordingMcpSession


class TestLambdaClosure:
    def test_compiles_the_agent_only_an_adapter_names(self, compose: Compose) -> None:
        application = compose(quotes=AcmeFakeQuotes())

        assert tuple(plan.name for plan in application.ai.plans) == (AGENT,)


class TestAdapterOnlyServer:
    async def test_is_opened_and_listed_at_startup(
        self,
        compose: Compose,
        sessions: CountingMcpClientFactory,
        session: RecordingMcpSession,
    ) -> None:
        host = compose(advice=AcmeFakeAdvice()).ai.agent_host
        assert host is not None

        async with host:
            granted = host.use_case_mcp_grant(SERVER)

        assert sessions.calls == [SERVER]
        assert session.listed == 1
        assert granted is not None
