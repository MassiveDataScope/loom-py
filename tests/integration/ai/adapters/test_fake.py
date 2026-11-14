"""H4: ``register_instance(Port, Fake())`` opens no MCP session and builds no agent.

An instance of the real adapter is an instance too: what its class declares is not walked.
"""

from __future__ import annotations

import pytest

from loom.core.identity import Identity
from tests.helpers.ai import CountingEngineProvider
from tests.integration.ai.adapters.conftest import Compose
from tests.integration.ai.adapters.doubles import (
    AGENT,
    SERVER,
    AcmeAnalystAdvice,
    AcmeFakeAdvice,
    AcmeFakeQuotes,
    AcmeGatewayQuotes,
    AcmeMarket,
)
from tests.integration.ai.conftest import CountingMcpClientFactory


class TestFakePorts:
    @pytest.mark.parametrize(
        ("quotes", "advice", "opened", "built"),
        [
            (AcmeGatewayQuotes, AcmeAnalystAdvice, [SERVER], [AGENT]),
            (AcmeFakeQuotes(), AcmeFakeAdvice(), [], []),
            (AcmeGatewayQuotes(gateway=object()), AcmeAnalystAdvice(analyst=object()), [], []),
        ],
        ids=["adapters", "fakes", "adapter-instances"],
    )
    async def test_open_and_build_only_what_the_adapters_declare(
        self,
        compose: Compose,
        sessions: CountingMcpClientFactory,
        engines: CountingEngineProvider,
        quotes: object,
        advice: object,
        opened: list[str],
        built: list[str],
    ) -> None:
        runtime = compose(quotes=quotes, advice=advice).ai.runtime
        assert runtime is not None

        async with runtime:
            pass

        assert (sessions.calls, engines.calls) == (opened, built)

    async def test_the_use_case_runs_on_the_fake(
        self, compose: Compose, identity: Identity
    ) -> None:
        application = compose(quotes=AcmeFakeQuotes(), advice=AcmeFakeAdvice())
        runtime = application.ai.runtime
        assert runtime is not None

        async with runtime:
            result = await application.kernel.executor.run(AcmeMarket, identity=identity)

        assert result == "fake"
