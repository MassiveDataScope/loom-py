"""H3: an agent reached only through an adapter is listed in health.

A server reached only through an adapter is opened like one reached only
through ``execute()`` (``test_closure.py``); neither is an agent health check
(``test_use_case_mcp_health_gap.py``).
"""

from __future__ import annotations

from tests.integration.ai.adapters.conftest import Compose
from tests.integration.ai.adapters.doubles import AGENT, AcmeFakeQuotes
from tests.integration.ai.conftest import _until


class TestAdapterOnlyAgentHealth:
    async def test_is_listed_in_health(self, compose: Compose) -> None:
        host = compose(quotes=AcmeFakeQuotes()).ai.agent_host
        assert host is not None

        async with host:
            await _until(lambda: host._health.get(AGENT) is not None)  # noqa: SLF001
            health = await host.health(AGENT)

        assert health.status == "ok"
