"""A run whose consumer leaves early closes its execution where it opened it."""

from __future__ import annotations

import asyncio
import gc
from typing import Any

from loom.ai.abc import FinalEvent, TextDeltaEvent
from loom.core.execution import current_execution
from tests.helpers.ai import DEFAULT_OUTPUT, DEFAULT_USAGE

from .doubles import AGENT, ALICE, AcmeActingEngine, abandon_stream_after_first_frame

_SLOW_SECOND: dict[str, Any] = {
    "script": (
        TextDeltaEvent(text="first "),
        TextDeltaEvent(text="never "),
        FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE),
    ),
    "delays_ms": (0, 60_000, 0),
}


def _loop_errors() -> list[dict[str, Any]]:
    """Record what the event loop reports, a generator closed in a foreign context included."""
    errors: list[dict[str, Any]] = []
    asyncio.get_running_loop().set_exception_handler(lambda _loop, context: errors.append(context))
    return errors


class TestAConsumerLeavingEarly:
    async def test_closes_the_execution_without_a_token_error(self, make_runtime: Any) -> None:
        engine = AcmeActingEngine(**_SLOW_SECOND)
        errors = _loop_errors()

        async with make_runtime(engine) as host:
            async with host.run_stream(AGENT, "p", identity=ALICE) as events:
                async for _ in events:
                    break
            after = current_execution()
        gc.collect(1)
        await asyncio.sleep(0)

        assert after is None
        assert engine.acting == [ALICE]
        assert errors == []


class TestAnHttpClientDisconnecting:
    async def test_leaves_no_execution_and_no_token_error(
        self, make_runtime: Any, serving: Any
    ) -> None:
        engine = AcmeActingEngine(**_SLOW_SECOND)
        errors = _loop_errors()

        async with make_runtime(engine) as host, serving(host) as app:
            await abandon_stream_after_first_frame(app)
            after = current_execution()
        gc.collect(1)
        await asyncio.sleep(0)

        assert after is None
        assert engine.acting == [ALICE]
        assert errors == []
