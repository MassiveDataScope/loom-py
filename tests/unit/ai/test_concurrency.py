"""``shield_and_drain`` on its own: the one place every session — the
engine's own, ``SharedMcpSession``, and any third-party
:class:`~loom.ai.abc.ConcurrentMcpSession` — keeps the drain guarantee.

Extracted into its own module (``loom/ai/_concurrency.py``) without a test of
its own until now. The drained call's own outcome must never be reported as
"never retrieved" once the caller that awaited it has moved on: a
black-box test through ``shield_and_drain`` alone -- a call that raises,
drained after its caller is cancelled -- pins that at the loop boundary,
without reaching into the private helper that keeps the promise.
"""

from __future__ import annotations

import asyncio
import gc

import pytest

from loom.ai._concurrency import shield_and_drain
from tests.helpers.gates import CallGate, await_cancelled


async def _finish_when_released(gate: CallGate) -> str:
    """Return once ``gate`` releases the call."""
    await gate.enter()
    return "finished"


class TestShieldAndDrainCompletesSuccessfully:
    async def test_returns_the_call_result(self) -> None:
        async def _call() -> str:
            return "answer"

        assert await shield_and_drain(_call(), label="crm") == "answer"


class TestShieldAndDrainDrainsOnCancel:
    async def test_reraises_cancelled_error_to_the_caller_after_draining(self) -> None:
        """Cancellation reaches the caller only after the shielded call has finished."""
        gate = CallGate()
        task = asyncio.create_task(shield_and_drain(_finish_when_released(gate), label="crm"))
        await gate.wait_all_entered(1)

        task.cancel()
        await gate.let_pending_calls_enter()

        assert not task.done()
        gate.release()
        await await_cancelled(task)

    async def test_a_second_cancellation_abandons_a_hung_call(self) -> None:
        """An external call may hang: a second cancellation must escape the
        drain while the call is still running, so the caller can give up."""
        hung = asyncio.Event()

        async def _hangs() -> None:
            await hung.wait()

        task = asyncio.create_task(shield_and_drain(_hangs(), label="crm"))
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        try:
            await asyncio.wait([task], timeout=1)
            escaped_while_hung = task.done()
        finally:
            hung.set()
            await asyncio.sleep(0)

        assert escaped_while_hung
        assert task.cancelled()

    async def test_the_label_reaches_the_drain_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """``label`` is not decoration: it is what names the session in the
        one log line a drained cancellation produces, which is the only
        place H5 asks it to matter."""
        caplog.set_level("DEBUG", logger="loom.ai._concurrency")
        gate = CallGate()
        task = asyncio.create_task(
            shield_and_drain(_finish_when_released(gate), label="inventory-mcp")
        )
        await gate.wait_all_entered(1)

        task.cancel()
        await gate.let_pending_calls_enter()
        assert not task.done()
        gate.release()
        await await_cancelled(task)

        assert "inventory-mcp" in caplog.text


class TestTheDrainedResultIsNotLeftUnretrieved:
    """O4: mutar ``shield_and_drain`` quitando el descarte del resultado
    drenado deja toda la suite en verde salvo este test -- el unico que
    atraviesa el punto de llamada real en vez de probar el helper privado en
    aislamiento. Una llamada que lanza, drenada tras cancelar a su llamante,
    no debe reportarse nunca como "exception was never retrieved" en el
    manejador de excepciones del bucle, que es donde el recolector de basura
    la reportaria si nadie la hubiese consumido."""

    async def test_a_raising_call_is_not_reported_as_unretrieved(self) -> None:
        async def _raising(gate: CallGate) -> None:
            """Raise once ``gate`` releases a call that is already in flight."""
            await gate.enter()
            raise ValueError("boom")

        reported: list[str] = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(
            lambda _loop, context: reported.append(str(context.get("message", "")))
        )
        gate = CallGate()
        try:
            task = asyncio.create_task(shield_and_drain(_raising(gate), label="crm"))
            await gate.wait_all_entered(1)
            task.cancel()
            await gate.let_pending_calls_enter()
            assert not task.done()
            gate.release()
            await await_cancelled(task)
            # ``pytest.raises`` would keep the caught exception's traceback --
            # and, through it, the drained future's own frame -- alive past
            # this point, hiding the leak a plain ``except`` lets the garbage
            # collector actually reclaim.
            del task
            for _ in range(3):
                gc.collect()
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

        assert not any("was never retrieved" in message for message in reported)
