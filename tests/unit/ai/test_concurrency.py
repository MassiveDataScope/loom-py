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


async def _delayed(delay_s: float = 0.05) -> str:
    """A call bounded to complete on its own -- never cancelled from inside."""
    await asyncio.sleep(delay_s)
    return "finished"


class TestShieldAndDrainCompletaConExito:
    async def test_devuelve_el_resultado_de_la_llamada(self) -> None:
        async def _call() -> str:
            return "answer"

        assert await shield_and_drain(_call(), label="crm") == "answer"


class TestShieldAndDrainDrenaAlCancelar:
    async def test_relanza_cancelled_error_al_llamante_tras_drenar(self) -> None:
        task = asyncio.create_task(shield_and_drain(_delayed(), label="crm"))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_una_segunda_cancelacion_abandona_una_llamada_colgada(self) -> None:
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

    async def test_el_label_llega_al_log_del_drenado(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``label`` is not decoration: it is what names the session in the
        one log line a drained cancellation produces, which is the only
        place H5 asks it to matter."""
        caplog.set_level("DEBUG", logger="loom.ai._concurrency")
        task = asyncio.create_task(shield_and_drain(_delayed(), label="inventory-mcp"))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert any("inventory-mcp" in record.message for record in caplog.records)


class TestElResultadoDrenadoNoQuedaSinRecuperar:
    """O4: mutar ``shield_and_drain`` quitando el descarte del resultado
    drenado deja toda la suite en verde salvo este test -- el unico que
    atraviesa el punto de llamada real en vez de probar el helper privado en
    aislamiento. Una llamada que lanza, drenada tras cancelar a su llamante,
    no debe reportarse nunca como "exception was never retrieved" en el
    manejador de excepciones del bucle, que es donde el recolector de basura
    la reportaria si nadie la hubiese consumido."""

    async def test_una_llamada_que_lanza_no_se_reporta_como_no_recuperada(self) -> None:
        async def _raising() -> None:
            await asyncio.sleep(0.02)
            raise ValueError("boom")

        reported: list[str] = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(
            lambda _loop, context: reported.append(str(context.get("message", "")))
        )
        try:
            task = asyncio.create_task(shield_and_drain(_raising(), label="crm"))
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("expected the cancelled call to re-raise")
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
