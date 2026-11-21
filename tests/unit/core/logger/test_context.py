from __future__ import annotations

import asyncio

import pytest

from loom.core.logger import LogField, current_log_fields, log_context


class TestLogContext:
    def test_fields_are_present_inside_the_block_only(self) -> None:
        with log_context(invoice_id="inv-1"):
            inside = dict(current_log_fields())

        assert inside == {"invoice_id": "inv-1"}
        assert dict(current_log_fields()) == {}

    def test_an_inner_block_shadows_an_outer_key_and_the_outer_value_returns(self) -> None:
        with log_context(invoice_id="outer", tenant="t1"):
            with log_context(invoice_id="inner"):
                inner = dict(current_log_fields())
            after_inner = dict(current_log_fields())

        assert inner == {"invoice_id": "inner", "tenant": "t1"}
        assert after_inner == {"invoice_id": "outer", "tenant": "t1"}

    def test_the_fields_are_removed_when_the_block_raises(self) -> None:
        with pytest.raises(RuntimeError), log_context(invoice_id="inv-1"):
            raise RuntimeError("boom")

        assert dict(current_log_fields()) == {}

    def test_a_dotted_key_is_given_as_a_mapping(self) -> None:
        with log_context({LogField.JOB: "SendInvoice"}, attempt=2):
            inside = dict(current_log_fields())

        assert inside == {"loom.job": "SendInvoice", "attempt": 2}

    async def test_concurrent_tasks_do_not_see_each_other_fields(self) -> None:
        seen: dict[str, object] = {}

        async def work(name: str) -> None:
            with log_context(task=name):
                await asyncio.sleep(0)
                seen[name] = current_log_fields().get("task")

        await asyncio.gather(work("a"), work("b"))

        assert seen == {"a": "a", "b": "b"}
