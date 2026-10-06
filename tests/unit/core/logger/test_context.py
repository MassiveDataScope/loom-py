from __future__ import annotations

import asyncio
import io
import logging
from collections.abc import Iterator

import pytest
import structlog

from loom.core.logger import get_logger, log_context

_REQUEST_ID = "req-7f3a"


def _loom_formatter() -> logging.Formatter:
    """Return the formatter ``configure_logging`` installed on the root logger."""
    return next(
        handler.formatter
        for handler in logging.getLogger().handlers
        if isinstance(handler.formatter, structlog.stdlib.ProcessorFormatter)
    )


@pytest.fixture(autouse=True)
def _empty_log_context() -> Iterator[None]:
    structlog.contextvars.clear_contextvars()
    yield
    structlog.contextvars.clear_contextvars()


@pytest.fixture
def output() -> Iterator[io.StringIO]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(_loom_formatter())
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield stream
    finally:
        root.removeHandler(handler)


def _fail_inside_context() -> None:
    with log_context(request_id=_REQUEST_ID):
        raise RuntimeError("boom")


def test_value_reaches_loom_logger_records(output: io.StringIO) -> None:
    with log_context(request_id=_REQUEST_ID):
        get_logger("tests.log_context").info("inside")

    assert _REQUEST_ID in output.getvalue()


def test_value_reaches_stdlib_records(output: io.StringIO) -> None:
    with log_context(request_id=_REQUEST_ID):
        logging.getLogger("tests.log_context.stdlib").warning("inside")

    assert _REQUEST_ID in output.getvalue()


def test_value_is_gone_after_exit(output: io.StringIO) -> None:
    with log_context(request_id=_REQUEST_ID):
        pass
    get_logger("tests.log_context").info("after")

    assert "after" in output.getvalue()
    assert _REQUEST_ID not in output.getvalue()


def test_value_is_gone_after_an_exception(output: io.StringIO) -> None:
    with pytest.raises(RuntimeError):
        _fail_inside_context()
    get_logger("tests.log_context").info("after")

    assert "after" in output.getvalue()
    assert _REQUEST_ID not in output.getvalue()


def test_nested_context_restores_the_outer_value(output: io.StringIO) -> None:
    with log_context(request_id="outer-req"):
        with log_context(request_id="inner-req"):
            pass
        get_logger("tests.log_context").info("outer again")

    assert "outer-req" in output.getvalue()
    assert "inner-req" not in output.getvalue()


async def test_concurrent_tasks_do_not_see_each_other(output: io.StringIO) -> None:
    both_bound = asyncio.Barrier(2)

    async def handle(request_id: str) -> None:
        with log_context(request_id=request_id):
            await both_bound.wait()
            get_logger("tests.log_context").info(f"handled-{request_id}")

    await asyncio.gather(handle("req-a"), handle("req-b"))

    lines = output.getvalue().splitlines()
    line_a = next(line for line in lines if "handled-req-a" in line)
    line_b = next(line for line in lines if "handled-req-b" in line)
    assert line_a.count("req-a") == 2
    assert "req-b" not in line_a
    assert line_b.count("req-b") == 2
    assert "req-a" not in line_b
