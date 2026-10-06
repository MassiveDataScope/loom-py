"""Per-context log fields, without depending on the logging backend."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from structlog.contextvars import bind_contextvars, reset_contextvars


@contextmanager
def log_context(**values: Any) -> Iterator[None]:
    """Add *values* to every log record emitted inside the block.

    The values live in a context variable, so they follow the current thread
    or asyncio task and never reach a concurrent request.  On exit, normal or
    by exception, each key regains the value it had before the block.

    Args:
        **values: Fields to attach, e.g. ``request_id="..."``.

    Yields:
        ``None``, with *values* bound.

    Example::

        with log_context(request_id=request_id):
            logger.info("audit.recorded")  # carries request_id
    """
    tokens = bind_contextvars(**values)
    try:
        yield
    finally:
        reset_contextvars(**tokens)
