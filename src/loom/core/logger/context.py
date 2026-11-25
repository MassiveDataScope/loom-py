"""Fields that every record emitted inside a block carries."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType

_NO_FIELDS: Mapping[str, object] = MappingProxyType({})
_fields: ContextVar[Mapping[str, object]] = ContextVar("loom_log_fields", default=_NO_FIELDS)


def current_log_fields() -> Mapping[str, object]:
    """The fields of the enclosing :func:`log_context` blocks, innermost wins."""
    return _fields.get()


@contextmanager
def log_context(fields: Mapping[str, object] = _NO_FIELDS, /, **named: object) -> Iterator[None]:
    """Add *fields* to every log record emitted inside the block.

    The records of the application and of its libraries carry them once
    :func:`~loom.core.logger.install.configure_logging` has installed loom's
    formats, which derive them when they write a record. The fields live in a context
    variable, so they follow the current thread or asyncio task and never reach a
    concurrent request. Blocks nest: an inner key shadows an outer one and the outer
    value returns on exit, normal or by exception. Enter and exit a block in the same context. As a
    decorator it binds only while the call itself runs, which for an ``async
    def`` is the creation of the coroutine: use a ``with`` block inside it.

    A dotted key is not a keyword: pass the fields as a mapping,
    ``log_context({LogField.JOB: name})``.

    Args:
        fields: Fields to attach under their key, ``LogField`` members included.
        **named: Fields to attach under their keyword, e.g. ``invoice_id="..."``.

    Yields:
        ``None``, with *fields* bound.

    Example::

        with log_context(invoice_id=invoice_id):
            log.info(InvoiceEvent.ISSUED)
    """
    token = _fields.set({**_fields.get(), **fields, **named})
    try:
        yield
    finally:
        _fields.reset(token)
