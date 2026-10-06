"""JSON nested deeper than msgspec can decode, on threads with a fixed stack.

The depth at which msgspec raises ``RecursionError`` depends on the Python
version: a fixed count of about 10 000 levels up to 3.13 (1 000 on 3.11), the C
stack left to the decoding thread from 3.14 (about 12 000 levels on a 4 MiB
stack). Requests sent inside :func:`fixed_thread_stack` are served on threads
with a fixed 4 MiB stack: deep enough that the count-based versions reach their
limit without overflowing it, small enough that :func:`nested_array` is far
deeper than 3.14 can decode on it. :func:`assert_too_deep_for_msgspec` checks
that premise, so a test built on it fails loudly instead of passing vacuously.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager

import msgspec

TOO_DEEP = 100_000
_THREAD_STACK_BYTES = 4 * 1024 * 1024


def nested_array(depth: int = TOO_DEEP) -> bytes:
    """Return a well-formed JSON array nested *depth* levels deep."""
    return b"[" * depth + b"]" * depth


@contextmanager
def fixed_thread_stack() -> Iterator[None]:
    """Start every thread created inside the block with the same small stack."""
    previous = threading.stack_size(_THREAD_STACK_BYTES)
    try:
        yield
    finally:
        threading.stack_size(previous)


def assert_too_deep_for_msgspec(body: bytes) -> None:
    """Fail unless msgspec raises ``RecursionError`` decoding *body* on such a thread."""
    outcome: list[BaseException | None] = []

    def decode() -> None:
        try:
            msgspec.json.decode(body)
        except RecursionError as exc:
            outcome.append(exc)
        else:
            outcome.append(None)

    with fixed_thread_stack():
        thread = threading.Thread(target=decode)
        thread.start()
        thread.join()
    assert isinstance(outcome[0], RecursionError), "body is not too deep for msgspec"
