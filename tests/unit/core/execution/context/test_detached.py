"""A detached frame runs with no caller, whatever execution encloses it (D7)."""

from __future__ import annotations

import pytest

from loom.core.errors import Unauthenticated
from loom.core.execution import (
    ExecutionScope,
    current_execution,
    detached,
    execution_identity,
    leave,
)
from loom.core.identity import Identity

_ALICE = Identity(subject="acme-alice", mechanism="test")


def test_a_detached_frame_carries_no_identity_and_a_fresh_cache() -> None:
    with ExecutionScope(_ALICE):
        outer = current_execution()
        assert outer is not None
        outer.store("acme-client", "outer")
        token = detached()
        try:
            context = current_execution()
            assert context is not None
            assert context is not outer
            assert context.identity is None
            assert context.get("acme-client") is None
            with pytest.raises(Unauthenticated):
                execution_identity()
        finally:
            leave(token)
        assert execution_identity() is _ALICE


def test_a_detached_frame_outside_any_execution_is_still_an_execution() -> None:
    """A job callback with no enclosing execution reads ``Unauthenticated``, not ``None``."""
    token = detached()
    try:
        with pytest.raises(Unauthenticated):
            execution_identity()
    finally:
        leave(token)
    assert current_execution() is None
