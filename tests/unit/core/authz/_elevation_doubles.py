from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field


async def _done() -> None:
    return None


async def _refused() -> None:
    raise RuntimeError("database says no")


@dataclass
class FakeSink:
    """Records what the frame asks of the database; ``open`` mimics an open transaction.

    It reaches no invocation scope; the frame's calls through a real sink and
    the scope's owner-task check are covered in
    ``tests/unit/core/repository/sqlalchemy/rls/test_elevation_sink_scope.py``.
    """

    open: bool = False
    set: list[str] = field(default_factory=list)
    cleared: list[frozenset[str]] = field(default_factory=list)
    fail_on_set: bool = False
    fail_on_clear: bool = False
    refuse_clear: bool = False
    """``clear_flags`` raises when called, as a sink refused by the scope checkpoint does."""
    fail_on_invalidate: bool = False
    invalidated: int = 0

    def in_transaction(self) -> bool:
        return self.open

    def set_flag(self, scope: str) -> Awaitable[object]:
        if self.fail_on_set:
            return _refused()
        self.set.append(scope)
        return _done()

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        if self.refuse_clear:
            raise RuntimeError("refused before any statement")
        if self.fail_on_clear:
            return _refused()
        self.cleared.append(scopes)
        return _done()

    def invalidate(self) -> Awaitable[object]:
        self.invalidated += 1
        return _refused() if self.fail_on_invalidate else _done()
