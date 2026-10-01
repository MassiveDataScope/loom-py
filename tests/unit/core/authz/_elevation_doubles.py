from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FakeSink:
    """Records what the frame asks of the database; ``open`` mimics an open transaction."""

    open: bool = False
    set: list[str] = field(default_factory=list)
    cleared: list[frozenset[str]] = field(default_factory=list)
    fail_on_set: bool = False
    fail_on_clear: bool = False
    invalidated: int = 0

    def in_transaction(self) -> bool:
        return self.open

    async def set_flag(self, scope: str) -> None:
        if self.fail_on_set:
            raise RuntimeError("database says no")
        self.set.append(scope)

    async def clear_flags(self, scopes: frozenset[str]) -> None:
        if self.fail_on_clear:
            raise RuntimeError("database says no")
        self.cleared.append(scopes)

    async def invalidate(self) -> None:
        self.invalidated += 1
