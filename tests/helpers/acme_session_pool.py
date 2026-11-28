"""A pool an installer's resource opens and closes, recording each step in :data:`EVENTS`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import msgspec

EVENTS: list[str] = []
"""Every check, open and close, in the order they happened."""


class PoolSettings(msgspec.Struct, kw_only=True):
    """What a pool's section may ask for: a failure while opening or while closing."""

    fail_open: bool = False
    fail_close: bool = False


class SessionPool:
    """Records ``check:``, ``open:`` and ``close:`` with its name; fails as its settings say."""

    def __init__(self, name: str, settings: PoolSettings) -> None:
        self.name = name
        self.settings = settings

    @asynccontextmanager
    async def lifespan(self) -> AsyncIterator[None]:
        EVENTS.append(f"open:{self.name}")
        if self.settings.fail_open:
            raise RuntimeError(f"open {self.name}")
        try:
            yield
        finally:
            EVENTS.append(f"close:{self.name}")
            if self.settings.fail_close:
                raise RuntimeError(f"close {self.name}")

    async def check(self) -> None:
        EVENTS.append(f"check:{self.name}")

    async def is_ready(self) -> bool:
        return True
