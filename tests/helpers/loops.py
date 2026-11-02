"""Record the event loops code under test creates and makes current."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest


@dataclass
class RecordedLoops:
    """The loops ``asyncio.new_event_loop`` created and what ``asyncio.set_event_loop`` was given.

    Attributes:
        created: Every loop created, in order.
        made_current: Every argument to ``set_event_loop``, ``None`` included.
    """

    created: list[asyncio.AbstractEventLoop] = field(default_factory=list)
    made_current: list[asyncio.AbstractEventLoop | None] = field(default_factory=list)


@pytest.fixture
def event_loops(monkeypatch: pytest.MonkeyPatch) -> RecordedLoops:
    """Record every loop created and made current while the test runs."""
    recorded = RecordedLoops()
    new_event_loop = asyncio.new_event_loop
    set_event_loop = asyncio.set_event_loop

    def recording_new_event_loop() -> asyncio.AbstractEventLoop:
        loop = new_event_loop()
        recorded.created.append(loop)
        return loop

    def recording_set_event_loop(loop: asyncio.AbstractEventLoop | None) -> None:
        recorded.made_current.append(loop)
        set_event_loop(loop)

    monkeypatch.setattr(asyncio, "new_event_loop", recording_new_event_loop)
    monkeypatch.setattr(asyncio, "set_event_loop", recording_set_event_loop)
    return recorded
