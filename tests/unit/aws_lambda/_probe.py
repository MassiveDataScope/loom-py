"""Per-test job observations for Lambda adapter tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class Probe:
    seen: list[tuple[str, str | None, str | None]] = field(default_factory=list)
    loops: list[asyncio.AbstractEventLoop] = field(default_factory=list)
