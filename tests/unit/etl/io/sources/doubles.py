"""Shared doubles for io source tests."""

from __future__ import annotations

import msgspec


class OptionalStatusOrderDoc(msgspec.Struct):
    id: str
    status: str | None = None
