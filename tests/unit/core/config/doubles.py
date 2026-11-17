"""Doubles shared by config tests."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct


class AcmeDbConfig(LoomFrozenStruct, frozen=True):
    host: str
    port: int = 5432
