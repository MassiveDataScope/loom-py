"""Doubles shared by config tests."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct


class AcmeDbConfig(LoomFrozenStruct, frozen=True):
    host: str
    port: int = 5432


class BotocoreClientError(Exception):
    """Stand-in for ``botocore.exceptions.ClientError`` (not a test dep)."""

    def __init__(self, message: str, response: dict[str, object]) -> None:
        super().__init__(message)
        self.response = response
