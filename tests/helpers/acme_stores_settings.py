"""The section a test package's installer decodes, with a log of each decode, and what it binds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import msgspec


class AcmeStoresSettings(msgspec.Struct, kw_only=True):
    """The ``acme.stores`` section; every decode is recorded in :attr:`decoded`."""

    bucket: str
    backend: str = "s3"
    decoded: ClassVar[list[str]] = []

    def __post_init__(self) -> None:
        self.decoded.append(self.bucket)


@dataclass(frozen=True)
class AcmeBucket:
    """What the configured installer binds from its settings."""

    name: str
    backend: str
