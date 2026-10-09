"""Output types the batch-runner artefacts refer to by ``type_ref``."""

from __future__ import annotations

import msgspec


class Reply(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    answer: str


class Other(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    label: str
