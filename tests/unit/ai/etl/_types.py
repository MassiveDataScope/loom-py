"""Output types the batch-runner artefacts refer to by ``type_ref``."""

from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

import msgspec
import pydantic


class Reply(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    answer: str


class Other(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    label: str


class Mood(enum.Enum):
    CALM = "calm"
    ANGRY = "angry"


class Priority(enum.IntEnum):
    LOW = 1
    HIGH = 2


class Offer(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    amount: Decimal
    currency: str


class Rich(msgspec.Struct, frozen=True, forbid_unknown_fields=True, rename={"seen_at": "seenAt"}):
    label: Literal["acepta", "rechaza"]
    mood: Mood
    priority: Priority
    day: date
    seen_at: datetime
    amount: Decimal
    score: float
    flag: bool
    tags: list[str]
    offer: Offer | None
    extra: dict[str, int]
    count: int | None = None


class PydanticReply(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    answer: str
    day: date
    count: int | None = pydantic.Field(default=None, alias="howMany")


class Clashing(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    answer: str
    agent_status: str
