"""The ``observability.log`` settings."""

from __future__ import annotations

import logging
from enum import StrEnum

import msgspec

from loom.core.environment import Environment, is_inside_lambda
from loom.core.errors import InvalidValue
from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import Secret


class LogFormatName(StrEnum):
    """The formats loom writes its records in."""

    LAMBDA = "lambda"
    JSON = "json"
    TEXT = "text"


def is_log_level(name: str) -> bool:
    """Whether *name* is a level of the standard library, in any case."""
    return name.upper() in logging.getLevelNamesMapping()


class LogSettings(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """How the process writes its log records.

    Attributes:
        format: ``lambda``, ``json`` or ``text``. ``None`` picks ``lambda`` inside
            AWS Lambda, ``json`` in a deployed environment and ``text`` anywhere else.
        level: Minimum level of the root logger, in Lambda too.
        named_levels: Minimum level of the named loggers, to quiet a noisy
            library: ``{"botocore": "WARNING"}``.
        subject_key: The secret loom's security records key the caller's
            ``user.hash`` with (HMAC-SHA256); ``None`` writes no caller at all.
    """

    format: LogFormatName | None = None
    level: str = "INFO"
    named_levels: dict[str, str] = msgspec.field(default_factory=dict)
    subject_key: Secret[str] | None = None

    def __post_init__(self) -> None:
        """Refuse a level the standard library does not know."""
        for name in (self.level, *self.named_levels.values()):
            if not is_log_level(name):
                raise InvalidValue("unsupported log level")

    def format_for(self, environment: Environment) -> LogFormatName:
        """The format name in effect for a process running in *environment*."""
        if self.format is not None:
            return self.format
        if is_inside_lambda():
            return LogFormatName.LAMBDA
        return LogFormatName.JSON if environment.is_deployed else LogFormatName.TEXT
