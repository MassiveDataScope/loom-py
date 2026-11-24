"""The ``observability.log`` settings."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final

import msgspec

from loom.core.environment import Environment, is_inside_lambda
from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import Secret


class LogFormatName(StrEnum):
    """The formats loom writes its records in."""

    LAMBDA = "lambda"
    JSON = "json"
    TEXT = "text"


class LogLevel(StrEnum):
    """A level of the standard library, written as it names it."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


QUIET_LIBRARIES: Final[Mapping[str, LogLevel]] = MappingProxyType(
    {
        "asyncio": LogLevel.WARNING,
        "botocore": LogLevel.WARNING,
        "boto3": LogLevel.WARNING,
        "s3transfer": LogLevel.WARNING,
        "urllib3": LogLevel.WARNING,
        "httpx": LogLevel.WARNING,
        "httpcore": LogLevel.WARNING,
        "sqlalchemy.engine": LogLevel.WARNING,
    }
)
"""Minimum level of the libraries that write a record per request or query below WARNING."""


class LogSettings(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """How the process writes its log records.

    Attributes:
        format: ``lambda``, ``json`` or ``text``. ``None`` picks ``lambda`` inside
            AWS Lambda, ``json`` in a deployed environment and ``text`` anywhere else.
        level: Minimum level of the root logger, in Lambda too.
        named_levels: Minimum level of the named loggers, over :data:`QUIET_LIBRARIES`
            under the exact same name only: ``{"botocore": "INFO"}`` lets botocore's INFO
            records through again and leaves ``boto3`` at WARNING.
        subject_key: The secret loom's security records key the caller's
            ``user.hash`` with (HMAC-SHA256); ``None`` writes no caller at all.
    """

    format: LogFormatName | None = None
    level: LogLevel = LogLevel.INFO
    named_levels: dict[str, LogLevel] = msgspec.field(default_factory=dict)
    subject_key: Secret[str] | None = None

    def format_for(self, environment: Environment) -> LogFormatName:
        """The format name in effect for a process running in *environment*."""
        if self.format is not None:
            return self.format
        if is_inside_lambda():
            return LogFormatName.LAMBDA
        return LogFormatName.JSON if environment.is_deployed else LogFormatName.TEXT

    def logger_levels(self) -> dict[str, LogLevel]:
        """The minimum level of each named logger: the quiet libraries', then the written ones."""
        return {**QUIET_LIBRARIES, **self.named_levels}
