"""The ``observability.log`` settings."""

from __future__ import annotations

import logging
import os
from typing import Final

import msgspec

from loom.core.environment import Environment
from loom.core.model import LoomFrozenStruct

LAMBDA_FORMAT: Final = "lambda"
JSON_FORMAT: Final = "json"
TEXT_FORMAT: Final = "text"


def is_log_level(name: str) -> bool:
    """Whether *name* is a level of the standard library, in any case."""
    return name.upper() in logging.getLevelNamesMapping()


class LogSettings(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """How the process writes its log records.

    Attributes:
        format: Name of a format registered under the ``loom.logging.formats``
            entry-point group: ``lambda``, ``json`` or ``text`` ship with loom.
            ``None`` picks ``lambda`` inside AWS Lambda, ``json`` in a deployed
            environment and ``text`` anywhere else.
        level: Minimum level of the root logger. The ``lambda`` format leaves
            it to the function configuration.
        named_levels: Minimum level of the named loggers, to quiet a noisy
            library: ``{"botocore": "WARNING"}``.
    """

    format: str | None = None
    level: str = "INFO"
    named_levels: dict[str, str] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refuse a level the standard library does not know."""
        for name in (self.level, *self.named_levels.values()):
            if not is_log_level(name):
                raise ValueError(f"unsupported log level {name!r}")

    def format_for(self, environment: Environment) -> str:
        """The format name in effect for a process running in *environment*."""
        if self.format is not None:
            return self.format
        if os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None:
            return LAMBDA_FORMAT
        return JSON_FORMAT if environment.is_deployed else TEXT_FORMAT
