"""The ``observability.log`` settings."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

import msgspec

from loom.core.environment import Environment, is_inside_lambda
from loom.core.model import LoomFrozenStruct

LAMBDA_FORMAT: Final = "lambda"
JSON_FORMAT: Final = "json"
TEXT_FORMAT: Final = "text"

THIRD_PARTY_FLOORS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "sqlalchemy.engine": "WARNING",
        "botocore": "WARNING",
        "urllib3": "WARNING",
        "httpx": "WARNING",
        "uvicorn.access": "WARNING",
        "celery.app.trace": "ERROR",
    }
)
"""Minimum levels of the libraries whose records carry values in their message text.

Loom cannot clean a library's message: an SQL statement, a request URL, a task
result. ``named_levels`` lowers one on purpose; a deploy check warns about it.
"""


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
        named_levels: Minimum level of the named loggers, over the
            :data:`THIRD_PARTY_FLOORS`: ``{"my_library": "WARNING"}``.
    """

    format: str | None = None
    level: str = "INFO"
    named_levels: dict[str, str] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refuse a level the standard library does not know."""
        for name in (self.level, *self.named_levels.values()):
            if not is_log_level(name):
                raise ValueError(f"unsupported log level {name!r}")

    @staticmethod
    def lowers_floor(logger: str, level: str) -> bool:
        """Whether setting *logger* to *level* goes below its :data:`THIRD_PARTY_FLOORS` entry."""
        floor = THIRD_PARTY_FLOORS.get(logger)
        if floor is None or not is_log_level(level):
            return False
        levels = logging.getLevelNamesMapping()
        return levels[level.upper()] < levels[floor]

    def logger_levels(self) -> dict[str, str]:
        """The level of each named logger: the third-party floors, then ``named_levels``."""
        return {**THIRD_PARTY_FLOORS, **self.named_levels}

    def format_for(self, environment: Environment) -> str:
        """The format name in effect for a process running in *environment*."""
        if self.format is not None:
            return self.format
        if is_inside_lambda():
            return LAMBDA_FORMAT
        return JSON_FORMAT if environment.is_deployed else TEXT_FORMAT
