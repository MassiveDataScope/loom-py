"""What loom may say about a failure without repeating foreign text."""

from __future__ import annotations

from typing import Final

import msgspec

from loom.core.errors.base import CodedError

EXCEPTION_TYPE_FIELD: Final = "exception.type"
ERROR_CODE_FIELD: Final = "loom.error.code"


class ErrorSummary(msgspec.Struct, frozen=True, kw_only=True):
    """What a failure is, without its message: its type and, for a :class:`CodedError`, its code."""

    type: str
    code: str | None = None

    @classmethod
    def of(cls, exc: BaseException) -> ErrorSummary:
        """Summarise *exc*."""
        code = str(exc.code) if isinstance(exc, CodedError) else None
        return cls(type=type(exc).__qualname__, code=code)

    @classmethod
    def message_for(cls, exc: BaseException) -> str:
        """The text a loom message may repeat about *exc*, its cause.

        Loom's own exceptions carry text loom wrote, so their message is repeated;
        any other exception is named by its summary, since its text can hold values.
        """
        if type(exc).__module__.startswith("loom."):
            return str(exc)
        return cls.of(exc).describe()

    def describe(self) -> str:
        """The type, followed by the code when there is one: ``DomainError (not_found)``."""
        return self.type if self.code is None else f"{self.type} ({self.code})"

    def fields(self) -> dict[str, str]:
        """The record fields: ``exception.type`` and, with a code, ``loom.error.code``."""
        if self.code is None:
            return {EXCEPTION_TYPE_FIELD: self.type}
        return {EXCEPTION_TYPE_FIELD: self.type, ERROR_CODE_FIELD: self.code}
