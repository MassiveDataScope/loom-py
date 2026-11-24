"""What loom may say about a failure without repeating foreign text."""

from __future__ import annotations

import msgspec

from loom.core.errors.base import CodedError
from loom.core.log_names import LogField


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

    def fields(self, type_key: LogField = LogField.ERROR_TYPE) -> dict[str, str]:
        """The record fields: the type under *type_key* and, with a code, ``loom.error.code``.

        A record with no exception to carry, such as a lifecycle event that kept the
        summary alone or loom's own token failure, whose frames hold the token and the key,
        writes ``error.type``. ``exception.type`` is the context filter's alone: it writes it
        from the ``exc_info`` of a record that logs a caught exception.
        """
        if self.code is None:
            return {type_key: self.type}
        return {type_key: self.type, LogField.ERROR_CODE: self.code}
