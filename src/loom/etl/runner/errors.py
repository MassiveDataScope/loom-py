"""Errors the ETL pillar raises at run time."""

from __future__ import annotations

from collections.abc import Iterable

from loom.core.errors import LoomError
from loom.core.errors.codes import ErrorCode
from loom.etl.declarative._format import Format

__all__ = ["UnsupportedFormatError"]


class UnsupportedFormatError(LoomError, ValueError):
    """Raised when no handler of the backend implements a declared file format.

    Args:
        fmt:       Format the pipeline declared.
        supported: Formats the backend registered for this operation.

    Attributes:
        format:    Format the pipeline declared.
        supported: Formats the backend registered, sorted by value.

    Example::

        raise UnsupportedFormatError(Format.XLSX, (Format.CSV, Format.PARQUET))
    """

    def __init__(self, fmt: Format, supported: Iterable[Format]) -> None:
        ordered = tuple(sorted(supported, key=lambda item: item.value))
        names = ", ".join(item.value for item in ordered) or "none"
        super().__init__(
            f"Unsupported format: {fmt.value}. Supported here: {names}.",
            code=ErrorCode.UNSUPPORTED_FORMAT,
        )
        self.format = fmt
        self.supported = ordered
