"""Boundary type errors."""

from __future__ import annotations

from loom.core.errors.base import LoomError
from loom.core.errors.codes import ErrorCode


class BoundaryValidationError(LoomError, ValueError):
    """A boundary value failed validation.

    Raised by every :class:`LoomType` implementation with the library error as
    ``__cause__``. The pydantic message is built field by field and omits the
    input and any documentation URL, and its tag, UUID and bytes-encoding
    reasons drop the detail pydantic derives from the input; the msgspec
    message is forwarded as one line, without the value msgspec echoes for an
    invalid enum value or tag. A message raised by the type's own validator
    keeps its text. A repository raises it for a value its storage cannot
    hold, such as the SQLAlchemy repository for a ``time`` whose UTC offset,
    present or missing, its column cannot take. It is also a
    :class:`ValueError`, so an ``except ValueError`` that used to catch
    ``msgspec.ValidationError`` or ``pydantic.ValidationError`` keeps
    catching it.

    Args:
        message: One-line description of the failure.
        violations: ``(field, message)`` pairs, one per failed field. Empty
            when the caller has no per-field breakdown to offer.
        code: Defaults to ``boundary_validation``; a transport may re-raise
            under its own code.
    """

    def __init__(
        self,
        message: str,
        violations: tuple[tuple[str, str], ...] = (),
        *,
        code: ErrorCode = ErrorCode.BOUNDARY_VALIDATION,
    ) -> None:
        super().__init__(message, code=code)
        self.violations = violations


class UnsupportedBoundaryType(LoomError):
    """A symbol cannot be compiled into a :class:`LoomType`.

    Args:
        reason: Why the symbol is rejected, shared by every boundary's issue.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason, code=ErrorCode.BOUNDARY_TYPE_UNSUPPORTED)
