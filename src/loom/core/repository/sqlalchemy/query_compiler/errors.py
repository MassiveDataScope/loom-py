"""Query compiler error types."""

from __future__ import annotations

from loom.core.errors import DomainError
from loom.core.errors.codes import ErrorCode


class FilterPathError(DomainError, ValueError):
    """Raised when a filter references a field path that does not exist.

    Carries the ``unsupported_query`` code, so the REST layer answers ``400``.

    Args:
        path: The unresolvable dot-separated field path.

    Example::

        raise FilterPathError("category.unknown_field")
    """

    def __init__(self, path: str) -> None:
        super().__init__(f"Unknown filter field path: {path!r}", code=ErrorCode.UNSUPPORTED_QUERY)
        self.path = path


class UnsafeFilterError(DomainError, ValueError):
    """Raised when a filter field is not in the repository's allowed set.

    This is a hard rejection — the caller requested filtering on a field
    that the repository has not explicitly permitted.  Carries the
    ``unsupported_query`` code, so the REST layer answers ``400``.

    Args:
        field: The rejected field name.

    Example::

        raise UnsafeFilterError("internal_notes")
    """

    def __init__(self, field: str) -> None:
        super().__init__(
            f"Filtering on field {field!r} is not allowed by this repository.",
            code=ErrorCode.UNSUPPORTED_QUERY,
        )
        self.field = field
