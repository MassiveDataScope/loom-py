"""Framework error bases."""

from __future__ import annotations

from loom.core.errors.codes import ErrorCode


class LoomError(Exception):
    """Base class for all framework errors.

    Carries a machine-readable ``code`` used by transport adapters to
    produce appropriate responses (HTTP status, dead-letter routing, etc.)
    without coupling the domain to any transport layer.

    Args:
        message: Human-readable description of the error.
        code: Machine-readable discriminator (e.g. ``"not_found"``).

    Attributes:
        retryable: Whether running the work again could still succeed; a
            subclass a retry would repeat or make worse sets it ``False``.

    Example::

        raise NotFound("User", id=42)
    """

    retryable: bool = True

    def __init__(self, message: str, *, code: str) -> None:
        self.message = message
        self.code = code
        super().__init__(message)


class DomainError(LoomError):
    """Base class for all business-logic errors.

    Raised inside UseCase execution. Transport adapters catch and convert
    these to their own error representations.
    """


class SystemError(LoomError):
    """Base class for infrastructure and unexpected errors.

    Raised when a repository, external service, or internal subsystem
    fails. Transport adapters may retry on SystemError and dead-letter
    on DomainError.

    Args:
        message: Description of the system failure.
        code: The subclass's own code; ``system_error`` by default.
    """

    def __init__(self, message: str, *, code: ErrorCode = ErrorCode.SYSTEM_ERROR) -> None:
        super().__init__(message, code=code)
