"""Framework error bases."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from loom.core.errors.codes import ErrorCode


class LoomError(Exception):
    """The base of loom's own errors: a marker with no code and no transport meaning.

    :class:`CodedError` adds the code transports translate.
    """


class InvalidValue(LoomError, ValueError):
    """A loom API was given a value of the right type that it cannot accept.

    The message names the parameter and the rule, never the value it was given.
    """


class InvalidType(LoomError, TypeError):
    """A loom API was given a value of a type it does not accept.

    The message names the parameter and the expected type, never the value it was given.
    """


class CodedError(LoomError):
    """A failure transports translate into an answer.

    Carries a machine-readable ``code`` used by transport adapters to
    produce appropriate responses (HTTP status, dead-letter routing, etc.)
    without coupling the domain to any transport layer. A code is a member
    of a catalogue: :class:`~loom.core.errors.codes.ErrorCode` for the
    errors core raises and maps, or an adapter's own ``StrEnum`` (such as
    ``SQSErrorCode``) for failures only that adapter reports; a literal
    string is not a code.

    An application error may declare the HTTP answer itself, on the class
    or on the instance, and still not import a web framework:

    - ``http_status``: a 4xx or 5xx status (an :class:`~http.HTTPStatus` or
      its int), or ``None`` (the default) to keep the status the error's
      ``code`` maps to. Any other value is a misdeclaration: the REST mapper
      logs it and answers 500 with the fixed message. The attribute is typed
      ``int | None`` because :class:`~http.HTTPStatus` is an ``IntEnum`` and
      core does not import ``http``.
    - ``public_detail``: extra fields the HTTP detail gains. The default
      returns an empty mapping. A key the mapper already set is a
      misdeclaration, answered the same way, and does not replace that key.
    - ``public_message``: when ``True``, a 5xx response shows this error's
      message. The default ``False`` keeps the fixed 5xx text, so a message
      that names a host is not sent to the client.

    Args:
        message: Human-readable description of the error.
        code: The catalogue member that discriminates the error.

    Attributes:
        is_retryable: Whether running the work again could still succeed; a
            subclass a retry would repeat or make worse sets it ``False``.
        http_status: A 4xx or 5xx status (an ``HTTPStatus`` or its int), or
            ``None`` to keep the code table.
        public_message: Whether a 5xx response may show :attr:`message`.

    Example::

        raise NotFound("User", id=42)
    """

    is_retryable: bool = True
    http_status: int | None = None
    public_message: bool = False

    def __init__(self, message: str, *, code: StrEnum) -> None:
        self.message = message
        self.code: StrEnum = code
        super().__init__(message)

    def __reduce__(self) -> tuple[Any, ...]:
        """Pickle the error without calling ``__init__``, so a subclass may take any arguments.

        Returns:
            The class and arguments to rebuild the error and the state to restore.
        """
        return (type(self).__new__, (type(self), *self.args), self.__getstate__())

    def __setstate__(self, state: Any) -> None:
        """Restore the attributes :meth:`__reduce__` saved, those in ``__dict__`` and in slots.

        Args:
            state: The attribute mapping, or that mapping and the slot mapping.
        """
        attributes, slots = state if isinstance(state, tuple) else (state, {})
        for name, value in {**attributes, **slots}.items():
            setattr(self, name, value)

    def public_detail(self) -> Mapping[str, Any]:
        """Return fields an HTTP error detail may add. Empty unless overridden."""
        return {}


class DomainError(CodedError):
    """Base class for all business-logic errors.

    Raised inside UseCase execution. Transport adapters catch and convert
    these to their own error representations (a ``4xx``). Running the work
    again cannot fix the input or the rule it broke, so no transport retries
    one: a job raising it is consumed, not requeued.
    """

    is_retryable: bool = False


class SystemError(CodedError):
    """Base class for infrastructure and unexpected errors.

    Raised when a repository, external service, or internal subsystem
    fails. Transport adapters may retry on SystemError and dead-letter
    on DomainError.

    Args:
        message: Description of the system failure.
        code: The subclass's own code; ``system_error`` by default.
    """

    def __init__(self, message: str, *, code: StrEnum = ErrorCode.SYSTEM_ERROR) -> None:
        super().__init__(message, code=code)
