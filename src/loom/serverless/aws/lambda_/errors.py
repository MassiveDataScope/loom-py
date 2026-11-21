"""The errors of the Lambda edge."""

from __future__ import annotations

from loom.core.errors import LoomError, SystemError
from loom.core.errors.codes import ErrorCode
from loom.core.logger.record import exception_type


class LambdaInvocationError(SystemError):
    """What the Lambda runtime reports for a failed invocation: the type and code, no message.

    The runtime writes only the message, the type and the frames of the
    exception it catches, so the failure it replaces is kept as
    :attr:`failure` for a caller in the same process, never in the message or
    the exception chain.

    Args:
        failure: The exception the invocation raised.
    """

    def __init__(self, failure: BaseException) -> None:
        code = failure.code if isinstance(failure, LoomError) else ErrorCode.SYSTEM_ERROR
        super().__init__(f"{exception_type(type(failure))} ({code})", code=code)
        self.failure = failure
