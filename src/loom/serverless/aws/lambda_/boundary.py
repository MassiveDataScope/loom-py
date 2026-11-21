"""The edge of a Lambda invocation: what reaches the runtime when the work raises.

The Python runtime writes an uncaught exception itself, ``errorMessage``,
``errorType`` and ``stackTrace``, and returns it in the invoke response,
without going through ``logging``. :class:`LambdaBoundary` stands in front of
it: an exception no scope has logged is logged once through loom, under the
record factory's policy, then replaced by a :class:`LambdaInvocationError`
that names only its type and code, so the runtime reports no value.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from loom.core.logger.record import failure_level, is_logged
from loom.serverless.aws.lambda_.errors import LambdaInvocationError
from loom.serverless.aws.lambda_.log_events import LambdaLogEvent

if TYPE_CHECKING:
    from loom.serverless.aws.lambda_.handler import LambdaHandler

_log = logging.getLogger(__name__)


class LambdaBoundary:
    """A Lambda handler whose failures reach the runtime with no message.

    Args:
        handler: The handler of the function's trigger.
    """

    def __init__(self, handler: LambdaHandler) -> None:
        self._handler = handler

    def __call__(self, event: dict[str, Any], context: Any) -> Any:
        """Answer one invocation.

        Raises:
            LambdaInvocationError: For any exception the handler raised, from ``None``.
        """
        try:
            return self._handler(event, context)
        except Exception as exc:
            raise self.reported(exc) from None

    @staticmethod
    def reported(failure: BaseException) -> LambdaInvocationError:
        """Log *failure* once, unless a scope already wrote it, and return what replaces it."""
        if not is_logged(failure):
            _log.log(failure_level(failure), LambdaLogEvent.INVOCATION_FAILED, exc_info=failure)
        return LambdaInvocationError(failure)

    def close(self) -> None:
        """Close the wrapped handler."""
        self._handler.close()
