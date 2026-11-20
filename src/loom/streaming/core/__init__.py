"""Core primitives for the streaming DSL.

No dependencies on other loom.streaming sub-packages.
"""

from loom.streaming.core._dispatch import DispatchTable
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind, ErrorMessage, ErrorMessageMeta
from loom.streaming.core._message import Message, MessageMeta, StreamPayload

__all__ = [
    "DispatchTable",
    "ErrorEnvelope",
    "ErrorKind",
    "ErrorMessage",
    "ErrorMessageMeta",
    "Message",
    "MessageMeta",
    "StreamPayload",
]
