"""Recognition of a FAILFAST ``from_json`` rejecting a malformed document."""

from __future__ import annotations

from contextlib import suppress
from typing import Any, Final

from py4j.protocol import Py4JError

_MALFORMED_RECORD: Final = "MALFORMED_RECORD_IN_PARSING"
_ERROR_CLASS_GETTERS: Final = ("getCondition", "getErrorClass")


def is_json_decode_failure(error: Exception) -> bool:
    """Whether *error* is a FAILFAST ``from_json`` rejecting a malformed document.

    Spark reports it as the error class ``MALFORMED_RECORD_IN_PARSING`` on a
    cause of the JVM exception that PySpark carries.
    """
    java_error = getattr(error, "java_exception", None)
    while java_error is not None:
        if _error_class_of(java_error).startswith(_MALFORMED_RECORD):
            return True
        java_error = java_error.getCause()
    return False


def _error_class_of(java_error: Any) -> str:
    for getter in _ERROR_CLASS_GETTERS:
        with suppress(Py4JError):
            return str(getattr(java_error, getter)() or "")
    return ""
