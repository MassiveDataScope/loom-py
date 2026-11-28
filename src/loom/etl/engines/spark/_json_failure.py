"""The ``from_json`` options of each JSON mismatch policy and recognition of a FAILFAST refusal."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from typing import Any, Final

from py4j.protocol import Py4JError

from loom.etl.declarative.source import JsonMismatch

FROM_JSON_OPTIONS: Final[Mapping[JsonMismatch, Mapping[str, str]]] = {
    JsonMismatch.FAIL: {"mode": "FAILFAST", "timeZone": "UTC"},
    JsonMismatch.NULL: {"mode": "PERMISSIVE", "timeZone": "UTC"},
}

_REFUSAL_CLASS: Final = "MALFORMED_RECORD_IN_PARSING"
_ERROR_CLASS_GETTERS: Final = ("getCondition", "getErrorClass")


def is_json_decode_failure(error: Exception) -> bool:
    """Whether *error* is a FAILFAST ``from_json`` rejecting a document."""
    java_error = getattr(error, "java_exception", None)
    while java_error is not None:
        if _error_class_of(java_error).startswith(_REFUSAL_CLASS):
            return True
        java_error = java_error.getCause()
    return False


def _error_class_of(java_error: Any) -> str:
    for getter in _ERROR_CLASS_GETTERS:
        with suppress(Py4JError):
            return str(getattr(java_error, getter)() or "")
    return ""
