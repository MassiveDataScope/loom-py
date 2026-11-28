"""Recognition of Polars refusing a JSON document while decoding a column."""

from __future__ import annotations

import re
from typing import Final

import polars as pl

_DOCUMENT_REFUSAL_PREFIX: Final = "error deserializing JSON"
_DECODE_EXPRESSION: Final = ".str.json_decode("
_CONVERSION_REFUSAL: Final = re.compile(
    r"conversion from `str` to `(date|datetime\[)|not all elements have the specified width"
)
_REFUSING_ERRORS: Final = (pl.exceptions.ComputeError, pl.exceptions.InvalidOperationError)


def is_json_decode_failure(error: Exception) -> bool:
    """Whether *error* is Polars refusing a JSON document while decoding a column.

    A refused document is a ``ComputeError``. A refused date, timestamp or array
    width is a ``ComputeError`` or an ``InvalidOperationError`` raised by an
    expression that decodes JSON.
    """
    if not isinstance(error, _REFUSING_ERRORS):
        return False
    message = str(error)
    if isinstance(error, pl.exceptions.ComputeError) and message.startswith(
        _DOCUMENT_REFUSAL_PREFIX
    ):
        return True
    return bool(_CONVERSION_REFUSAL.match(message)) and _DECODE_EXPRESSION in message
