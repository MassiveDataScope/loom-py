"""Recognition of Polars refusing a JSON document while decoding a column."""

from __future__ import annotations

import re
from typing import Final

import polars as pl

_JSON_REFUSAL_PREFIX: Final = "error deserializing JSON"
_TEMPORAL_REFUSAL: Final = re.compile(r"conversion from `str` to `(date|datetime\[)")


def is_json_decode_failure(error: Exception) -> bool:
    """Whether *error* is Polars refusing a JSON document while decoding a column.

    ``str.json_decode`` reports a malformed document and one that does not match the
    declared type with the same ``ComputeError`` message prefix, and invalid date or
    timestamp text is refused by the conversion that follows it with an
    ``InvalidOperationError``; any other ``ComputeError``, such as a Parquet or Delta
    writer failure, is not one.
    """
    if isinstance(error, pl.exceptions.ComputeError):
        return str(error).startswith(_JSON_REFUSAL_PREFIX)
    return isinstance(error, pl.exceptions.InvalidOperationError) and bool(
        _TEMPORAL_REFUSAL.match(str(error))
    )
