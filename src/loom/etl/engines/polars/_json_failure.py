"""Recognition of Polars refusing a JSON document while decoding a column."""

from __future__ import annotations

from typing import Final

import polars as pl

_JSON_REFUSAL_PREFIX: Final = "error deserializing JSON"


def is_json_decode_failure(error: Exception) -> bool:
    """Whether *error* is Polars refusing a JSON document while decoding a column.

    ``str.json_decode`` reports a malformed document and one that does not match the
    declared type with the same ``ComputeError`` message prefix; any other
    ``ComputeError``, such as a Parquet or Delta writer failure, is not one.
    """
    return isinstance(error, pl.exceptions.ComputeError) and str(error).startswith(
        _JSON_REFUSAL_PREFIX
    )
