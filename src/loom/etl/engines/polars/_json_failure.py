"""Recognition of Polars refusing a JSON document while decoding a column."""

from __future__ import annotations

import polars as pl


def is_json_decode_failure(error: Exception) -> bool:
    """Whether *error* is Polars refusing a JSON document while decoding a column."""
    return isinstance(error, pl.exceptions.ComputeError) and "error deserializing" in str(error)
