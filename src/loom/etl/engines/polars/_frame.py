"""Frame type variable of the Polars backend.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import TypeVar

import polars as pl

FrameT = TypeVar("FrameT", pl.DataFrame, pl.LazyFrame)
"""An eager or lazy Polars frame, kept as the same kind from argument to result."""

__all__ = ["FrameT"]
