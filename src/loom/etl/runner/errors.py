"""Runner-specific error types.

:class:`InvalidStageError` is defined next to :func:`loom.etl.compiler.filter_plan`,
which raises it; it is re-exported here so existing imports keep the same class.
"""

from __future__ import annotations

from loom.etl.compiler._filter import InvalidStageError

__all__ = ["InvalidStageError"]
