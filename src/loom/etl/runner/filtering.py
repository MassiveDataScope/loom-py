"""Deprecated plan-filter names, removed in loom 3.0.

Use :func:`loom.etl.compiler.filter_plan`. Accessing any name here emits a
:class:`DeprecationWarning` naming its replacement.
"""

from __future__ import annotations

import warnings
from typing import Any

from loom.etl.compiler import _filter

_REPLACEMENTS: dict[str, str] = {
    "_filter_plan": "loom.etl.compiler.filter_plan",
    "_filter_process": "loom.etl.compiler.filter_plan",
    "_collect_all_names": "loom.etl.compiler.iter_processes and loom.etl.compiler.iter_all_steps",
    "_map_pipeline_nodes": "loom.etl.compiler.filter_plan",
    "_map_process_nodes": "loom.etl.compiler.filter_plan",
}

_SOURCES: dict[str, str] = {"_filter_plan": "filter_plan"}


def __getattr__(name: str) -> Any:
    replacement = _REPLACEMENTS.get(name)
    if replacement is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    warnings.warn(
        f"{__name__}.{name} is deprecated and will be removed in loom 3.0; "
        f"use {replacement} instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return getattr(_filter, _SOURCES.get(name, name))


__all__: list[str] = []
