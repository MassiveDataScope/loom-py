"""Garbage-collector policies an engine applies once the flow is built.

An engine calls :func:`apply` after compiling and wiring its flow and before
the first record. Policies:

* ``""``: the interpreter's defaults;
* ``"freeze"``: ``gc.collect()`` then ``gc.freeze()``. Everything allocated
  during startup moves to the permanent generation, and the collector never
  traverses it again;
* ``"threshold:T0[,T1[,T2]]"``: ``gc.set_threshold(T0, T1, T2)``. Raising T0
  makes young collections less frequent. On Python 3.14, whose collector is
  incremental, a young collection also scans a slice of the old generation, so
  T0 sets the frequency of both.
"""

from __future__ import annotations

import gc


def apply(policy: str) -> None:
    """Apply *policy*.

    Raises:
        ValueError: If *policy* is not one of the documented forms.
    """
    if not policy:
        return
    if policy == "freeze":
        gc.collect()
        gc.freeze()
        return
    name, _, values = policy.partition(":")
    if name == "threshold" and values:
        gc.set_threshold(*(int(v) for v in values.split(",")))
        return
    raise ValueError(f"unknown gc policy {policy!r}")


def describe() -> dict[str, object]:
    """Return the collector's current thresholds and frozen object count."""
    return {"gc_threshold": list(gc.get_threshold()), "gc_frozen": gc.get_freeze_count()}
