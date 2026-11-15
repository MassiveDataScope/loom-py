"""Memory regression tests for MongoBatchProcessor.

These tests guard against the bug where conflict detection retained every
complex value in intermediate lists and the per-step doc-copy chain produced
3-4 copies of each batch.
"""

from __future__ import annotations

import tracemalloc
from typing import Any

from loom.etl.io.sources._mongo_batch import MongoBatchProcessor, _classify_batch_keys


def _heavy_complex_batch(rows: int, items_per_doc: int) -> list[dict[str, Any]]:
    payload = "x" * 1000
    return [
        {
            "id": i,
            "images": [{"url": payload, "alt": payload} for _ in range(items_per_doc)],
        }
        for i in range(rows)
    ]


def _peak_kb(fn: Any) -> int:
    tracemalloc.start()
    try:
        fn()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return peak // 1024


class TestMongoBatchMemory:
    def test_classify_does_not_retain_values(self) -> None:
        batch = _heavy_complex_batch(rows=1000, items_per_doc=50)

        def _run() -> None:
            _classify_batch_keys(batch)

        peak_kb = _peak_kb(_run)
        # Each doc holds ~50 * 2 strings * 1000 bytes = ~100 KB raw payload; 1000 docs = ~100 MB
        # of payload already retained outside tracemalloc. The classification scratch should be
        # bounded by the number of distinct sub-paths × type-tag sets — kilobytes, not megabytes.
        assert peak_kb < 5_000, (
            f"_classify_batch_keys retained too much memory: {peak_kb} kB"
            " — should be bounded by distinct sub-paths, not by value count"
        )

    def test_build_frame_constant_memory_in_value_count(self) -> None:
        def _run(rows: int) -> int:
            batch = _heavy_complex_batch(rows=rows, items_per_doc=20)
            processor = MongoBatchProcessor(schema_str_fields=frozenset())

            def _go() -> None:
                processor.build_frame(batch)

            return _peak_kb(_go)

        small_peak = _run(100)
        large_peak = _run(10_000)

        # Memory should grow roughly linearly with rows for the Arrow output, but the
        # processor itself must not amplify it by 3-4× via doc copies + value lists.
        # Allow a generous 150× ratio between 100-row and 10_000-row runs — the bug
        # exhibited >250× growth from accumulated value lists.
        assert large_peak < small_peak * 150, (
            f"build_frame memory grows super-linearly: 100-row peak {small_peak} kB,"
            f" 10_000-row peak {large_peak} kB (ratio {large_peak / max(small_peak, 1):.1f}×)"
        )
