"""``loom.core.di``, ``loom.core.execution`` and ``loom.core.repository`` import in any order."""

from __future__ import annotations

import subprocess
import sys
from itertools import permutations

_PACKAGES = ("loom.core.di", "loom.core.execution", "loom.core.repository")


class TestCoreImportOrder:
    def test_each_import_order_succeeds_in_a_fresh_interpreter(self) -> None:
        orders = list(permutations(_PACKAGES))
        runs = [
            subprocess.Popen(
                [sys.executable, "-I", "-c", "; ".join(f"import {name}" for name in order)],
                stderr=subprocess.PIPE,
                text=True,
            )
            for order in orders
        ]
        errors = [run.communicate(timeout=60)[1] for run in runs]

        assert [run.returncode for run in runs] == [0] * len(orders), dict(
            zip(orders, errors, strict=True)
        )
