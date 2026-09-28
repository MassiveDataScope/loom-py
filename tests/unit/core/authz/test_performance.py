from __future__ import annotations

import statistics
import time

import pytest

from loom.core.authz import Grant, Permission, Role, RoleCatalog, Scope, evaluate

READ = Permission("catalog.read")
CATALOG = RoleCatalog([READ], [Role("viewer", {READ})])


@pytest.mark.slow
def test_evaluate_stays_under_a_millisecond_with_a_thousand_grants() -> None:
    grants = [Grant("ada", "viewer", Scope.of(f"w{i}", "x")) for i in range(999)]
    grants.append(Grant("ada", "viewer", Scope.of("target")))
    target = Scope.of("target", "sales", "orders")
    assert evaluate(CATALOG, grants, READ, target).allowed

    samples = []
    for _ in range(200):
        start = time.perf_counter()
        evaluate(CATALOG, grants, READ, target)
        samples.append(time.perf_counter() - start)

    assert statistics.median(samples) < 0.001
