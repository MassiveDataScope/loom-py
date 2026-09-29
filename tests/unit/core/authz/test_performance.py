from __future__ import annotations

import statistics
import time
from datetime import UTC, datetime, timedelta

import pytest

from loom.core.authz import Grant, Permission, Role, RoleCatalog, Scope, evaluate

READ = Permission("catalog.read")
CATALOG = RoleCatalog([READ], [Role("viewer", {READ})])
NOW = datetime(2030, 1, 1, tzinfo=UTC)


@pytest.mark.slow
@pytest.mark.parametrize("expires_at", [None, NOW + timedelta(days=1)])
def test_evaluate_stays_under_a_millisecond_with_a_thousand_grants(
    expires_at: datetime | None,
) -> None:
    grants = [Grant("ada", "viewer", Scope.of(f"w{i}", "x"), expires_at) for i in range(999)]
    grants.append(Grant("ada", "viewer", Scope.of("target"), expires_at))
    target = Scope.of("target", "sales", "orders")
    assert evaluate(CATALOG, grants, READ, target, now=NOW).allowed

    samples = []
    for _ in range(200):
        start = time.perf_counter()
        evaluate(CATALOG, grants, READ, target, now=NOW)
        samples.append(time.perf_counter() - start)

    assert statistics.median(samples) < 0.001
