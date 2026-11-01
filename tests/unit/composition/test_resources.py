from __future__ import annotations

from contextlib import nullcontext

from loom.composition.resources import Resource, ResourceKind


def _store(readiness_key: str = "") -> Resource:
    return Resource(
        kind=ResourceKind.STORE,
        name="default",
        manager=nullcontext,
        readiness_key=readiness_key,
    )


def test_readiness_is_reported_under_the_name_by_default() -> None:
    assert _store().readiness_key == "default"


def test_a_declared_readiness_key_wins_over_the_name() -> None:
    assert _store("sqlalchemy").readiness_key == "sqlalchemy"
