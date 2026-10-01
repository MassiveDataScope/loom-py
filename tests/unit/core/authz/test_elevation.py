from __future__ import annotations

import asyncio

import pytest
from loom.core.authz.elevation import elevate_scope, elevated_scopes, elevation_scope

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from tests.unit.core.authz._elevation_doubles import FakeSink

WRITE = Permission("rows.write")
READ = Permission("rows.read")
CATALOG = RoleCatalog((WRITE, READ), (Role("editor", (WRITE, READ)), Role("viewer", (READ,))))
BOUNDARY = Scope.of("b1")


def _allowed(role: str = "editor", scope: Scope = BOUNDARY) -> Decision:
    return Decision(allowed=True, grant=Grant(subject="ana", role=role, scope=scope))


async def _elevate(decision: Decision | None = None, *, at: Scope = BOUNDARY) -> None:
    await elevate_scope("editor", decision or _allowed(), at=at, permission=WRITE, catalog=CATALOG)


def test_no_frame_means_no_elevation() -> None:
    assert elevated_scopes() == frozenset()


async def test_elevating_outside_an_execution_is_a_programming_error() -> None:
    with pytest.raises(RuntimeError, match="frame"):
        await _elevate()


async def test_elevation_is_visible_inside_the_frame_and_gone_after_it() -> None:
    sink = FakeSink()
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
        assert elevated_scopes() == frozenset({"editor"})
    assert elevated_scopes() == frozenset()


async def test_without_an_open_transaction_the_flag_is_left_to_the_provider() -> None:
    sink = FakeSink(open=False)
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
    assert sink.set == []


async def test_with_an_open_transaction_the_flag_is_set_immediately() -> None:
    sink = FakeSink(open=True)
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
    assert sink.set == ["editor"]


async def test_a_failing_sink_leaves_the_scope_unelevated_and_re_raises() -> None:
    sink = FakeSink(open=True, fail_on_set=True)
    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(RuntimeError, match="database says no"):
            await _elevate()
        assert elevated_scopes() == frozenset()


async def test_a_denied_decision_cannot_elevate() -> None:
    sink = FakeSink()
    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(PermissionError):
            await _elevate(Decision(allowed=False, grant=None))


async def test_a_grant_whose_role_lacks_the_mapped_permission_cannot_elevate() -> None:
    sink = FakeSink()
    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(PermissionError, match="viewer"):
            await _elevate(_allowed(role="viewer"))


async def test_a_grant_that_does_not_cover_the_boundary_cannot_elevate() -> None:
    sink = FakeSink()
    narrow = _allowed(scope=Scope.of("b1", "dept", "x"))
    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(PermissionError, match="cover"):
            await _elevate(narrow, at=BOUNDARY)


async def test_a_nested_frame_inherits_but_never_mutates_its_parent() -> None:
    sink = FakeSink(open=True)
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
        async with elevation_scope(owns_transaction=False, sink=sink):
            assert elevated_scopes() == frozenset({"editor"})
            await elevate_scope(
                "second", _allowed(), at=BOUNDARY, permission=WRITE, catalog=CATALOG
            )
            assert elevated_scopes() == frozenset({"editor", "second"})
        assert elevated_scopes() == frozenset({"editor"})
    assert sink.cleared == [frozenset({"second"})]


async def test_a_nested_frame_elevating_an_already_elevated_scope_does_not_downgrade_it() -> None:
    sink = FakeSink(open=True)
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
        async with elevation_scope(owns_transaction=False, sink=sink):
            await _elevate()
        assert elevated_scopes() == frozenset({"editor"})
    assert sink.cleared == []


async def test_an_owning_frame_never_clears_flags_the_commit_ends_them() -> None:
    sink = FakeSink(open=True)
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
    assert sink.cleared == []


async def test_a_task_spawned_inside_the_frame_loses_elevation_when_the_frame_closes() -> None:
    sink = FakeSink()
    seen_inside: list[frozenset[str]] = []
    release = asyncio.Event()

    async def child() -> frozenset[str]:
        seen_inside.append(elevated_scopes())
        await release.wait()
        return elevated_scopes()

    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
        task = asyncio.create_task(child())
        await asyncio.sleep(0)
    release.set()

    assert seen_inside == [frozenset({"editor"})]
    assert await task == frozenset()


async def test_a_cancelled_inner_frame_still_clears_what_it_added() -> None:
    sink = FakeSink(open=True)

    async def inner() -> None:
        async with elevation_scope(owns_transaction=False, sink=sink):
            await _elevate()
            await asyncio.sleep(10)

    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.01):
                await inner()
        assert elevated_scopes() == frozenset()
    assert sink.cleared == [frozenset({"editor"})]
