from __future__ import annotations

import asyncio

import pytest

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, elevated_scopes, elevation_scope
from tests.unit.core.authz._elevation_doubles import FakeSink

WRITE = Permission("rows.write")
READ = Permission("rows.read")
CATALOG = RoleCatalog((WRITE, READ), (Role("editor", (WRITE, READ)), Role("viewer", (READ,))))
BOUNDARY = Scope.of("b1")


def _allowed(role: str = "editor", scope: Scope = BOUNDARY) -> Decision:
    return Decision(allowed=True, grant=Grant(subject="ana", role=role, scope=scope))


async def _elevate(decision: Decision | None = None, *, at: Scope = BOUNDARY) -> None:
    chosen = _allowed() if decision is None else decision
    await elevate_scope("editor", chosen, at=at, permission=WRITE, catalog=CATALOG)


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


async def test_a_failing_sink_invalidates_leaves_the_scope_unelevated_and_re_raises() -> None:
    sink = FakeSink(open=True, fail_on_set=True)
    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(RuntimeError, match="database says no"):
            await _elevate()
        assert elevated_scopes() == frozenset()
    assert sink.invalidated == 1


async def test_a_cancellation_once_the_flag_was_sent_invalidates_the_session() -> None:
    sink = FakeSink(open=True, set_gate=asyncio.Event())
    assert sink.set_gate is not None
    task = asyncio.create_task(_nested_elevation(sink))
    while not sink.set:
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    sink.set_gate.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert (sink.invalidated, sink.cleared) == (1, [])


class _Interrupt(BaseException):
    """Not an ``Exception``, and not special to asyncio."""


async def test_a_clearing_interrupted_by_a_base_exception_invalidates_the_session() -> None:
    sink = FakeSink(open=True, clear_raises=_Interrupt())

    with pytest.raises(_Interrupt):
        await _nested_elevation(sink)

    assert sink.invalidated == 1


async def test_a_denied_decision_cannot_elevate() -> None:
    sink = FakeSink()
    denied = Decision(allowed=False, grant=None)
    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(PermissionError):
            await _elevate(denied)


async def test_a_grant_whose_role_lacks_the_mapped_permission_cannot_elevate() -> None:
    sink = FakeSink()
    async with elevation_scope(owns_transaction=True, sink=sink):
        viewer_grant = _allowed(role="viewer")
        with pytest.raises(PermissionError, match="viewer"):
            await _elevate(viewer_grant)


async def test_a_grant_that_does_not_cover_the_boundary_cannot_elevate() -> None:
    sink = FakeSink()
    narrow = _allowed(scope=Scope.of("b1", "dept", "x"))
    async with elevation_scope(owns_transaction=True, sink=sink):
        with pytest.raises(PermissionError, match="cover"):
            await _elevate(narrow, at=BOUNDARY)


async def _elevate_second() -> None:
    await elevate_scope("second", _allowed(), at=BOUNDARY, permission=WRITE, catalog=CATALOG)


async def test_a_nested_frame_inherits_nothing_and_sees_only_what_it_elevated() -> None:
    sink = FakeSink(open=True)
    seen: list[frozenset[str]] = []
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
        async with elevation_scope(owns_transaction=False, sink=sink):
            seen.append(elevated_scopes())
            await _elevate_second()
            seen.append(elevated_scopes())

    assert seen == [frozenset(), frozenset({"second"})]


async def test_a_nested_frame_never_mutates_its_parent_and_clears_what_it_added() -> None:
    sink = FakeSink(open=True)
    async with elevation_scope(owns_transaction=True, sink=sink):
        await _elevate()
        async with elevation_scope(owns_transaction=False, sink=sink):
            await _elevate_second()
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
    elevated = asyncio.Event()

    async def inner() -> None:
        async with elevation_scope(owns_transaction=False, sink=sink):
            await _elevate()
            elevated.set()
            await asyncio.Event().wait()

    async with elevation_scope(owns_transaction=True, sink=sink):
        task = asyncio.create_task(inner())
        await elevated.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert elevated_scopes() == frozenset()
    assert (sink.cleared, sink.invalidated) == ([frozenset({"editor"})], 0)


async def _nested_elevation(
    sink: FakeSink, *, hold: asyncio.Event | None = None, elevated: asyncio.Event | None = None
) -> None:
    """Elevate in a nested frame; with ``hold``, wait for it before the frame closes."""
    outer = elevation_scope(owns_transaction=True, sink=sink)
    inner = elevation_scope(owns_transaction=False, sink=sink)
    async with outer, inner:
        await _elevate()
        if elevated is not None:
            elevated.set()
        if hold is not None:
            await hold.wait()


@pytest.mark.parametrize(
    ("sink", "message"),
    [
        (FakeSink(open=True, fail_on_clear=True), "database says no"),
        (FakeSink(open=True, refuse_clear=True), "refused before any statement"),
    ],
    ids=["clear-fails", "clear-refused"],
)
async def test_a_failed_clear_invalidates_the_session_and_is_raised(
    sink: FakeSink, message: str
) -> None:
    with pytest.raises(RuntimeError, match=message):
        await _nested_elevation(sink)

    assert sink.invalidated == 1


async def test_a_failed_invalidation_after_a_failed_clear_is_raised_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sink = FakeSink(open=True, fail_on_clear=True, fail_on_invalidate=True)

    with pytest.raises(RuntimeError, match="database says no") as raised:
        await _nested_elevation(sink)

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "could not be invalidated" in caplog.text
    assert elevated_scopes() == frozenset()


async def test_a_failed_invalidation_is_raised_over_a_cancellation() -> None:
    sink = FakeSink(open=True, fail_on_clear=True, fail_on_invalidate=True)
    elevated = asyncio.Event()
    task = asyncio.create_task(_nested_elevation(sink, hold=asyncio.Event(), elevated=elevated))
    await elevated.wait()
    task.cancel()

    with pytest.raises(RuntimeError, match="database says no"):
        await task
    assert sink.invalidated == 1


async def test_elevate_outside_an_execution_is_a_runtime_error_even_without_a_product() -> None:
    from loom.core.authz.product import clear_authz_product
    from loom.core.repository.sqlalchemy.rls import elevate

    clear_authz_product()

    allowed_decision = _allowed()

    with pytest.raises(RuntimeError, match="execution frame"):
        await elevate("editor", allowed_decision, at=BOUNDARY)
