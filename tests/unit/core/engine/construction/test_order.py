"""The use case is built inside its execution, before ``EXEC_START``, events unchanged."""

from __future__ import annotations

from tests.unit.core.engine.construction import acme


async def test_run_builds_inside_the_execution_and_before_it_starts() -> None:
    probe = await acme.executor().run(acme.AcmeProbe, factory=acme.factory(), identity=acme.ALICE)

    assert probe.built_in is not None
    assert acme.JOURNAL.entries == ["built", "EXEC_START", "EXEC_DONE"]


async def test_execute_given_an_instance_emits_the_same_events() -> None:
    probe = acme.AcmeProbe(acme.AcmeSession(acme.ALICE))
    acme.JOURNAL.entries.clear()

    await acme.executor().execute(probe, identity=acme.ALICE)

    assert acme.JOURNAL.entries == ["EXEC_START", "EXEC_DONE"]
