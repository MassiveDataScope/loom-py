"""A constructor failure raises as is, before ``EXEC_START``, logged like an execution failure."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.service import InlineJobService
from tests.unit.core.engine.construction import acme


async def test_a_constructor_failure_raises_before_the_execution_starts() -> None:
    with pytest.raises(LookupError, match="acme constructor failed"):
        await acme.executor().run(acme.AcmeBroken, factory=acme.factory(), identity=acme.ALICE)

    assert acme.JOURNAL.entries == []
    assert current_execution() is None
    assert current_identity() is ANONYMOUS


async def test_a_constructor_failure_is_logged_as_the_execution_failing() -> None:
    logger = acme.Recorder()

    with pytest.raises(LookupError):
        await acme.executor(logger=logger).run(acme.AcmeBroken, factory=acme.factory())

    ((event, fields),) = logger.errors
    assert event == "[FAIL] AcmeBroken"
    assert fields["error_kind"] == "build"
    assert fields["error"] == "acme constructor failed"


class _OnFailure:
    calls: list[str] = []

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        self.calls.append(exc_msg)


@pytest.mark.parametrize("on_failure", [None, _OnFailure], ids=["no-callback", "callback"])
async def test_an_inline_job_that_fails_to_build_is_reported_like_one_that_fails_to_run(
    on_failure: type[_OnFailure] | None,
) -> None:
    _OnFailure.calls.clear()
    logger = acme.Recorder()
    factory = acme.factory()
    factory.register(_OnFailure)
    service = InlineJobService(factory, acme.executor(logger=logger))

    service.dispatch(acme.AcmeBrokenJob, on_failure=on_failure)
    await flush_pending_dispatches()

    assert [event for event, _ in logger.errors] == ["[FAIL] AcmeBrokenJob"]
    assert _OnFailure.calls == ([] if on_failure is None else ["acme job constructor failed"])
