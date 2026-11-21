"""Acme shipping on an eager Celery worker: the job reaches MCP and the agent as the enqueuer.

The worker opens the application on ``worker_init`` and closes it on
``worker_shutdown``; each task carries the identity of whoever enqueued it,
in the job envelope's wire format.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from typing import Any

import pytest
from celery.signals import worker_init, worker_shutdown

from loom.celery.constants import TASK_JOB_PREFIX
from loom.core.errors import DelegationExpired, Unauthenticated
from loom.core.identity import Identity
from loom.core.identity.wire import encode_identity
from loom.core.logger.failure import exception_type
from tests.helpers.celery import WorkerDouble, isolated_signals
from tests.integration.hexagonal.conftest import (
    ANA,
    BAD_IDENTITIES,
    BO,
    CALLERS,
    PARAMS,
    RENDEZVOUS_SECONDS,
    Compose,
    Seen,
    acted_as,
    notice_for,
)
from tests.integration.hexagonal.shipping import TrackShipmentJob

_isolated_signals = isolated_signals(worker_init, worker_shutdown)


@pytest.fixture
def task(compose: Compose) -> Iterator[Any]:
    """The ``TrackShipmentJob`` task of the acme worker, started for the test, run eagerly."""
    _, celery_app = compose("celery")
    worker_init.send(sender=WorkerDouble())
    yield celery_app.tasks[f"{TASK_JOB_PREFIX}.{TrackShipmentJob.__qualname__}"]
    worker_shutdown.send(sender=None)


def _enqueued_with(identity: object) -> dict[str, Any]:
    return {"params": PARAMS, "identity": identity}


class TestTrackShipmentJobOnCelery:
    @CALLERS
    def test_runs_through_adapters_acting_as_the_enqueuer(
        self, task: Any, seen: Seen, caller: Identity
    ) -> None:
        result = task.apply(kwargs=_enqueued_with(encode_identity(caller)))

        assert result.get() == notice_for(caller)
        assert seen.calls == acted_as(caller)

    def test_a_task_without_a_caller_fails_closed_before_reaching_an_adapter(
        self, task: Any, seen: Seen
    ) -> None:
        result = task.apply(kwargs=_enqueued_with(None))

        assert result.result.exception_type == exception_type(Unauthenticated)
        assert seen.calls == []

    @pytest.mark.parametrize(
        ("bad", "refusal"),
        [
            ("empty-subject", Unauthenticated),
            ("not-a-mapping", Unauthenticated),
            ("subject-not-a-string", Unauthenticated),
            ("expired", DelegationExpired),
            ("unreadable-until", DelegationExpired),
        ],
    )
    def test_a_task_naming_no_valid_caller_fails_closed_before_reaching_an_adapter(
        self, task: Any, seen: Seen, bad: str, refusal: type[Exception]
    ) -> None:
        result = task.apply(kwargs=_enqueued_with(BAD_IDENTITIES[bad]))

        assert (result.state, result.result.exception_type) == (
            "FAILURE",
            exception_type(refusal),
        )
        assert seen.calls == []


class TestTasksAppliedFromTwoThreads:
    def test_never_see_each_others_caller(self, task: Any, seen: Seen) -> None:
        seen.rendezvous = asyncio.Barrier(2)
        start = threading.Barrier(2, timeout=RENDEZVOUS_SECONDS)
        notices: dict[str, str] = {}

        def apply_as(caller: Identity) -> None:
            start.wait()
            result = task.apply(kwargs=_enqueued_with(encode_identity(caller)))
            notices[caller.subject] = result.get()

        threads = [threading.Thread(target=apply_as, args=(caller,)) for caller in (ANA, BO)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(RENDEZVOUS_SECONDS)

        assert notices == {ANA.subject: notice_for(ANA), BO.subject: notice_for(BO)}
        assert sorted(seen.calls, key=repr) == sorted(acted_as(ANA, BO), key=repr)
