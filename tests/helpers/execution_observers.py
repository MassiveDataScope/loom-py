"""Execution observers an application declares as ``module:Class``, for tests of that wiring."""

from __future__ import annotations

from typing import ClassVar

from loom.core.engine.events import AbandonReason, EventKind, ExecutionEvent
from loom.core.job.job import Job
from loom.core.job.outcome import JobAbandonment, JobPostponement

RECORDING = "tests.helpers.execution_observers:JobOutcomeRecorder"
RAISING = "tests.helpers.execution_observers:RaisingObserver"
NEEDS_ARGUMENTS = "tests.helpers.execution_observers:NeedsArguments"
NOT_AN_OBSERVER = "tests.helpers.execution_observers:NotAnObserver"


class JobOutcomeRecorder:
    """Keeps every event it receives on the class, as an application's own observer would."""

    events: ClassVar[list[ExecutionEvent]] = []

    def on_event(self, event: ExecutionEvent) -> None:
        self.events.append(event)

    @classmethod
    def abandonments(cls) -> list[AbandonReason | None]:
        return [event.reason for event in cls.events if event.kind is EventKind.JOB_ABANDONED]


class RaisingObserver:
    """An observer that fails on every event."""

    def on_event(self, event: ExecutionEvent) -> None:
        raise RuntimeError("observer down")


class NeedsArguments:
    """A class that cannot be built with no arguments."""

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    def on_event(self, event: ExecutionEvent) -> None:
        return None


class NotAnObserver:
    """A class without ``on_event``."""


class GiveUpJob(Job[None]):
    """A job that gives up for good on its first run."""

    def execute(self) -> None:
        raise JobAbandonment("customer closed")


class PostponeForLongJob(Job[None]):
    """A job that asks to run again in a quarter of an hour, so a timer still holds it at exit."""

    def execute(self) -> None:
        raise JobPostponement("later", 900)
