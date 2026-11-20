"""Incident-triage domain types exercised by the output-hook phase tests.

``RecordTriage`` is the satisfiable hook: its command asks only for the
nested ``output`` and context names the host offers.  The other use cases
each break exactly one rule of the compile-time proof.
"""

from __future__ import annotations

from typing import Any

import msgspec

from loom.core.command import Command, Internal
from loom.core.identity import Identity
from loom.core.use_case import Caller, Input, UseCase
from loom.core.use_case.keys import use_case_key


class TriageReport(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Structured triage verdict an agent returns."""

    incident_ref: str = ""
    severity: str = "low"
    confidence: float = 0.0
    alerts: list[str] = []


TriageCounts = dict[str, int]
"""A container alias, not a class: a ``deps_type`` resolving to this compiles
through ``msgspec.json.schema()`` directly, the way it did before
``loom_type`` existed (FR-007)."""


class TriageRecorded(msgspec.Struct, frozen=True):
    """Result of recording one triage."""

    triage_id: str


class RecordTriageCommand(Command, frozen=True, kw_only=True):
    """Command fed from the run: the validated output plus context fields."""

    output: TriageReport
    interaction_id: str
    agent: str
    model: str
    recorded_by: Internal[str]
    conversation_id: str | None = None


@use_case_key("incidents.record_triage")
class RecordTriage(UseCase[Any, TriageRecorded]):
    """Hook use case whose Input is satisfiable from a run."""

    async def execute(
        self,
        cmd: RecordTriageCommand = Input(),
        caller: Identity = Caller(),
    ) -> TriageRecorded:
        return TriageRecorded(triage_id=cmd.interaction_id)


class ReviewedTriageCommand(Command, frozen=True, kw_only=True):
    """Command demanding a name no run offers."""

    output: TriageReport
    interaction_id: str
    reviewer_email: str


@use_case_key("incidents.record_reviewed_triage")
class RecordReviewedTriage(UseCase[Any, TriageRecorded]):
    """Refused: ``reviewer_email`` is required but never offered."""

    async def execute(self, cmd: ReviewedTriageCommand = Input()) -> TriageRecorded:
        return TriageRecorded(triage_id=cmd.interaction_id)


@use_case_key("incidents.record_triage_by_id")
class RecordTriageById(UseCase[Any, TriageRecorded]):
    """Refused: declares a primitive parameter the run cannot bind."""

    async def execute(
        self,
        triage_id: str,
        cmd: RecordTriageCommand = Input(),
    ) -> TriageRecorded:
        return TriageRecorded(triage_id=triage_id)


@use_case_key("incidents.count_triages")
class CountTriages(UseCase[Any, int]):
    """Refused: declares no ``Input()``."""

    async def execute(self, caller: Identity = Caller()) -> int:
        return 0


class RecordTurnCommand(Command, frozen=True, kw_only=True):
    """Command additionally requiring the run's serialised ``messages``."""

    output: TriageReport
    interaction_id: str
    messages: bytes


@use_case_key("incidents.record_turn")
class RecordTurn(UseCase[Any, TriageRecorded]):
    """Hook use case whose Input also demands the run's new messages."""

    async def execute(self, cmd: RecordTurnCommand = Input()) -> TriageRecorded:
        return TriageRecorded(triage_id=cmd.interaction_id)
