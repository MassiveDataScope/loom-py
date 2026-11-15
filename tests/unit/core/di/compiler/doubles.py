"""Doubles shared by the compiler tests."""

from __future__ import annotations

from loom.core.repository.abc import RepoFor
from tests.unit.core.doubles import AcmeClock


class AcmeFrozenClock(AcmeClock):
    pass


class AcmeClockRates:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeQuotes:
    def __init__(self, rates: AcmeClockRates) -> None:
        self.rates = rates


class AcmeAudit:
    """Unrelated to the clock."""


class AcmeTenant:
    """Registered as an instance."""


class AcmeAuditReport:
    def __init__(self, audits: RepoFor[AcmeAudit]) -> None:
        self.audits = audits
