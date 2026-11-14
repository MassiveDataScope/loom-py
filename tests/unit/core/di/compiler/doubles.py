"""Doubles shared by the compiler tests."""

from __future__ import annotations

from loom.core.repository.abc import RepoFor


class AcmeClock:
    pass


class AcmeFrozenClock(AcmeClock):
    pass


class AcmeRates:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeQuotes:
    def __init__(self, rates: AcmeRates) -> None:
        self.rates = rates


class AcmeAudit:
    """Unrelated to the clock."""


class AcmeTenant:
    """Registered as an instance."""


class AcmeAuditReport:
    def __init__(self, audits: RepoFor[AcmeAudit]) -> None:
        self.audits = audits
