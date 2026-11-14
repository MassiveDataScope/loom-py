"""Doubles shared by the dependency-graph tests.

``AcmeClock`` is the empty clock also used outside this package. The ledger,
rates, quote and report below are the graph's chain; other packages' classes
of those names take different constructors.
"""

from __future__ import annotations

from tests.unit.core.doubles import AcmeClock


class AcmeLedger:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeRates:
    def __init__(self, ledger: AcmeLedger) -> None:
        self.ledger = ledger


class AcmeQuote:
    def __init__(self, rates: AcmeRates) -> None:
        self.rates = rates


class AcmeReport:
    def __init__(self, ledger: AcmeLedger) -> None:
        self.ledger = ledger
