"""SpendLedger: reserve the worst case before a call, settle with the real cost after."""

from __future__ import annotations

from decimal import Decimal

from loom.ai.etl._ledger import SpendLedger


class TestWithoutBudget:
    def test_every_reservation_is_granted(self) -> None:
        ledger = SpendLedger(None)

        assert all(ledger.reserve(Decimal("100")) for _ in range(5))

    def test_spend_is_still_recorded(self) -> None:
        ledger = SpendLedger(None)
        ledger.reserve(Decimal("1"))

        ledger.settle(Decimal("1"), Decimal("0.25"))

        assert ledger.spent == Decimal("0.25")


class TestWithBudget:
    def test_a_reservation_inside_the_budget_is_granted(self) -> None:
        assert SpendLedger(Decimal("1")).reserve(Decimal("1"))

    def test_reservations_in_flight_count_against_the_budget(self) -> None:
        ledger = SpendLedger(Decimal("1"))
        ledger.reserve(Decimal("0.6"))

        assert not ledger.reserve(Decimal("0.6"))

    def test_settling_replaces_the_reservation_with_the_real_cost(self) -> None:
        ledger = SpendLedger(Decimal("1"))
        ledger.reserve(Decimal("0.6"))

        ledger.settle(Decimal("0.6"), Decimal("0.1"))

        assert ledger.spent == Decimal("0.1")
        assert ledger.reserve(Decimal("0.6"))

    def test_an_unknown_cost_is_settled_at_the_reservation(self) -> None:
        ledger = SpendLedger(Decimal("1"))
        ledger.reserve(Decimal("0.6"))

        ledger.settle(Decimal("0.6"), None)

        assert ledger.spent == Decimal("0.6")

    def test_once_exhausted_nothing_else_is_launched(self) -> None:
        ledger = SpendLedger(Decimal("1"))
        ledger.reserve(Decimal("0.9"))
        assert not ledger.reserve(Decimal("0.5"))
        ledger.settle(Decimal("0.9"), Decimal("0"))

        assert not ledger.reserve(Decimal("0.01"))
