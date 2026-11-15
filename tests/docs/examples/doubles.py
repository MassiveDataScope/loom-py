"""Replacing an adapter with a test double."""

from __future__ import annotations

from collections.abc import Callable

from loom.core.di.container import LoomContainer
from tests.docs.examples.invoicing import InvoiceNotifier


class RecordingNotifier:
    """A double of ``InvoiceNotifier`` that records each call."""

    def __init__(self) -> None:
        self.issued: list[tuple[str, str]] = []

    async def invoice_issued(self, invoice_id: str, subject: str) -> None:
        self.issued.append((invoice_id, subject))


def with_double(notifier: RecordingNotifier) -> Callable[[LoomContainer], None]:
    """A module that binds *notifier* behind the port, after the application's modules."""

    def module(container: LoomContainer) -> None:
        container.register_instance(InvoiceNotifier, notifier)

    return module
