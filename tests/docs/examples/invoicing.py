"""A use case that declares a port, an adapter for it, and the module that binds them."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.di.container import LoomContainer
from loom.core.identity import Identity
from loom.core.use_case import Caller, UseCase


class InvoiceNotifier(Protocol):
    """The port the use case depends on."""

    async def invoice_issued(self, invoice_id: str, subject: str) -> None: ...


class IssueInvoice(UseCase[Any, str]):
    """Issues an invoice and notifies the port, as the caller of the execution."""

    def __init__(self, notifier: InvoiceNotifier) -> None:
        super().__init__()
        self._notifier = notifier

    async def execute(self, invoice_id: str, caller: Identity = Caller()) -> str:
        await self._notifier.invoice_issued(invoice_id, caller.subject)
        return invoice_id


@dataclass(frozen=True)
class MailSettings:
    """Settings the adapter needs."""

    sender: str


class EmailInvoiceNotifier:
    """The adapter: its constructor parameters are resolved from the container."""

    def __init__(self, settings: MailSettings) -> None:
        self._settings = settings
        self.sent: list[str] = []

    async def invoice_issued(self, invoice_id: str, subject: str) -> None:
        self.sent.append(f"{self._settings.sender} to {subject}: {invoice_id}")


def invoicing_module(container: LoomContainer) -> None:
    """Bind the settings and the adapter behind the port."""
    container.register_instance(MailSettings, MailSettings(sender="billing@acme.test"))
    container.register(InvoiceNotifier, EmailInvoiceNotifier)


class AcmeConfig:
    """The application's configuration object."""


def invoicing_kernel(*modules: Callable[[LoomContainer], None]) -> KernelRuntime:
    """Build a kernel serving ``IssueInvoice``, with ``invoicing_module`` and *modules* after it."""
    return create_kernel(
        config=AcmeConfig(),
        use_cases=[IssueInvoice],
        modules=[invoicing_module, *modules],
    )


async def issue_as(kernel: KernelRuntime, caller: Identity, invoice_id: str) -> str:
    """Run ``IssueInvoice`` as *caller*: an explicit identity for this call."""
    return await kernel.app.for_identity(caller).invoke(
        IssueInvoice, params={"invoice_id": invoice_id}
    )
