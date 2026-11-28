"""Runs the examples of the ports and adapters guide (docs/guides/ports-and-adapters.md)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from loom.core.bootstrap import BootstrapError, create_kernel
from loom.core.di.graph import DependencyGraphError
from loom.core.errors import MissingCaller
from loom.core.execution import ExecutionRequired, ExecutionScope, execution_identity
from loom.core.identity import ANONYMOUS, NO_DELEGATION, Identity
from tests.docs.examples.crm import CrmDirectory, CustomerDirectory, TriageTicket, crm_module
from tests.docs.examples.doubles import RecordingNotifier, with_double
from tests.docs.examples.identity import AuditStamp, Basket, baskets, captured_by_dispatch, stamp_as
from tests.docs.examples.invoicing import (
    AcmeConfig,
    EmailInvoiceNotifier,
    InvoiceNotifier,
    IssueInvoice,
    MailSettings,
    invoicing_kernel,
    issue_as,
)
from tests.docs.examples.refusals import ScheduleReminder, factory_module


@dataclass
class _FakeCrm:
    callers: list[str] = field(default_factory=list)

    async def call(self, tool: str, arguments: dict[str, Any], *, expect: type[Any]) -> Any:
        self.callers.append(execution_identity().subject)
        return expect(email=arguments["email"], tier="gold")


@dataclass(frozen=True)
class _Answer:
    output: str


@dataclass
class _FakeSummariser:
    callers: list[str] = field(default_factory=list)

    async def run(self, prompt: str) -> _Answer:
        self.callers.append(execution_identity().subject)
        return _Answer(prompt.upper())


@dataclass
class _Resolver:
    """A call-time resolver: one handle per declaration, no identity read when resolving."""

    handle: object
    declarations: list[object] = field(default_factory=list)

    def resolve(self, declaration: object, annotation: object) -> object:
        self.declarations.append(declaration)
        return self.handle


class TestPorts:
    async def test_the_use_case_gets_the_adapter_bound_behind_its_port(self, ada: Identity) -> None:
        kernel = invoicing_kernel()

        assert await issue_as(kernel, ada, "INV-1") == "INV-1"
        assert isinstance(kernel.container.resolve(InvoiceNotifier), EmailInvoiceNotifier)

    async def test_the_adapter_receives_the_call_as_the_caller(self, ada: Identity) -> None:
        notifier = EmailInvoiceNotifier(MailSettings(sender="billing@acme.test"))
        kernel = invoicing_kernel(lambda c: c.register_instance(InvoiceNotifier, notifier))

        await issue_as(kernel, ada, "INV-1")

        assert notifier.sent == [f"billing@acme.test to {ada.subject}: INV-1"]

    async def test_caller_fails_closed_without_an_identity(self) -> None:
        kernel = invoicing_kernel()

        with pytest.raises(MissingCaller):
            await kernel.invoker.invoke(IssueInvoice, params={"invoice_id": "INV-1"})


class TestMarkers:
    async def test_the_adapter_markers_resolve_per_call_as_the_caller(self, ada: Identity) -> None:
        crm, summariser = _FakeCrm(), _FakeSummariser()
        mcp, agents = _Resolver(crm), _Resolver(summariser)
        kernel = create_kernel(
            config=AcmeConfig(),
            use_cases=[TriageTicket],
            modules=[crm_module],
            agent_resolver=agents,
            mcp_resolver=mcp,
        )

        result = await kernel.invoker.for_identity(ada).invoke(
            TriageTicket, params={"email": "ada@acme.test", "text": "printer down"}
        )

        assert result == "gold: PRINTER DOWN"
        assert crm.callers == summariser.callers == ["ada"]
        assert [repr(d) for d in mcp.declarations] == ["Mcp('acme-crm', include=['find_*'])"]
        assert [repr(d) for d in agents.declarations] == ["Agent('acme-summariser')"]
        with ExecutionScope(ada):
            assert isinstance(kernel.container.resolve(CustomerDirectory), CrmDirectory)


class TestRefusals:
    def test_an_unbound_port_stops_start_up_naming_its_path(self) -> None:
        with pytest.raises(
            BootstrapError, match="ScheduleReminder -> clock: ReminderClock"
        ) as caught:
            create_kernel(config=AcmeConfig(), use_cases=[ScheduleReminder])

        assert isinstance(caught.value.__cause__, DependencyGraphError)

    def test_a_module_binding_a_kernel_key_is_refused(self) -> None:
        with pytest.raises(BootstrapError, match="binds UseCaseFactory"):
            create_kernel(config=AcmeConfig(), use_cases=[], modules=[factory_module])


class TestIdentity:
    def test_a_singleton_reads_the_identity_of_each_call(self, ada: Identity) -> None:
        stamp = AuditStamp()
        grace = Identity(subject="grace", mechanism="jwt")

        assert stamp_as(stamp, ada) == "ada:approved"
        assert stamp_as(stamp, grace) == "grace:approved"

    def test_reading_the_identity_outside_an_execution_fails_closed(self) -> None:
        with pytest.raises(ExecutionRequired):
            AuditStamp().stamp("approved")

    def test_a_nested_execution_gets_its_own_per_execution_instances(self, ada: Identity) -> None:
        kernel = create_kernel(
            config=AcmeConfig(), use_cases=[], modules=[lambda c: c.register(Basket, Basket)]
        )

        first, again, nested = baskets(kernel.container, ada)

        assert first is again
        assert nested is not first

    def test_dispatched_work_captures_the_caller_and_its_deadline(self, ada: Identity) -> None:
        delegation = captured_by_dispatch(ada)

        assert delegation.caller == ada
        assert delegation.until is not None
        assert captured_by_dispatch(ANONYMOUS) == NO_DELEGATION


class TestDoubles:
    async def test_a_double_bound_after_the_modules_replaces_the_adapter(
        self, ada: Identity
    ) -> None:
        notifier = RecordingNotifier()
        kernel = invoicing_kernel(with_double(notifier))

        await issue_as(kernel, ada, "INV-7")

        assert notifier.issued == [("INV-7", "ada")]
