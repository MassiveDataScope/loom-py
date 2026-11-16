"""The application the characterisation composes, and the components each refusal needs.

Every class is module level, so its qualname, which the snapshots record, is
stable.
"""

from __future__ import annotations

from typing import Any

from loom.ai.abc import AgentHandle, McpHandle
from loom.core.command import Command
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.identity import Identity
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.model import BaseModel, ColumnField
from loom.core.sql import SqlQueryService
from loom.core.stores.repository import RepoFor
from loom.core.stores.repository.registry import RepositoryToken
from loom.core.use_case import Agent, Caller, Input, Joined, LoadById, Mcp, UseCase
from loom.rest.model import RestInterface, RestRoute
from loom.testing.in_memory import InMemoryRepository


class AcmeRecord(BaseModel):
    """The model of the default store."""

    __tablename__ = "pipeline_acme_records"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=20)


class AcmeLedgerEntry(BaseModel):
    """A model of the ``ledger`` store."""

    __tablename__ = "pipeline_acme_ledger_entries"
    __store__ = "ledger"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class AcmeUnserved(BaseModel):
    """A model no path discovers."""

    __tablename__ = "pipeline_acme_unserved"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class AcmeNamed(Command, frozen=True):
    name: str


class AcmePing(UseCase[AcmeRecord, str]):
    """About ``AcmeRecord``, so discovery serves it, without a repository."""

    async def execute(self) -> str:
        return "pong"


class AcmeInterface(RestInterface[str]):
    prefix = "/acme"
    routes = (RestRoute(use_case=AcmePing, method="GET", path="/"),)


class AcmeJob(Job[None]):
    """The job every jobs path runs."""

    def execute(self, payload: AcmeNamed = Input()) -> None:
        del payload


# ---------------------------------------------------------------------------
# Refused components
# ---------------------------------------------------------------------------


class AcmeUncompilable(UseCase[AcmeRecord, None]):
    """Fails to compile: two ``Input()`` parameters."""

    async def execute(self, first: AcmeNamed = Input(), second: AcmeNamed = Input()) -> None:
        del first, second


class AcmeUncompilableJob(Job[None]):
    """Fails to compile: two ``Input()`` parameters."""

    def execute(self, first: AcmeNamed = Input(), second: AcmeNamed = Input()) -> None:
        del first, second


class AcmeLedgerWriter(Job[None]):
    """Writes to ``ledger``, whose backend has no unit of work."""

    __store__ = "ledger"

    def __init__(self, entries: RepoFor[AcmeLedgerEntry]) -> None:
        self.entries = entries

    def execute(self) -> None:
        return None


class AcmeMissingStore(Job[None]):
    """Declares a store the catalog does not hold."""

    __store__ = "missing"

    def execute(self) -> None:
        return None


class AcmeJoinedLedger(UseCase[AcmeRecord, None]):
    """Joins ``ledger``, which cannot roll back."""

    def __init__(self, entries: Joined[RepoFor[AcmeLedgerEntry]]) -> None:
        self.entries = entries

    async def execute(self) -> None:
        return None


class AcmeBesideModel(UseCase[AcmeLedgerEntry, int]):
    """Declares ``__store__`` beside a main model of another store."""

    __store__ = "default"

    async def execute(self) -> int:
        return 0


class AcmeReadsUnserved(UseCase[object, int]):
    """Reads, through a marker, a model nothing serves."""

    async def execute(
        self, unserved_id: int, unserved: AcmeUnserved = LoadById(AcmeUnserved, by="unserved_id")
    ) -> int:
        return unserved.id


class AcmeAgentUser(UseCase[object, object]):
    """Declares an agent marker for an agent no path builds."""

    async def execute(self, triage: AgentHandle[dict[str, Any]] = Agent("triage")) -> object:
        return triage


class AcmeMcpUser(UseCase[object, object]):
    """Declares an MCP marker for a server no path declares."""

    async def execute(
        self, knowledge: McpHandle = Mcp("knowledge", include=["search_*"])
    ) -> object:
        return knowledge


class AcmeUnregistered:
    """A service no module binds."""


class AcmeNeedsService(UseCase[AcmeRecord, None]):
    """Injects a service no module binds: ``factory.verify`` refuses it, ``bootstrap_app`` not."""

    def __init__(self, service: AcmeUnregistered) -> None:
        super().__init__()
        self.service = service

    async def execute(self) -> None:
        return None


class AcmeNeedsServiceInterface(RestInterface[None]):
    prefix = "/needs-service"
    routes = (RestRoute(use_case=AcmeNeedsService, method="GET", path="/"),)


# ---------------------------------------------------------------------------
# Graph refusals (T04): each fails at its first build before the analysis
# ---------------------------------------------------------------------------


class AcmeCycleSecond:
    """Bound by :func:`binds_cycle`; needs :class:`AcmeCycleFirst` back."""

    def __init__(self, first: AcmeCycleFirst) -> None:
        self.first = first


class AcmeCycleFirst:
    """Bound by :func:`binds_cycle`; needs :class:`AcmeCycleSecond`."""

    def __init__(self, second: AcmeCycleSecond) -> None:
        self.second = second


class AcmeNeedsCycle(UseCase[AcmeRecord, None]):
    """Injects the head of a dependency cycle."""

    def __init__(self, first: AcmeCycleFirst) -> None:
        super().__init__()
        self.first = first

    async def execute(self) -> None:
        return None


class AcmeAuditTrail:
    """Bound per execution by :func:`binds_captive`."""


class AcmeAuditLedger:
    """An ``APPLICATION`` binding of :func:`binds_captive` holding a per-execution one."""

    def __init__(self, trail: AcmeAuditTrail) -> None:
        self.trail = trail


class AcmeNeedsLedger(UseCase[AcmeRecord, None]):
    """Injects :class:`AcmeAuditLedger`."""

    def __init__(self, ledger: AcmeAuditLedger) -> None:
        super().__init__()
        self.ledger = ledger

    async def execute(self) -> None:
        return None


class AcmeCallerCache:
    """An ``APPLICATION`` binding of :func:`binds_caller_cache` declaring ``Caller()``."""

    def __init__(self, caller: Identity = Caller()) -> None:
        self.caller = caller


class AcmeNeedsCallerCache(UseCase[AcmeRecord, None]):
    """Injects :class:`AcmeCallerCache`."""

    def __init__(self, cache: AcmeCallerCache) -> None:
        super().__init__()
        self.cache = cache

    async def execute(self) -> None:
        return None


class AcmeInputInConstructor(UseCase[AcmeRecord, None]):
    """Declares ``Input()`` in its constructor, where only ``execute()`` may."""

    def __init__(self, payload: AcmeNamed = Input()) -> None:
        super().__init__()
        self.payload = payload

    async def execute(self) -> None:
        return None


class AcmeUnresolvable(UseCase[AcmeRecord, None]):
    """Requires a value no binding serves."""

    def __init__(self, names: tuple[str, ...]) -> None:
        super().__init__()
        self.names = names

    async def execute(self) -> None:
        return None


def binds_cycle(container: LoomContainer) -> None:
    """Binds two classes that need each other."""
    container.register(AcmeCycleFirst, AcmeCycleFirst)
    container.register(AcmeCycleSecond, AcmeCycleSecond)


def binds_captive(container: LoomContainer) -> None:
    """Binds a singleton needing a per-execution binding."""
    container.register(AcmeAuditTrail, AcmeAuditTrail)
    container.register(AcmeAuditLedger, AcmeAuditLedger, scope=Scope.APPLICATION)


def binds_caller_cache(container: LoomContainer) -> None:
    """Binds a singleton declaring ``Caller()``."""
    container.register(AcmeCallerCache, AcmeCallerCache, scope=Scope.APPLICATION)


# ---------------------------------------------------------------------------
# Caller modules
# ---------------------------------------------------------------------------


class AcmeUnbuildable:
    """An application service whose constructor fails."""

    def __init__(self) -> None:
        raise RuntimeError("acme service cannot start")


def binds_sql_service(container: LoomContainer) -> None:
    """Binds a key the framework reserves."""
    container.register(SqlQueryService, object, scope=Scope.APPLICATION)


def binds_unbuildable(container: LoomContainer) -> None:
    """Binds an ``APPLICATION`` service that fails to instantiate."""
    container.register(AcmeUnbuildable, AcmeUnbuildable, scope=Scope.APPLICATION)


def raises_at_registration(container: LoomContainer) -> None:
    """Fails while it registers."""
    del container
    raise RuntimeError("acme module failed")


FAKE_RECORDS = InMemoryRepository(AcmeRecord)
"""The repository an override binds for ``AcmeRecord``."""

FAKE_JOBS = object()
"""The ``JobService`` an override binds."""


def overrides_repository_and_jobs(container: LoomContainer) -> None:
    """Binds ``FAKE_RECORDS`` under the default store's key of ``AcmeRecord``, and ``FAKE_JOBS``."""
    container.register_instance(RepositoryToken(AcmeRecord), FAKE_RECORDS)
    container.register_instance(JobService, FAKE_JOBS)
