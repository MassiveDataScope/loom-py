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
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import RepoFor
from loom.core.sql import SqlQueryService
from loom.core.use_case import Agent, Input, Joined, LoadById, Mcp, UseCase
from loom.rest.model import RestInterface, RestRoute


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
