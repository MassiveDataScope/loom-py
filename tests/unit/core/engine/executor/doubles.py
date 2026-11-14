"""Doubles shared by the executor tests: use cases, stores, loggers and resolvers."""

from __future__ import annotations

import time
from typing import Any, cast
from unittest.mock import AsyncMock

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, elevated_scopes
from loom.core.command import Command
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope as DiScope
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.execution import (
    ExecutionContext,
    current_execution,
    execution_identity,
    per_execution,
)
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import RepoFor
from loom.core.repository.dependency import classify_key
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import Participant
from loom.core.uow.scope import Frame, InvocationScope, current_scope
from loom.core.use_case import Joined
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.kinds import CALLER, CallerResolver, use_case_kinds
from loom.core.use_case.markers import Agent, Caller, Exists, Input, Load, LoadById, Mcp, OnMissing
from loom.core.use_case.use_case import UseCase

from ...uow._coordinator_fakes import Ledger, NonTransactionalStore, PhasedStore
from .._lifecycle_doubles import Body, Run

# ---------------------------------------------------------------------------
# Callers
# ---------------------------------------------------------------------------

ALICE = Identity(subject="acme-alice", mechanism="test")
ANA = Identity(subject="acme-ana", mechanism="test")
BOB = Identity(subject="acme-bob", mechanism="test")


# ---------------------------------------------------------------------------
# Loggers and event sinks
# ---------------------------------------------------------------------------


class RecordingLogger:
    """A logger keeping every message it is given, without fields."""

    def __init__(self) -> None:
        self.messages: list[str] = []

    def bind(self, **fields: Any) -> RecordingLogger:
        return self

    def debug(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def info(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def warning(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def error(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def exception(self, event: str, **fields: Any) -> None:
        self.messages.append(event)


class ErrorRecorder:
    """A logger keeping each error line it is given, with its fields."""

    def __init__(self) -> None:
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def bind(self, **fields: Any) -> ErrorRecorder:
        return self

    def debug(self, event: str, **fields: Any) -> None: ...

    def info(self, event: str, **fields: Any) -> None: ...

    def warning(self, event: str, **fields: Any) -> None: ...

    def error(self, event: str, **fields: Any) -> None:
        self.errors.append((event, fields))

    def exception(self, event: str, **fields: Any) -> None:
        self.errors.append((event, fields))


class Journal:
    """What happened, in order: constructors, executions and runtime events."""

    def __init__(self) -> None:
        self.entries: list[str] = []

    def on_event(self, event: RuntimeEvent) -> None:
        self.entries.append(event.kind.name)


JOURNAL = Journal()


class SlowStart:
    """A metrics adapter slow on ``EXEC_START``; keeps the ``EXEC_DONE`` duration."""

    DELAY_S = 0.02

    def __init__(self) -> None:
        self.done_ms: float | None = None

    def on_event(self, event: RuntimeEvent) -> None:
        if event.kind is EventKind.EXEC_START:
            time.sleep(self.DELAY_S)
        elif event.kind is EventKind.EXEC_DONE:
            self.done_ms = event.duration_ms


# ---------------------------------------------------------------------------
# Built inside the execution
# ---------------------------------------------------------------------------


class AcmeSession:
    """A per-execution adapter that declares the caller of its execution."""

    def __init__(self, caller: Identity = Caller()) -> None:
        self.caller = caller
        self.execution = current_execution()


class AcmeProbe(UseCase[Any, "AcmeProbe"]):
    """Records the execution, the ambient identity and the session it is built with."""

    def __init__(self, session: AcmeSession) -> None:
        JOURNAL.entries.append("built")
        self.session = session
        self.built_in: ExecutionContext | None = current_execution()
        self.built_as = current_identity()
        self.ran_in: ExecutionContext | None = None

    async def execute(self) -> AcmeProbe:
        self.ran_in = current_execution()
        return self


class AcmeProbeJob(Job["AcmeProbeJob"]):
    """The job counterpart of :class:`AcmeProbe`."""

    def __init__(self, session: AcmeSession) -> None:
        self.session = session

    async def execute(self) -> AcmeProbeJob:
        return self


class AcmeBroken(UseCase[Any, None]):
    """Its constructor fails."""

    def __init__(self) -> None:
        raise LookupError("acme constructor failed")

    async def execute(self) -> None:
        return None


class AcmeBrokenJob(Job[None]):
    """Its constructor fails."""

    def __init__(self) -> None:
        raise LookupError("acme job constructor failed")

    async def execute(self) -> None:
        return None


class OnFailure:
    """A job failure callback recording each message it is given."""

    calls: list[str] = []

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        self.calls.append(exc_msg)


def serves_session(container: LoomContainer) -> None:
    """Registers :class:`AcmeSession` per execution."""
    container.register(AcmeSession, AcmeSession, scope=DiScope.REQUEST)


def session_factory() -> UseCaseFactory:
    """A factory whose container serves :class:`AcmeSession` per execution."""
    container = LoomContainer(classify_key=classify_key)
    use_case_kinds(container.kinds)
    container.kinds.bind(CALLER, CallerResolver())
    serves_session(container)
    built = UseCaseFactory(container)
    for use_case in (AcmeProbe, AcmeProbeJob, AcmeBroken, AcmeBrokenJob):
        built.register(use_case)
    return built


class TracedUseCase(UseCase[object, str]):
    async def execute(self) -> str:
        return "ok"


def refusing_builder(use_case_type: type[object]) -> object:
    raise AssertionError("the executor's builder was used instead of the given factory")


class AcmeSettings:
    """An empty application configuration."""


class AcmeAssistant(UseCase[Any, ExecutionContext | None]):
    async def execute(
        self,
        agent: Any = Agent("acme-triage"),
        docs: Any = Mcp("acme-docs", include=["search"]),
    ) -> ExecutionContext | None:
        assert agent == docs
        return current_execution()


# ---------------------------------------------------------------------------
# Who an execution runs as
# ---------------------------------------------------------------------------

Seen = tuple[Identity | None, Identity, Identity | None]


def seen(caller: Identity | None) -> Seen:
    """The caller received, the ambient identity and the execution context's identity."""
    context = current_execution()
    assert context is not None
    return caller, current_identity(), context.identity


class AcmeWho(UseCase[Any, Seen]):
    async def execute(self, caller: Identity = Caller()) -> Seen:
        return seen(caller)


class AcmeAnyone(UseCase[Any, Seen]):
    async def execute(self) -> Seen:
        return seen(None)


class AcmeNests(UseCase[Any, Seen]):
    """Runs ``inner`` nested, as ``identity`` (``None`` to inherit)."""

    def __init__(self, executor: RuntimeExecutor, inner: UseCase[Any, Seen], identity: Any) -> None:
        self.executor, self.inner, self.identity = executor, inner, identity

    async def execute(self) -> Seen:
        result: Seen = await self.executor.execute(self.inner, identity=self.identity)
        return result


class AcmeTampers(AcmeNests):
    """Changes the ambient identity, then runs ``inner`` nested with no identity."""

    async def execute(self) -> Seen:
        token = set_identity(BOB)
        try:
            return await super().execute()
        finally:
            reset_identity(token)


class AcmeTicket:
    """A per-execution value."""


TICKET = per_execution(AcmeTicket, AcmeTicket)


class AsCaller(UseCase[Any, Any]):
    """Reports who it runs as, its per-execution ticket and its scope."""

    def __init__(self) -> None:
        self._ticket = TICKET()

    async def execute(self) -> Any:
        return execution_identity(), self._ticket, current_scope()


class Building:
    """A factory building a use case with no dependencies."""

    def build(self, cls: type[Any]) -> Any:
        return cls()


BUILDING = cast(UseCaseFactory, Building())


async def identity_now() -> Any:
    return execution_identity()


async def ambient_now() -> Any:
    return current_identity()


async def channel_now() -> Any:
    return active_channel()


def done(value: Any = "ok") -> Body:
    async def body() -> Any:
        return value

    return body


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------

AUDIT = StoreKey("audit")


class Stores:
    """A unit-of-work factory handing out a fresh phased store per invocation."""

    def __init__(self, ledger: Ledger, fault: Any = None) -> None:
        self._ledger = ledger
        self._fault = fault
        self.created: list[PhasedStore] = []

    def create(self) -> UnitOfWork:
        store = PhasedStore(DEFAULT_STORE, self._ledger, self._fault)
        self.created.append(store)
        return cast(UnitOfWork, store)

    @property
    def last(self) -> PhasedStore:
        return self.created[-1]


class NonTransactionalStores:
    def create(self) -> UnitOfWork:
        return cast(UnitOfWork, NonTransactionalStore(DEFAULT_STORE, Ledger()))


def scope_now() -> InvocationScope:
    scope = current_scope()
    assert scope is not None
    return scope


def write(stores: Stores, row: str) -> None:
    """Write ``row`` through the default store the way a repository will (PR 3)."""
    scope_now().participant_for(DEFAULT_STORE, stores.last.owner, write=True)
    stores.last.write(row)


async def second_store(store: PhasedStore) -> None:
    """Enlist ``store`` by hand, a store the running execution's frame does not name."""
    scope = scope_now()
    participant = Participant(store.key, cast(UnitOfWork, store))
    await participant.begin()
    scope.participants[store.key] = participant


async def write_joined(store: PhasedStore, row: str) -> None:
    """Write through ``store`` as a ``Joined`` repository will (T046): a frame declares it."""
    scope = scope_now()
    async with scope.frame(Frame(DEFAULT_STORE, frozenset({store.key}))):
        scope.participant_for(store.key, store.owner, write=True)
        store.write(row)


class AuditEntry(BaseModel):
    __tablename__ = "executor_scope_audit_entries"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class Audited(Run):
    """Declares ``Joined`` on ``audit``; compiled on its first execution, not at startup."""

    def __init__(self, body: Body, entries: Joined[RepoFor[AuditEntry]]) -> None:
        super().__init__(body)
        self.entries = entries


# ---------------------------------------------------------------------------
# Elevation
# ---------------------------------------------------------------------------

WRITE = Permission("rows.write")
CATALOG = RoleCatalog((WRITE,), (Role("editor", (WRITE,)),))
BOUNDARY = Scope.of("b1")
DECISION = Decision(allowed=True, grant=Grant(subject="ana", role="editor", scope=BOUNDARY))


class Elevating(UseCase[Any, list[str]]):
    async def execute(self, value: str) -> list[str]:
        await elevate_scope("editor", DECISION, at=BOUNDARY, permission=WRITE, catalog=CATALOG)
        return sorted(elevated_scopes())


class SeesElevation(PhasedStore):
    """Records the elevated scopes visible when the coordinator flushes and commits.

    The session-settings provider reads them in ``after_begin``, and a
    transaction whose first SQL is the coordinator's flush begins there.
    """

    def __init__(self) -> None:
        super().__init__(DEFAULT_STORE, Ledger())
        self.seen: dict[str, list[str]] = {}

    async def flush(self) -> None:
        self.seen["flush"] = sorted(elevated_scopes())
        await super().flush()

    async def commit(self) -> None:
        self.seen["commit"] = sorted(elevated_scopes())
        await super().commit()


# ---------------------------------------------------------------------------
# Arguments of execute(): domain, use cases and repositories
# ---------------------------------------------------------------------------


class CreateUserCommand(Command, frozen=True):
    email: str
    name: str


class UpdateUserCommand(Command, frozen=True):
    email: str


class User:
    def __init__(self, id: int, email: str) -> None:
        self.id = id
        self.email = email


class Order:
    def __init__(self, id: int) -> None:
        self.id = id


class UserResult:
    def __init__(self, id: int) -> None:
        self.id = id


def fake_repo(entity: Any | None) -> Any:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=entity)
    repo.get_by = AsyncMock(return_value=entity)
    repo.exists_by = AsyncMock(return_value=entity is not None)
    return repo


class NoMarkersUseCase(UseCase[Any, str]):
    async def execute(self, value: int) -> str:
        return f"value={value}"


class InputOnlyUseCase(UseCase[Any, str]):
    async def execute(self, cmd: CreateUserCommand = Input()) -> str:
        return cmd.email


class RenamedInputStruct(Command, frozen=True, rename="camel"):
    price_cents: int
    stock: int


class RenamedInputUseCase(UseCase[Any, tuple[int, frozenset[str]]]):
    async def execute(self, cmd: RenamedInputStruct = Input()) -> tuple[int, frozenset[str]]:
        return cmd.price_cents, frozenset({"price_cents", "stock"})


class ParamsAndInputUseCase(UseCase[Any, UserResult]):
    async def execute(
        self,
        tenant_id: str,
        cmd: CreateUserCommand = Input(),
    ) -> UserResult:
        return UserResult(id=99)


class WithLoadUseCase(UseCase[Any, str]):
    async def execute(
        self,
        user_id: int,
        user: User = LoadById(User, by="user_id"),
    ) -> str:
        return user.email


class WithLoadByFieldUseCase(UseCase[Any, str]):
    async def execute(
        self,
        email: str,
        user: User = Load(User, from_param="email", against="email"),
    ) -> str:
        return user.email


class WithLoadFromCommandUseCase(UseCase[Any, str]):
    async def execute(
        self,
        cmd: CreateUserCommand = Input(),
        user: User = Load(User, from_command="email", against="email"),
    ) -> str:
        return user.email


class WithExistsUseCase(UseCase[Any, bool]):
    async def execute(
        self,
        email: str,
        user_exists: bool = Exists(User, from_param="email", against="email"),
    ) -> bool:
        return user_exists


class WithExistsRaiseUseCase(UseCase[Any, bool]):
    async def execute(
        self,
        email: str,
        user_exists: bool = Exists(
            User,
            from_param="email",
            against="email",
            on_missing=OnMissing.RAISE,
        ),
    ) -> bool:
        return user_exists


class FullPipelineUseCase(UseCase[Any, UserResult]):
    def __init__(self) -> None:
        self.received_user: User | None = None
        self.received_cmd: CreateUserCommand | None = None

    async def execute(
        self,
        user_id: int,
        cmd: CreateUserCommand = Input(),
        user: User = LoadById(User, by="user_id"),
    ) -> UserResult:
        self.received_user = user
        self.received_cmd = cmd
        return UserResult(id=user_id)


class MultiLoadUseCase(UseCase[Any, tuple[str, int]]):
    """Two independent LoadById steps on different entity types."""

    async def execute(
        self,
        user_id: int,
        order_id: int,
        user: User = LoadById(User, by="user_id"),
        order: Order = LoadById(Order, by="order_id"),
    ) -> tuple[str, int]:
        return user.email, order.id


class MultiExistsUseCase(UseCase[Any, tuple[bool, bool]]):
    """Two independent Exists steps on different entity types."""

    async def execute(
        self,
        email: str,
        order_id: int,
        user_exists: bool = Exists(User, from_param="email", against="email"),
        order_exists: bool = Exists(Order, from_param="order_id", against="id"),
    ) -> tuple[bool, bool]:
        return user_exists, order_exists


class EveryInputUseCase(UseCase[Any, tuple[Any, ...]]):
    """Returns everything it was handed, and whether it runs read-only."""

    async def execute(
        self,
        user_id: int,
        order_id: int,
        cmd: UpdateUserCommand = Input(),
        user: User = LoadById(User, by="user_id"),
        order_exists: bool = Exists(Order, from_param="order_id", against="id"),
        caller: Identity = Caller(),
    ) -> tuple[Any, ...]:
        scope = current_scope()
        assert scope is not None
        return user_id, cmd.email, user, order_exists, caller, scope.read_only


# ---------------------------------------------------------------------------
# Mcp()
# ---------------------------------------------------------------------------

McpCalls = list[tuple[str, tuple[str, ...], Identity]]


class FakeMcpHandle:
    """Stands in for an ``McpHandle`` without pulling in the AI pillar."""

    def __init__(self, server: str, include: tuple[str, ...], identity: Identity) -> None:
        self.server = server
        self.include = include
        self.identity = identity


def mcp_resolver(calls: McpCalls) -> Any:
    def resolve(server: str, include: tuple[str, ...], identity: Identity) -> FakeMcpHandle:
        calls.append((server, include, identity))
        return FakeMcpHandle(server, include, identity)

    return resolve


class DocsSearchUseCase(UseCase[object, tuple[str, str]]):
    """Declares one Mcp() marker alongside the caller."""

    async def execute(
        self,
        caller: Identity = Caller(),
        docs: Any = Mcp("docs-server", include=["search"]),
    ) -> tuple[str, str]:
        return caller.subject, docs.server


class TwoServersUseCase(UseCase[object, str]):
    """Two distinct named MCP servers in one signature."""

    async def execute(
        self,
        first: Any = Mcp("docs-server", include=["search"]),
        second: Any = Mcp("billing-server", include=["lookup"]),
    ) -> str:
        return f"{first.server}:{second.server}"


class McpOnlyUseCase(UseCase[object, str]):
    """Declares Mcp() and no Caller(): its refusal can only come from the Mcp() step."""

    async def execute(self, docs: Any = Mcp("docs-server", include=["search"])) -> str:
        return docs.server


class NoMcpUseCase(UseCase[object, str]):
    """Declares no Mcp() marker at all."""

    async def execute(self) -> str:
        return "no servers here"
