"""Use cases whose constructors record the execution they are built in."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import RuntimeEvent
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import ExecutionContext, current_execution
from loom.core.identity import Identity, current_identity
from loom.core.job.job import Job
from loom.core.repository.dependency import classify_key
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.kinds import CALLER, CallerResolver, use_case_kinds
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase

ALICE = Identity(subject="acme-alice", mechanism="test")
BOB = Identity(subject="acme-bob", mechanism="test")


class Journal:
    """What happened, in order: constructors, executions and runtime events."""

    def __init__(self) -> None:
        self.entries: list[str] = []

    def on_event(self, event: RuntimeEvent) -> None:
        self.entries.append(event.kind.name)


JOURNAL = Journal()


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


class Recorder:
    """A logger keeping each error line it is given, with its fields."""

    def __init__(self) -> None:
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def bind(self, **fields: Any) -> Recorder:
        return self

    def debug(self, event: str, **fields: Any) -> None: ...

    def info(self, event: str, **fields: Any) -> None: ...

    def warning(self, event: str, **fields: Any) -> None: ...

    def error(self, event: str, **fields: Any) -> None:
        self.errors.append((event, fields))

    def exception(self, event: str, **fields: Any) -> None:
        self.errors.append((event, fields))


def serves_session(container: LoomContainer) -> None:
    """Registers :class:`AcmeSession` per execution."""
    container.register(AcmeSession, AcmeSession, scope=Scope.REQUEST)


def factory() -> UseCaseFactory:
    """A factory whose container serves :class:`AcmeSession` per execution."""
    container = LoomContainer(classify_key=classify_key)
    use_case_kinds(container.kinds)
    container.kinds.bind(CALLER, CallerResolver())
    serves_session(container)
    built = UseCaseFactory(container)
    for use_case in (AcmeProbe, AcmeProbeJob, AcmeBroken, AcmeBrokenJob):
        built.register(use_case)
    return built


def executor(
    *, builder: Callable[[type[Any]], Any] | None = None, logger: Recorder | None = None
) -> RuntimeExecutor:
    """An executor reporting its events to :data:`JOURNAL`, building with *builder* if given."""
    return RuntimeExecutor(UseCaseCompiler(), metrics=JOURNAL, builder=builder, logger=logger)
