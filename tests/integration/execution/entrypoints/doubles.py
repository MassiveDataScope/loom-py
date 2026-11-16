"""Doubles shared by the entry point tests: what each entry point builds, and what it saw."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass
from typing import Any

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope as DiScope
from loom.core.errors import Unauthenticated
from loom.core.execution import ExecutionRequired, current_execution, execution_identity
from loom.core.identity import (
    ANONYMOUS,
    Identity,
    current_caller,
    current_identity,
)
from loom.core.job.callback import JobCallback
from loom.core.job.job import Job
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.kinds import declare_kinds
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute
from tests.helpers.delegation import acting_as

ALICE = Identity(subject="acme-alice", mechanism="test")
BOB = Identity(subject="acme-bob", mechanism="test")

CALLERS = pytest.mark.parametrize(
    ("caller", "subject"),
    [(ALICE, ALICE.subject), (None, ANONYMOUS.subject)],
    ids=["authenticated", "anonymous"],
)
"""Each request authenticated as alice, and anonymous; ``subject`` is who it runs as."""

Asgi = Callable[
    [MutableMapping[str, Any], Callable[[], Awaitable[Any]], Callable[[Any], Awaitable[None]]],
    Awaitable[None],
]


# ---------------------------------------------------------------------------
# Use cases and jobs: built with a per-execution adapter declaring Caller()
# ---------------------------------------------------------------------------


class AcmeSession:
    """A per-execution adapter whose constructor declares the caller of its execution."""

    def __init__(self, caller: Identity = Caller()) -> None:
        self.caller = caller
        self.execution = current_execution()


class AcmeProbe(UseCase[Any, "AcmeProbe"]):
    """Keeps the session it was built with and the execution it ran in."""

    def __init__(self, session: AcmeSession) -> None:
        self.session = session
        self.ran_in: object = None

    async def execute(self) -> AcmeProbe:
        self.ran_in = current_execution()
        return self


class AcmeProbeJob(Job["AcmeProbeJob"]):
    """The job counterpart of :class:`AcmeProbe`."""

    def __init__(self, session: AcmeSession) -> None:
        self.session = session
        self.ran_in: object = None

    async def execute(self) -> AcmeProbeJob:
        self.ran_in = current_execution()
        return self


class AcmeRun(UseCase[Any, Any]):
    """Runs the body it is given, as the execution's business code."""

    def __init__(self, body: Callable[[], Awaitable[Any]]) -> None:
        self._body = body

    async def execute(self) -> Any:
        return await self._body()


class AcmeFailingJob(Job[None]):
    async def execute(self) -> None:
        raise LookupError("acme job failed")


class AcmeRouted(UseCase[Any, dict[str, Any]]):
    """Answers who its constructor's session and ``execute()``'s ``Caller()`` saw."""

    def __init__(self, session: AcmeSession) -> None:
        self.session = session

    async def execute(self, caller: Identity = Caller()) -> dict[str, Any]:
        return {
            "built_as": self.session.caller.subject,
            "runs_as": caller.subject,
            "same_execution": self.session.execution is current_execution() is not None,
            "ambient": current_identity().subject,
        }


class AcmeBuiltAs(UseCase[Any, dict[str, Any]]):
    """Declares ``Caller()`` in its own constructor, with no adapter in between."""

    def __init__(self, caller: Identity = Caller()) -> None:
        self.caller = caller
        self.built_in = current_execution()

    async def execute(self) -> dict[str, Any]:
        return {
            "built_as": self.caller.subject,
            "same_execution": self.built_in is current_execution() is not None,
        }


class AcmeGreeter:
    """A port the container injects into a constructor."""

    def greet(self, subject: str) -> str:
        return f"hello {subject}"


class AcmeGreeting(UseCase[Any, dict[str, str]]):
    """Its constructor takes a port and ``Caller()``; ``execute()`` takes ``Caller()`` too."""

    def __init__(self, greeter: AcmeGreeter, caller: Identity = Caller()) -> None:
        self.greeter = greeter
        self.caller = caller

    async def execute(self, caller: Identity = Caller()) -> dict[str, str]:
        return {
            "greeting": self.greeter.greet(self.caller.subject),
            "runs_as": caller.subject,
            "ambient": current_identity().subject,
        }


class AcmeRoutes(RestInterface[dict[str, Any]]):
    prefix = "/acme"
    routes = (RestRoute(use_case=AcmeRouted, method="GET", path="/"),)


# ---------------------------------------------------------------------------
# Job callbacks: what they saw while built and while called
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Seen:
    """Whether code ran in an execution, who acted, and what a handle got there."""

    in_execution: bool
    caller: Identity | None
    ambient: Identity
    handle: str


def seen_now() -> Seen:
    """What a handle reading the execution identity at call time (D2) would get here."""
    execution = current_execution()
    try:
        handle = execution_identity().subject
    except (ExecutionRequired, Unauthenticated) as exc:
        handle = type(exc).__name__
    return Seen(execution is not None, current_caller(), current_identity(), handle)


AS_ALICE = Seen(in_execution=True, caller=ALICE, ambient=ALICE, handle=ALICE.subject)
"""What a job callback alice's dispatch caused sees, built and called (D7 revised, T09a)."""

NO_CALLER = Seen(in_execution=True, caller=None, ambient=ANONYMOUS, handle="Unauthenticated")
"""What a job callback whose dispatch had no caller sees: an execution acting as no one."""


class CallbackLog(list[tuple[str, Seen]]):
    """Each moment a callback recorded (``built``, ``on_success``, ``on_failure``)."""


class AcmeCallback(JobCallback):
    """A job callback built by the factory, recording what it saw when built and called."""

    def __init__(self, log: CallbackLog) -> None:
        self.log = log
        log.append(("built", seen_now()))

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        self.log.append(("on_success", seen_now()))

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        self.log.append(("on_failure", seen_now()))


class AcmeAsyncCallback(AcmeCallback):
    async def on_success(self, job_id: str, result: Any, **context: Any) -> None:  # type: ignore[override]
        self.log.append(("on_success", seen_now()))

    async def on_failure(  # type: ignore[override]
        self, job_id: str, exc_type: str, exc_msg: str, **context: Any
    ) -> None:
        self.log.append(("on_failure", seen_now()))


class AcmeSuccessOnlyCallback(JobCallback):
    """A job callback overriding ``on_success`` alone: ``on_failure`` is the base's no-op."""

    def __init__(self, log: CallbackLog) -> None:
        self.log = log
        log.append(("built", seen_now()))

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        self.log.append(("on_success", seen_now()))


CALLBACKS = pytest.mark.parametrize(
    "callback", [AcmeCallback, AcmeAsyncCallback], ids=["sync", "async"]
)
"""Each test runs with a sync and an async callback."""


class AcmeCallerCallback(JobCallback):
    """A job callback whose constructor declares ``Caller()``: its dispatcher's (D7)."""

    def __init__(self, log: CallbackLog, caller: Identity = Caller()) -> None:
        log.append(("caller", seen_now()))
        self.caller = caller

    def on_success(self, job_id: str, result: Any, **context: Any) -> None: ...

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None: ...


class AcmeRecordingFactory:
    """A factory keeping each job it builds, for entry points that return nothing."""

    def __init__(self, factory: UseCaseFactory, built: list[AcmeProbeJob]) -> None:
        self._factory = factory
        self._built = built

    def build(self, job_type: type[AcmeProbeJob]) -> AcmeProbeJob:
        job = self._factory.build(job_type)
        self._built.append(job)
        return job


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


def probe_factory(log: CallbackLog) -> UseCaseFactory:
    """A factory serving :class:`AcmeSession` per execution and ``log`` to callbacks."""
    container = LoomContainer(classify_key=classify_key, implicit=main_repository)
    declare_kinds(container.kinds)
    container.register(AcmeSession, AcmeSession, scope=DiScope.REQUEST)
    container.register(AcmeGreeter, AcmeGreeter, scope=DiScope.APPLICATION)
    container.register_instance(CallbackLog, log)
    factory = UseCaseFactory(container)
    for built in (AcmeProbe, AcmeProbeJob, AcmeFailingJob, AcmeRouted, AcmeGreeting):
        factory.register(built)
    return factory


class AcmeAuthenticator:
    """Authenticates every request as the identity it is given."""

    name = "acme"
    provides_roles = False

    def __init__(self, identity: Identity) -> None:
        self._identity = identity

    async def authenticate(self, credentials: object) -> Identity:
        return self._identity


def as_caller(app: Asgi, identity: Identity | None) -> Asgi:
    """``app`` behind a transport that authenticated each request as ``identity``, if any."""

    async def authenticated(
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[Any]],
        send: Callable[[Any], Awaitable[None]],
    ) -> None:
        if identity is None:
            await app(scope, receive, send)
            return
        with acting_as(identity):
            await app(scope, receive, send)

    return authenticated
