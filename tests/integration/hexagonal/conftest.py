"""Compose acme shipping on each transport, its agent and MCP server answered by stubs.

The stub resolvers implement :class:`~loom.core.execution.ExecutionResolver`.
Every handle they return reads who it acts as on each call the way loom's own
handles do (:func:`~loom.ai.errors.require_authenticated`) and writes it down.
The transport's own adapter composes the application in the testing mode of
its recipe (the mode :class:`~loom.testing.HttpTestHarness` composes in), the
one where resolvers other than the agent runtime's are bound; the production
mode is composed by ``compose_strictly``, and refuses.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from loom.ai.abc import AgentAnswer, AgentUsage
from loom.ai.errors import require_authenticated
from loom.celery.runtime import CeleryRuntime
from loom.composition import Application, Project, Recipe, Runtime
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.identity import ANONYMOUS, Delegation, Identity
from loom.core.job.envelope import encode_job_message
from loom.core.use_case.kinds import AGENT, MCP
from loom.rest.auth import RequestCredentials
from loom.rest.fastapi import FastAPIRuntime
from loom.serverless.aws.lambda_ import LambdaRuntime
from tests.helpers.composition import Serving, WithOverrides, composition_project
from tests.integration.hexagonal.adapters import gateway_root
from tests.integration.hexagonal.shipping import TrackShipment, TrackShipmentJob

SHIPPING = "tests.integration.hexagonal.shipping"
API_FUNCTION = "acme-api"
WORKER_FUNCTION = "acme-worker"
KEY_HEADER = "x-acme-key"
ANA = Identity(subject="acme-ana", mechanism="acme-key")
BO = Identity(
    subject="acme-bo", roles=("dispatcher",), attributes={"depot": "north"}, mechanism="acme-key"
)
KEYS = {"key-ana": ANA, "key-bo": BO, "key-guest": ANONYMOUS}
STATUS = "in transit"
PATH_TO_GATEWAY = "TrackShipment -> queries: ShipmentQueries: gateway"
CALLERS = pytest.mark.parametrize("caller", [ANA, BO], ids=["ana", "bo"])
RENDEZVOUS_SECONDS = 2
PARAMS = {"shipment_id": "ACME-1"}
BAD_IDENTITIES: dict[str, object] = {
    "empty-subject": {"subject": ""},
    "not-a-mapping": "ana",
    "subject-not-a-string": {"subject": 1},
    "expired": Delegation(ANA, 0.0).encoded(),
    "unreadable-until": {**(Delegation(ANA, None).encoded() or {}), "until": "x"},
}
"""Identity fields a queued job may carry that name no caller it may act as."""

Module = Callable[[LoomContainer], None]
Call = tuple[str, Identity]


class AcmeKeys:
    """Authenticates a request by its exact ``x-acme-key`` header."""

    name = "acme-key"
    provides_roles = True

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        return KEYS.get(credentials.header(KEY_HEADER) or "")


def key_of(identity: Identity) -> dict[str, str]:
    """The header that authenticates *identity*."""
    return {KEY_HEADER: next(key for key, known in KEYS.items() if known == identity)}


def notice_for(identity: Identity, shipment_id: str = "ACME-1") -> str:
    """The notice the stub agent words for *identity* about *shipment_id*."""
    return f"{identity.subject}: Tell the customer that {shipment_id} is {STATUS}."


def report_for(identity: Identity, shipment_id: str = "ACME-1") -> dict[str, str]:
    """The report ``TrackShipment`` returns to *identity* for *shipment_id*."""
    notice = notice_for(identity, shipment_id)
    return {"shipment_id": shipment_id, "status": STATUS, "notice": notice}


def acted_as(*identities: Identity) -> list[Call]:
    """What one ``TrackShipment`` run as each of *identities* asks the stub handles, in order."""
    return [call for who in identities for call in (("mcp:track_shipment", who), ("agent", who))]


def job_body(identity: object) -> str:
    """The SQS body of a ``TrackShipmentJob`` envelope whose identity field is *identity*."""
    envelope = json.loads(
        encode_job_message(TrackShipmentJob, payload=None, params=PARAMS, identity=None)
    )
    return json.dumps({**envelope, "identity": identity})


@dataclasses.dataclass
class Seen:
    """Who each stub handle acted as, call by call, as ``(handle, identity)``.

    With a ``rendezvous``, each MCP call waits there before it reads who it
    acts as, so the executions it gathers are all in flight at that moment.
    """

    calls: list[Call] = dataclasses.field(default_factory=list)
    rendezvous: asyncio.Barrier | None = None

    def record(self, handle: str) -> Identity:
        caller = require_authenticated(handle)
        self.calls.append((handle, caller))
        return caller

    async def meet(self) -> None:
        if self.rendezvous is not None:
            async with asyncio.timeout(RENDEZVOUS_SECONDS):
                await self.rendezvous.wait()


class TrackingHandle:
    """The tracking server: answers ``track_shipment`` with :data:`STATUS`."""

    def __init__(self, seen: Seen) -> None:
        self._seen = seen

    async def call_untyped(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        await self._seen.meet()
        self._seen.record(f"mcp:{tool}")
        return {"shipment_id": arguments["shipment_id"], "status": STATUS}


class WriterHandle:
    """The notice writer: answers every prompt with a notice addressed to the caller."""

    def __init__(self, seen: Seen) -> None:
        self._seen = seen

    async def run(self, prompt: str) -> AgentAnswer[str]:
        caller = self._seen.record("agent")
        usage = AgentUsage(input_tokens=0, output_tokens=0, requests=1, duration_ms=0)
        return AgentAnswer(output=f"{caller.subject}: {prompt}", usage=usage)


class StubResolver:
    """Resolves every declaration of its kind to one handle, keeping the declarations."""

    def __init__(self, handle: object) -> None:
        self._handle = handle
        self.declarations: list[Any] = []

    def resolve(self, declaration: object, annotation: object) -> object:
        self.declarations.append(declaration)
        return self._handle


@dataclasses.dataclass
class Stubs:
    """The agent and MCP resolvers, and the module binding them."""

    agent: StubResolver
    mcp: StubResolver

    def register(self, container: LoomContainer) -> None:
        container.kinds.register(AGENT, self.agent)
        container.kinds.register(MCP, self.mcp)


class Stubbed:
    """*transport*'s own adapter, composing in testing mode with the stub resolvers bound."""

    def __init__(self, transport: Runtime[Any], stubs: Stubs, root: Module) -> None:
        self._transport = transport
        self._stubs = stubs
        self._root = root

    def recipe(self, project: Project) -> Recipe:
        recipe = self._transport.recipe(project)
        return dataclasses.replace(
            recipe,
            agents=False,
            lenient=True,
            overrides=(*recipe.overrides, self._root, self._stubs.register),
        )

    def mount(self, application: Application) -> Any:
        return self._transport.mount(application)


def acme_config() -> dict[str, Any]:
    """One route, an HTTP and a jobs Lambda function, an eager worker, no database."""
    route = {"use_case": f"{SHIPPING}:TrackShipment", "method": "GET", "path": "/{shipment_id}"}
    api = {"source": "http", "interfaces": ["shipments"], "auth": {"custom": True}}
    return {
        "app": {
            "name": "acme-shipping",
            "job_service": "inline",
            "packages": [SHIPPING],
            "rest": {
                "docs_url": None,
                "redoc_url": None,
                "openapi_url": None,
                "auth": {"custom": True},
                "interfaces": {"shipments": {"prefix": "/shipments", "routes": [route]}},
            },
        },
        "stores": {"default": {"backend": "none"}},
        "aws": {"region": "us-east-1"},
        "sqs": {
            "queues": {
                "jobs": {
                    "url": "https://sqs.us-east-1.amazonaws.com/123456789012/loom-jobs",
                    "dead_letter": "discard",
                }
            }
        },
        "lambda": {
            "functions": {
                API_FUNCTION: {"trigger": api},
                WORKER_FUNCTION: {"trigger": {"source": "jobs", "queue": {"name": "jobs"}}},
            }
        },
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
    }


TRANSPORTS: dict[str, Callable[[], Runtime[Any]]] = {
    "fastapi": lambda: FastAPIRuntime(authenticator=AcmeKeys()),
    "lambda-http": lambda: LambdaRuntime(function=API_FUNCTION, authenticator=AcmeKeys()),
    "lambda-jobs": lambda: LambdaRuntime(function=WORKER_FUNCTION, jobs=(TrackShipmentJob,)),
    "celery": lambda: CeleryRuntime(jobs=(TrackShipmentJob,), use_cases=(TrackShipment,)),
    "inline-jobs": lambda: Serving(use_cases=(TrackShipment,), jobs=(TrackShipmentJob,)),
}
"""Each entry point's own adapter, as acme builds it; ``inline-jobs`` mounts no transport."""


@pytest.fixture
def project() -> Project:
    """Acme shipping's configuration, held in memory."""
    return composition_project(ConfigContext.from_dict(acme_config()))


@pytest.fixture
def seen() -> Seen:
    """Who the stub handles acted as."""
    return Seen()


@pytest.fixture
def stubs(seen: Seen) -> Stubs:
    """The stub agent and MCP resolvers."""
    return Stubs(agent=StubResolver(WriterHandle(seen)), mcp=StubResolver(TrackingHandle(seen)))


Compose = Callable[..., tuple[Application, Any]]


@pytest.fixture
def compose(project: Project, stubs: Stubs) -> Iterator[Compose]:
    """Compose acme shipping on *transport* with *root*; return the application and its entry.

    A Lambda function's handler is closed when the test ends.
    """
    opened: list[LambdaRuntime] = []

    def build(transport: str, root: Module = gateway_root) -> tuple[Application, Any]:
        adapter = TRANSPORTS[transport]()
        if isinstance(adapter, LambdaRuntime):
            opened.append(adapter)
        stubbed = Stubbed(adapter, stubs, root)
        application = project.compose(stubbed)
        return application, stubbed.mount(application)

    yield build
    for adapter in opened:
        adapter.close()


@pytest.fixture
def compose_strictly(project: Project) -> Callable[..., Application]:
    """Compose acme shipping on *transport* with *root* as production does, with no stub."""

    def build(transport: str, root: Module = gateway_root) -> Application:
        return project.compose(WithOverrides(TRANSPORTS[transport](), root))

    return build
