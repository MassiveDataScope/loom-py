"""The behavioural canary of handoffs: what leaves the process never carries a failure's text.

Every runtime adapter lists its handoffs as a :class:`~loom.core.logger.failure.Handoff`
subclass. Each member has a driver here that makes the adapter hand off
:func:`~tests.helpers.canary.canary_failure` through its real code path and
returns everything the foreign side received, as text. A member with no driver
fails :meth:`TestHandoffList.test_every_handoff_has_a_driver`.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import traceback
from collections.abc import AsyncIterator, Callable, MutableMapping
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest
from celery.exceptions import Retry
from prefect.states import exception_to_failed_state
from starlette.applications import Starlette
from typer.testing import CliRunner

from loom.ai._transport import failure_event
from loom.ai.a2a.events import A2AEventProjector
from loom.ai.abc import AiHandoff
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.fastapi.endpoints import _run_error_response
from loom.ai.fastapi.streaming import encode_sse_event
from loom.celery.runner import CeleryHandoff, _CeleryProcessBridge
from loom.celery.runtime import _WorkerLifecycle
from loom.cli import CliHandoff
from loom.cli.app import app as cli_app
from loom.core.errors import Conflict
from loom.core.job.job import Job
from loom.core.job.policy import Backoff
from loom.core.logger.failure import FailureReference, Handoff
from loom.core.observability.observability import Observability
from loom.prefect._config import flow_config_from_mapping
from loom.prefect.flow._assemble import FlowSettings, PrefectHandoff, assemble_flow
from loom.prefect.flow._hooks import _event_from_run
from loom.prefect.notify._slack import SlackNotifier
from loom.prefect.notify._sns import _render_message as _render_sns_message
from loom.rest.errors import HttpErrorMapper
from loom.rest.fastapi.runtime import RestHandoff, application_lifespan
from loom.serverless.aws.lambda_.boundary import LambdaBoundary, LambdaHandoff, lambda_init
from loom.streaming.core._errors import StreamingHandoff
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import try_decode_record
from tests.helpers.canary import CANARY, raise_canary_failure
from tests.helpers.jobs import CeleryWorker
from tests.helpers.source_tree import parsed, source_files
from tests.unit.streaming.doubles import AcmePayload


def _raised_text(call: Callable[[], object]) -> str:
    """Everything a runtime that catches what *call* raises can write: type, text, traceback."""
    try:
        call()
    except BaseException as caught:  # noqa: BLE001 - the canary reads whatever leaves
        return "".join(traceback.format_exception(caught)) + repr(caught) + repr(caught.args)
    pytest.fail("the handoff raised nothing")


def _lambda_init() -> str:
    return _raised_text(lambda: lambda_init(raise_canary_failure))


def _lambda_invoke() -> str:
    return _raised_text(lambda: LambdaBoundary(raise_canary_failure)({}, None))  # type: ignore[arg-type]


class _Application:
    """Stands in for an application whose resources fail to open or to close."""

    def __init__(self, *, fails_on_close: bool) -> None:
        self._fails_on_close = fails_on_close

    @asynccontextmanager
    async def running(self) -> AsyncIterator[None]:
        if not self._fails_on_close:
            raise_canary_failure()
        yield
        raise_canary_failure()


async def _lifespan_messages(*, fails_on_close: bool) -> list[dict[str, Any]]:
    """What the ASGI server receives over the lifespan protocol, as uvicorn logs it."""
    app = Starlette(lifespan=application_lifespan(_Application(fails_on_close=fails_on_close)))  # type: ignore[arg-type]
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    for kind in ("lifespan.startup", "lifespan.shutdown"):
        inbox.put_nowait({"type": kind})
    sent: list[dict[str, Any]] = []

    async def send(message: MutableMapping[str, Any]) -> None:
        sent.append(dict(message))

    with suppress(BaseException):
        await app({"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}, inbox.get, send)
    return sent


def _lifespan(*, fails_on_close: bool) -> str:
    sent = asyncio.run(_lifespan_messages(fails_on_close=fails_on_close))
    failed = [m for m in sent if m["type"].endswith(".failed")]
    assert failed, sent
    return repr(sent)


def _rest_error_body() -> str:
    return repr(HttpErrorMapper().to_http(Conflict(CANARY)).detail)


class _CanaryJob(Job[None]):
    __retry__ = Backoff(first_s=3, factor=3, max_s=300, jitter=False)

    def execute(self) -> None:
        raise_canary_failure()


_LAST_ATTEMPT = 3
_CALLBACK = "loom.callback_error.AcmeCallback"


def _celery(run: Callable[[CeleryWorker], str]) -> str:
    worker = CeleryWorker()
    try:
        return run(worker)
    finally:
        worker.close()


def _celery_result(worker: CeleryWorker) -> str:
    return _raised_text(lambda: worker.run(_CanaryJob, attempt=_LAST_ATTEMPT))


def _celery_retry(worker: CeleryWorker) -> str:
    try:
        worker.run(_CanaryJob, attempt=1)
    except Retry as retried:
        stored = "".join(traceback.format_exception(retried)) + repr(retried.exc)
        return stored + repr(retried.exc.args if retried.exc else ())
    pytest.fail("the job was not retried")


def _celery_failure_callback(worker: CeleryWorker) -> str:
    with suppress(BaseException):
        worker.run(_CanaryJob, attempt=_LAST_ATTEMPT, failure_callback=_CALLBACK)
    (message,) = worker.published(_CALLBACK)
    return repr(message.kwargs)


class _FailingToOpen:
    @asynccontextmanager
    async def running(self, *, check: bool = True) -> AsyncIterator[None]:
        raise_canary_failure()
        yield


def _celery_worker_init() -> str:
    bridge = _CeleryProcessBridge()
    lifecycle = _WorkerLifecycle(_FailingToOpen(), bridge)  # type: ignore[arg-type]
    lifecycle.on_process_init()
    refused = _raised_text(bridge.ensure_open)
    terminated = _raised_text(lambda: lifecycle.on_worker_init(None))
    return refused + terminated


class _CanaryParams(msgspec.Struct):
    pass


def _prefect_failed_state() -> Any:
    """The ``Failed`` state Prefect builds from what a loom-built flow raised."""
    settings = FlowSettings(
        correlation_field=None,
        schedule=None,
        raw_params={},
        pool_config={},
        tags=(),
        notifiers=(),
        retry_policy=flow_config_from_mapping(None),
    )
    flow = assemble_flow(
        name="canary",
        body=raise_canary_failure,
        signature=inspect.Signature(),
        params_type=_CanaryParams,
        settings=settings,
        config_path=__file__,
        source_file=__file__,
    )
    try:
        flow.fn()
    except BaseException as caught:  # noqa: BLE001 - Prefect turns whatever the body raised
        return asyncio.run(exception_to_failed_state(caught))
    pytest.fail("the flow raised nothing")


def _prefect_flow_state() -> str:
    state = _prefect_failed_state()
    return f"{state.message}{state.data!r}"


def _prefect_notification() -> str:
    state = _prefect_failed_state()
    run = SimpleNamespace(name="run", parameters={}, id=None, total_run_time=None)
    event = _event_from_run("canary", run, state, "Failed")
    slack = SlackNotifier(webhook_url="https://hooks.example/x")._render(event)
    return repr(slack) + _render_sns_message(event)


def _streaming_error_envelope() -> str:
    pytest.importorskip("bytewax")
    from loom.streaming.bytewax import _error_boundary  # noqa: PLC0415 - needs bytewax

    envelope = _error_boundary._execute_in_boundary(
        _error_boundary._classify_task,
        Message(payload=AcmePayload(value="v"), meta=MessageMeta(message_id="m-1")),
        raise_canary_failure,
        _error_boundary.ErrorBoundary(observer=Observability.noop(), flow="orders"),
    )
    return repr(envelope)


class _RaisingCodec:
    def decode(self, data: bytes, payload_type: type[Any]) -> Any:
        raise_canary_failure()


def _streaming_wire_error() -> str:
    record = KafkaRecord(topic="orders", key=b"k", value=b"x", partition=0, offset=1)
    return repr(try_decode_record(record, AcmePayload, _RaisingCodec()))  # type: ignore[arg-type]


def _streaming_dlq_header() -> str:
    pytest.importorskip("bytewax")
    from loom.streaming.bytewax import _runtime_io  # noqa: PLC0415 - needs bytewax
    from loom.streaming.kafka._errors import KafkaDeliveryError  # noqa: PLC0415
    from tests.unit.streaming.bytewax.cases import AcmeOrder, build_compiled_sink  # noqa: PLC0415
    from tests.unit.streaming.kafka.doubles import RawProducerStub  # noqa: PLC0415

    raw = RawProducerStub()
    raw.flush_errors = [KafkaDeliveryError(CANARY)]
    original = _runtime_io.KafkaProducerClient
    _runtime_io.KafkaProducerClient = lambda settings: raw  # type: ignore[assignment,misc]
    try:
        sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink(dlq_topic="orders.dlq"))
        sink.write_batch(
            [Message(payload=AcmeOrder(order_id="a"), meta=MessageMeta(message_id="m"))]
        )  # type: ignore[list-item]
    finally:
        _runtime_io.KafkaProducerClient = original  # type: ignore[misc]
    return repr([record.headers for record in raw.sent if record.topic == "orders.dlq"])


def _run_error(code: AgentRunErrorCode) -> AgentRunError:
    return AgentRunError(code, CANARY, interaction_id="i-1")


def _ai_sse_error_event() -> str:
    return encode_sse_event(failure_event(_run_error(AgentRunErrorCode.RUN_TIMEOUT))).decode()


def _ai_http_error_body() -> str:
    bodies = [
        _run_error_response(_run_error(code)).body
        for code in (
            AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED,
            AgentRunErrorCode.PROVIDER_UNAVAILABLE,
        )
    ]
    return repr(bodies)


def _ai_a2a_task_status() -> str:
    projector = A2AEventProjector(task_id="t-1", context_id="c-1", max_steps=3)
    return repr(projector.project(failure_event(_run_error(AgentRunErrorCode.RUN_TIMEOUT))))


def _cli_error_output() -> str:
    result = CliRunner().invoke(cli_app, ["schema", "init", f"Bad-{CANARY}"])
    assert result.exit_code == 1, result.output
    return result.output


DRIVERS: dict[Handoff, Callable[[], str]] = {
    LambdaHandoff.INIT: _lambda_init,
    LambdaHandoff.INVOKE: _lambda_invoke,
    RestHandoff.LIFESPAN_STARTUP: lambda: _lifespan(fails_on_close=False),
    RestHandoff.LIFESPAN_SHUTDOWN: lambda: _lifespan(fails_on_close=True),
    RestHandoff.ERROR_BODY: _rest_error_body,
    CeleryHandoff.RESULT: lambda: _celery(_celery_result),
    CeleryHandoff.RETRY: lambda: _celery(_celery_retry),
    CeleryHandoff.FAILURE_CALLBACK: lambda: _celery(_celery_failure_callback),
    CeleryHandoff.WORKER_INIT: _celery_worker_init,
    PrefectHandoff.FLOW_STATE: _prefect_flow_state,
    PrefectHandoff.NOTIFICATION: _prefect_notification,
    StreamingHandoff.ERROR_ENVELOPE: _streaming_error_envelope,
    StreamingHandoff.WIRE_ERROR: _streaming_wire_error,
    StreamingHandoff.DLQ_HEADER: _streaming_dlq_header,
    AiHandoff.SSE_ERROR_EVENT: _ai_sse_error_event,
    AiHandoff.HTTP_ERROR_BODY: _ai_http_error_body,
    AiHandoff.A2A_TASK_STATUS: _ai_a2a_task_status,
    CliHandoff.ERROR_OUTPUT: _cli_error_output,
}


def _declared_handoff_classes() -> set[str]:
    """The names of every ``Handoff`` subclass ``src/loom`` declares."""
    return {
        node.name
        for path in source_files()
        for node in ast.walk(parsed(path))
        if isinstance(node, ast.ClassDef)
        and any(isinstance(base, ast.Name) and base.id == "Handoff" for base in node.bases)
    }


class TestHandoffList:
    def test_every_declared_handoff_list_is_driven(self) -> None:
        driven = {type(member).__name__ for member in DRIVERS}

        assert _declared_handoff_classes() == driven

    def test_every_handoff_has_a_driver(self) -> None:
        lists: set[type[Handoff]] = {type(member) for member in DRIVERS}
        members = {member for kind in lists for member in kind}

        assert members == set(DRIVERS)


class TestHandoffCanary:
    @pytest.mark.parametrize("handoff", list(DRIVERS), ids=str)
    def test_no_handoff_carries_the_canary(self, handoff: Handoff) -> None:
        received = DRIVERS[handoff]()

        assert received, "the foreign side received the failure"
        assert CANARY not in received

    def test_a_handed_off_reference_has_no_context(self) -> None:
        with pytest.raises(FailureReference) as raised:
            LambdaBoundary(raise_canary_failure)({}, None)  # type: ignore[arg-type]

        assert (raised.value.__context__, raised.value.__cause__) == (None, None)
