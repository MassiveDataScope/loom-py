"""Event triggers: the job a function declares runs for each event, as the identity it declares."""

from __future__ import annotations

import socket
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig, compose_application
from loom.core.command import Command
from loom.core.config import ConfigContext, ConfigError
from loom.core.errors import LoomError, MalformedJobMessage
from loom.core.identity import Identity, current_identity
from loom.core.job.job import Job
from loom.core.model import BoundaryValidationError
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case import Input
from loom.rest.auth import RequestCredentials
from loom.serverless.aws.lambda_ import LambdaAdapter, S3Object
from loom.serverless.aws.lambda_.triggers import eventbridge, s3, schedule, sns
from loom.testing import LambdaEvent
from tests.helpers.composition import composition_source

pytestmark = pytest.mark.timeout(30)

_FUNCTION = "invoices_in"
_SENTINEL = "payload-sentinel-4f2a"
_SEVEN = {"number": "7"}
_IDENTITY = {"subject": "svc:invoices", "roles": ["invoice_importer"], "attributes": {"t": "acme"}}


class Invoice(Command, frozen=True, kw_only=True):
    number: str
    note: str = ""


class _Final(LoomError):
    retryable = False


_RAN: list[tuple[Any, Identity]] = []


class ImportInvoice(Job[None]):
    __retries__ = 0

    def execute(self, invoice: Invoice = Input()) -> None:
        _RAN.append((invoice, current_identity()))
        if invoice.number == "fail":
            raise ValueError("job failed")
        if invoice.number == "final":
            raise _Final("job failed finally", code="final")


class ImportObject(Job[None]):
    __retries__ = 0

    def execute(self, found: S3Object = Input()) -> None:
        _RAN.append((found, current_identity()))
        if found.key.startswith("fail"):
            raise ValueError("job failed")


@pytest.fixture
def ran() -> Iterator[list[tuple[Any, Identity]]]:
    _RAN.clear()
    yield _RAN
    _RAN.clear()


class _Observer:
    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


Serve = Callable[..., Any]


@pytest.fixture
def serve(tmp_path: Path) -> Iterator[Serve]:
    """Compose the function *name* of the given *trigger* and bind its handler."""
    adapters: list[LambdaAdapter] = []

    def compose(
        trigger: dict[str, Any],
        *,
        name: str = _FUNCTION,
        observer: _Observer | None = None,
        written: dict[str, Any] | None = None,
        **options: Any,
    ) -> Any:
        function = {"trigger": trigger} if written is None else written
        document = {"lambda": {"functions": {name: function}}}
        source = composition_source(
            ConfigContext.from_dict(document),
            app=AppConfig(name="events", job_service="inline", entry=name),
            code_path=tmp_path,
            config_root=tmp_path,
        )
        adapter = LambdaAdapter(jobs=[ImportInvoice, ImportObject], **options)
        application = compose_application(source, adapter)
        if observer is not None:
            application.observability = ObservabilityRuntime([observer])
        adapters.append(adapter)
        return adapter.bind(application)

    yield compose
    for adapter in adapters:
        adapter.close()


_QUEUE = {"name": "events"}


def _events(job: str = "ImportInvoice", **trigger: Any) -> dict[str, Any]:
    return {"source": "eventbridge", "job": job, **trigger}


class TestDecoders:
    @pytest.mark.parametrize(
        ("decode", "event", "event_id"),
        [
            (eventbridge.decode, LambdaEvent.eventbridge(_SEVEN, event_id="e1"), "e1"),
            (schedule.decode, LambdaEvent.schedule(_SEVEN), None),
            (sns.decode, LambdaEvent.sns(_SEVEN, notification_id="n1"), "n1"),
        ],
        ids=["eventbridge-detail", "schedule-input", "sns-message"],
    )
    def test_an_event_runs_its_payload_with_its_id(
        self, decode: Callable[[Any], Any], event: dict[str, Any], event_id: str | None
    ) -> None:
        (decoded,) = decode(event)

        assert (decoded.payload, decoded.event_id) == (_SEVEN, event_id)

    def test_an_sns_message_that_is_no_object_is_malformed(self) -> None:
        event = LambdaEvent.sns()
        event["Records"][0]["Sns"]["Message"] = '["a list"]'

        with pytest.raises(MalformedJobMessage, match="SNS Message is not a JSON object"):
            sns.decode(event)

    def test_an_sns_subscription_confirmation_is_refused_and_nothing_fetched(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        connected: list[object] = []
        monkeypatch.setattr(socket.socket, "connect", lambda _, address: connected.append(address))
        event = LambdaEvent.sns(type="SubscriptionConfirmation")
        event["Records"][0]["Sns"]["SubscribeURL"] = "http://169.254.169.254/latest/meta-data"

        with pytest.raises(MalformedJobMessage, match="not a Notification"):
            sns.decode(event)
        assert connected == []

    def test_s3_runs_one_job_per_object_its_key_decoded(self) -> None:
        objects = ["reports/a b+c.csv", {"key": "ünï/x.csv", "size": 5, "version_id": "v2"}]
        event = LambdaEvent.s3(objects, bucket="inbox")

        decoded = s3.decode(event)

        assert [item.payload["key"] for item in decoded] == ["reports/a b+c.csv", "ünï/x.csv"]
        assert decoded[0].payload["bucket"] == "inbox"
        assert decoded[1].payload["size"] == 5
        assert decoded[1].payload["version_id"] == "v2"
        assert decoded[0].payload["event_name"] == "ObjectCreated:Put"

    @pytest.mark.parametrize(
        ("decode", "event"),
        [
            (eventbridge.decode, {"id": "1", "detail": "text"}),
            (schedule.decode, ["a list"]),
            (sns.decode, {"Records": "none"}),
            (s3.decode, {"Records": [{"s3": {"bucket": {}, "object": {"key": "k"}}}]}),
        ],
        ids=["eventbridge", "schedule", "sns", "s3"],
    )
    def test_a_malformed_event_raises_the_typed_error(
        self, decode: Callable[[Any], Any], event: Any
    ) -> None:
        with pytest.raises(MalformedJobMessage):
            decode(event)


class TestDirectTrigger:
    def test_the_job_runs_as_the_declared_identity_never_the_events(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        handler = serve(_events(identity=_IDENTITY))
        event = LambdaEvent.eventbridge({"number": "7"})
        event["identity"] = {"subject": "mallory", "roles": ["admin"]}

        assert handler(event, None) is None

        ((invoice, caller),) = ran
        assert invoice == Invoice(number="7")
        assert (caller.subject, caller.roles, caller.attributes) == (
            "svc:invoices",
            ("invoice_importer",),
            {"t": "acme"},
        )
        assert caller.mechanism == f"lambda:{_FUNCTION}:eventbridge"

    def test_without_an_identity_the_job_runs_anonymous(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        serve(_events())(LambdaEvent.eventbridge({"number": "7"}), None)

        ((_, caller),) = ran
        assert not caller.is_authenticated

    def test_the_span_id_is_the_event_id(self, serve: Serve) -> None:
        observer = _Observer()
        handler = serve(_events(), observer=observer)

        handler(LambdaEvent.eventbridge({"number": "7"}, event_id="evt-9"), None)

        starts = [e for e in observer.events if (e.scope, e.kind) == (Scope.JOB, EventKind.START)]
        assert [(e.name, e.id) for e in starts] == [("ImportInvoice", "evt-9")]

    @pytest.mark.parametrize(
        ("payload", "error"),
        [
            ({"number": "fail"}, ValueError),
            ({"number": "final"}, _Final),
            ({"number": 5}, BoundaryValidationError),
            ({"note": "no number"}, BoundaryValidationError),
        ],
        ids=["retryable", "not-retryable", "validation", "missing-field"],
    )
    def test_any_failed_job_fails_the_invocation(
        self, serve: Serve, payload: dict[str, Any], error: type[Exception]
    ) -> None:
        handler = serve(_events())

        with pytest.raises(error):
            handler(LambdaEvent.eventbridge(payload), None)

    def test_a_malformed_event_fails_the_invocation_in_a_failed_span(self, serve: Serve) -> None:
        observer = _Observer()
        handler = serve(_events(), observer=observer)

        with pytest.raises(MalformedJobMessage):
            handler({"id": "1", "detail": "text"}, None)
        assert [(e.scope, e.kind) for e in observer.events] == [
            (Scope.JOB, EventKind.START),
            (Scope.JOB, EventKind.ERROR),
        ]

    @pytest.mark.parametrize(
        "payload",
        [{"number": 5, "note": _SENTINEL}, {"number": "fail", "note": _SENTINEL}],
        ids=["validation", "job-raises"],
    )
    def test_a_failure_never_logs_the_payload(
        self,
        serve: Serve,
        payload: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        handler = serve(_events())

        with pytest.raises(Exception):  # noqa: B017 - any failure; the logs are what is checked
            handler(LambdaEvent.eventbridge(payload), None)

        captured = capsys.readouterr()
        assert any("Lambda event job failed" in message for message in caplog.messages)
        assert _SENTINEL not in caplog.text + captured.out + captured.err

    def test_an_s3_event_stops_at_its_first_failed_object(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        handler = serve({"source": "s3", "job": "ImportObject"})
        event = LambdaEvent.s3(["ok/1.csv", "fail/2.csv", "ok/3.csv"])

        with pytest.raises(ValueError, match="job failed"):
            handler(event, None)

        assert [found.key for found, _ in ran] == ["ok/1.csv", "fail/2.csv"]

    @pytest.mark.parametrize("missing", ["eventName", "eventTime"])
    def test_an_s3_record_without_its_event_name_or_time_fails_in_a_failed_span(
        self, serve: Serve, ran: list[tuple[Any, Identity]], missing: str
    ) -> None:
        observer = _Observer()
        handler = serve({"source": "s3", "job": "ImportObject"}, observer=observer)
        event = LambdaEvent.s3(["in/a.csv"])
        del event["Records"][0][missing]

        with pytest.raises(MalformedJobMessage, match=f"names no {missing}"):
            handler(event, None)
        assert ran == []
        assert [(e.scope, e.kind) for e in observer.events] == [
            (Scope.JOB, EventKind.START),
            (Scope.JOB, EventKind.ERROR),
        ]

    def test_an_s3_object_binds_the_typed_command(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        handler = serve({"source": "s3", "job": "ImportObject"})

        handler(LambdaEvent.s3([{"key": "in/a+b.csv", "size": 3}]), None)

        ((found, _),) = ran
        assert isinstance(found, S3Object)
        assert (found.bucket, found.key, found.size) == ("loom-test", "in/a+b.csv", 3)
        assert found.event_time.year == 2026

    def test_the_s3_test_event_runs_no_job(
        self, serve: Serve, ran: list[tuple[Any, Identity]], caplog: pytest.LogCaptureFixture
    ) -> None:
        handler = serve({"source": "s3", "job": "ImportObject"})

        assert handler(LambdaEvent.s3_test_event(), None) is None
        assert ran == []
        assert any("S3 test event acknowledged" in message for message in caplog.messages)

    @pytest.mark.parametrize(
        ("source", "event"),
        [
            ("sns", LambdaEvent.sns({"number": "7"})),
            ("schedule", LambdaEvent.schedule({"number": "7"})),
        ],
        ids=["sns", "schedule"],
    )
    def test_sns_and_schedule_run_their_payload(
        self, serve: Serve, ran: list[tuple[Any, Identity]], source: str, event: LambdaEvent
    ) -> None:
        handler = serve({"source": source, "job": "ImportInvoice"})

        handler(event, None)

        assert [invoice for invoice, _ in ran] == [Invoice(number="7")]


class TestQueued:
    def test_a_failed_message_is_reported_alone(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        handler = serve(_events(queue=_QUEUE, identity=_IDENTITY))
        records = [
            *LambdaEvent.eventbridge({"number": "1"}).via_sqs(message_id="m1")["Records"],
            *LambdaEvent.eventbridge({"number": "fail"}).via_sqs(message_id="m2")["Records"],
        ]

        result = handler({"Records": records}, None)

        assert result == {"batchItemFailures": [{"itemIdentifier": "m2"}]}
        assert [invoice.number for invoice, _ in ran] == ["1", "fail"]
        assert {caller.mechanism for _, caller in ran} == {f"lambda:{_FUNCTION}:eventbridge"}

    def test_a_final_failure_is_reported_too_so_it_is_dead_lettered(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        handler = serve(_events(queue=_QUEUE))
        event = LambdaEvent.eventbridge({"number": "final"}).via_sqs(message_id="m1")

        result = handler(event, None)

        assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
        assert [invoice.number for invoice, _ in ran] == ["final"]

    def test_an_sns_message_runs_from_its_envelope(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        handler = serve({"source": "sns", "queue": _QUEUE, "job": "ImportInvoice"})

        result = handler(LambdaEvent.sns({"number": "7"}).via_sqs(), None)

        assert (result, [invoice for invoice, _ in ran]) == (
            {"batchItemFailures": []},
            [Invoice(number="7")],
        )

    def test_an_sns_message_delivered_raw_is_malformed(
        self, serve: Serve, ran: list[tuple[Any, Identity]]
    ) -> None:
        handler = serve({"source": "sns", "queue": _QUEUE, "job": "ImportInvoice"})

        event = LambdaEvent.sqs_record('{"number": "7"}', message_id="r")

        result = handler(event, None)

        assert (result, ran) == ({"batchItemFailures": [{"itemIdentifier": "r"}]}, [])


def test_the_same_job_binds_the_same_command_from_a_queue_and_an_event(
    serve: Serve, ran: list[tuple[Any, Identity]]
) -> None:
    payload = {"number": "7", "note": "parity"}

    serve({"source": "jobs"}, name="worker")(LambdaEvent.jobs(ImportInvoice, payload), None)
    serve(_events())(LambdaEvent.eventbridge(payload), None)

    (from_queue, _), (from_event, _) = ran
    assert from_queue == from_event == Invoice(number="7", note="parity")


class _Authenticator:
    name = "test"
    provides_roles = True

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        return None


def test_startup_refuses_a_trigger_job_it_cannot_run(serve: Serve) -> None:
    with pytest.raises(ConfigError, match="'Missing', which is no discovered Job"):
        serve(_events("Missing"))


def test_startup_refuses_an_authenticator_for_an_event_trigger(serve: Serve) -> None:
    with pytest.raises(ConfigError, match="applies to an http function only"):
        serve(_events(), authenticator=_Authenticator())
