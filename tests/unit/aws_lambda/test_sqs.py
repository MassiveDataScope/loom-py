from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from loom.aws_lambda import LambdaSQSAdapter
from loom.aws_lambda.sqs import SQSHandler
from loom.composition import (
    Application,
    CompositionSource,
    CompositionStateError,
    compose_application,
)
from loom.composition.resources import Resource, ResourceKind
from loom.core.checks import REST_CHECKS
from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.job import Job
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.tracing import get_trace_id
from tests.unit.aws_lambda._probe import Probe
from tests.unit.aws_lambda._records import RecordFactory


@pytest.mark.parametrize(
    ("markers", "fifo", "failed", "ran"),
    [
        (["a", "b", "c"], False, [], ["a", "b", "c"]),
        (["a", "fail", "c"], False, ["2"], ["a", "fail", "c"]),
        (["a", "fail", "c"], True, ["2", "3"], ["a", "fail"]),
        (["fail", "fail", "fail"], False, ["1", "2", "3"], ["fail"] * 3),
    ],
)
def test_batch_outcomes(
    handler: SQSHandler,
    record: RecordFactory,
    markers: list[str],
    fifo: bool,
    failed: list[str],
    ran: list[str],
    seen: list[tuple[str, str | None, str | None]],
) -> None:
    records = [record(str(i), marker, fifo=fifo) for i, marker in enumerate(markers, 1)]
    result = handler({"Records": records}, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": item} for item in failed]
    assert [item[0] for item in seen] == ran


def test_malformed_body_fails_only_that_standard_record(
    handler: SQSHandler, record: RecordFactory, seen: list[tuple[str, str | None, str | None]]
) -> None:
    bad = record("1", "a")
    bad["body"] = "{"
    result = handler({"Records": [bad, record("2", "b")]}, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "1"}]
    assert [item[0] for item in seen] == ["b"]


def test_message_group_attribute_does_not_enable_fifo_rule(
    handler: SQSHandler, record: RecordFactory, seen: list[tuple[str, str | None, str | None]]
) -> None:
    first = record("1", "fail")
    first["attributes"] = {"MessageGroupId": "g"}
    result = handler({"Records": [first, record("2", "b")]}, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "1"}]
    assert [item[0] for item in seen] == ["fail", "b"]


def test_each_message_sees_its_own_identity_and_trace(
    handler: SQSHandler, record: RecordFactory, seen: list[tuple[str, str | None, str | None]]
) -> None:
    records = [
        record("1", "a", identity={"subject": "alice"}, trace_id="trace-a"),
        record("2", "b", identity={"subject": "bob"}, trace_id="trace-b"),
        record("3", "c"),
    ]
    result = handler({"Records": records}, None)
    assert result["batchItemFailures"] == []
    assert seen == [
        ("a", "alice", "trace-a"),
        ("b", "bob", "trace-b"),
        ("c", "", None),
    ]
    assert current_identity() is ANONYMOUS
    assert get_trace_id() is None


def test_fifty_warm_invocations_open_once_on_one_loop(
    handler: SQSHandler,
    record: RecordFactory,
    built: tuple[LambdaSQSAdapter, Application, list[str]],
    seen: list[tuple[str, str | None, str | None]],
    loops: list[asyncio.AbstractEventLoop],
) -> None:
    for number in range(50):
        result = handler(
            {"Records": [record(f"{number}-a", "a"), record(f"{number}-b", "b")]}, None
        )
        assert result["batchItemFailures"] == []
    assert len(seen) == 100
    assert len(set(loops)) == 1
    assert built[2] == ["open"]


def test_recipe_does_not_load_http_or_celery_libraries() -> None:
    code = """
import sys
from pathlib import Path
from loom.aws_lambda import LambdaSQSAdapter
from loom.composition import AppConfig, CompositionSource
from loom.core.config import ConfigContext
from loom.core.environment import Environment
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.job.job import Job
class Probe(Job[None]):
    def execute(self) -> None:
        pass
source = CompositionSource(
    config=ConfigContext.from_dict({"app": {"name": "lambda-sqs"}}),
    environment=Environment.TEST,
    code_path=Path.cwd(),
    config_root=Path.cwd(),
    app=AppConfig(name="lambda-sqs"),
    observability=ObservabilityRuntime.noop(),
    observability_config=ObservabilityConfig(),
)
LambdaSQSAdapter(jobs=[Probe]).recipe(source)
print([name for name in ("fastapi", "starlette", "pydantic", "celery") if name in sys.modules])
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]"


def test_keyboard_interrupt_propagates(handler: SQSHandler, record: RecordFactory) -> None:
    with pytest.raises(KeyboardInterrupt):
        handler({"Records": [record("1", "interrupt")]}, None)


def test_bind_requires_recipe() -> None:
    adapter = LambdaSQSAdapter()
    with pytest.raises(CompositionStateError, match="call recipe first"):
        adapter.bind(None)  # type: ignore[arg-type]


def test_recipe_uses_lambda_profile_and_skips_rest(source: CompositionSource) -> None:
    class LocalJob(Job[None]):
        def execute(self) -> None:
            return None

    adapter = LambdaSQSAdapter(jobs=[LocalJob])
    recipe = adapter.recipe(source)
    assert recipe.profile is LAMBDA_PROFILE
    assert recipe.agents is False
    assert REST_CHECKS in recipe.skip_checks


def _add_broken_resource(application: Application) -> None:
    @asynccontextmanager
    async def broken() -> AsyncIterator[None]:
        raise RuntimeError("open failed")
        yield

    application.resources = (
        *application.resources,
        Resource(kind=ResourceKind.STORE, name="broken", manager=broken),
    )


def test_bind_propagates_open_failure(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
) -> None:
    adapter, application, _ = built
    _add_broken_resource(application)
    with pytest.raises(RuntimeError, match="open failed"):
        adapter.bind(application)


@pytest.mark.parametrize("fifo", [False, True])
@pytest.mark.parametrize(
    ("marker", "failed"),
    [("committed", []), ("scope", []), ("fail", ["2", "3"])],
)
def test_retry_policy_in_batch(
    handler: SQSHandler,
    record: RecordFactory,
    seen: list[tuple[str, str | None, str | None]],
    fifo: bool,
    marker: str,
    failed: list[str],
) -> None:
    records = [
        record("1", "a", fifo=fifo),
        record("2", marker, fifo=fifo),
        record("3", "c", fifo=fifo),
    ]
    result = handler({"Records": records}, None)
    expected = failed if fifo else failed[:1]
    assert result["batchItemFailures"] == [{"itemIdentifier": item} for item in expected]
    assert [item[0] for item in seen] == (
        ["a", marker] if fifo and expected else ["a", marker, "c"]
    )


@pytest.mark.parametrize("fifo", [False, True])
@pytest.mark.parametrize("poison", ["body", "job"])
def test_poison_message_retries_and_logs_typed_error(
    handler: SQSHandler,
    record: RecordFactory,
    seen: list[tuple[str, str | None, str | None]],
    caplog: pytest.LogCaptureFixture,
    fifo: bool,
    poison: str,
) -> None:
    bad = record("2", "bad", fifo=fifo)
    if poison == "body":
        bad["body"] = "{"
    else:
        body = json.loads(bad["body"])
        body["job"] = "MissingJob"
        bad["body"] = json.dumps(body)
    records = [record("1", "a", fifo=fifo), bad, record("3", "c", fifo=fifo)]
    result = handler({"Records": records}, None)
    expected = ["2", "3"] if fifo else ["2"]
    assert result["batchItemFailures"] == [{"itemIdentifier": item} for item in expected]
    assert [item[0] for item in seen] == (["a"] if fifo else ["a", "c"])
    error_class = "MalformedJobMessage" if poison == "body" else "UnknownJob"
    assert any(error_class in message for message in caplog.messages)


def test_timeout_marks_remaining_records_failed(
    handler: SQSHandler,
    record: RecordFactory,
    seen: list[tuple[str, str | None, str | None]],
) -> None:
    class Context:
        calls = 0

        def get_remaining_time_in_millis(self) -> int:
            self.calls += 1
            return 2000 if self.calls <= 2 else 999

    records = [record("1", "a"), record("2", "b"), record("3", "c")]
    result = handler({"Records": records}, Context())
    assert result["batchItemFailures"] == [{"itemIdentifier": "2"}, {"itemIdentifier": "3"}]
    assert [item[0] for item in seen] == ["a"]


def test_failed_job_resets_identity_and_trace(handler: SQSHandler, record: RecordFactory) -> None:
    records = [record("1", "fail", identity={"subject": "alice"}, trace_id="child")]
    handler({"Records": records}, None)
    assert current_identity() is ANONYMOUS
    assert get_trace_id() is None


def test_outer_trace_survives_batch(
    handler: SQSHandler,
    record: RecordFactory,
    seen: list[tuple[str, str | None, str | None]],
) -> None:
    from loom.core.tracing import reset_trace_id, set_trace_id

    token = set_trace_id("outer")
    try:
        records = [record("1", "a", trace_id="inner"), record("2", "b")]
        handler({"Records": records}, None)
        assert get_trace_id() == "outer"
        assert [item[2] for item in seen] == ["inner", "outer"]
    finally:
        reset_trace_id(token)


def test_cancelled_error_propagates(handler: SQSHandler, record: RecordFactory) -> None:
    with pytest.raises(asyncio.CancelledError):
        handler({"Records": [record("1", "cancel")]}, None)


@pytest.mark.parametrize("action", ["recipe", "bind", "bind_after_close"])
def test_adapter_already_composed(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
    source: CompositionSource,
    action: str,
) -> None:
    adapter, application, _ = built
    if action != "recipe":
        adapter.bind(application)
    if action == "bind_after_close":
        adapter.close()
    try:
        with pytest.raises(CompositionStateError) as caught:
            if action == "recipe":
                adapter.recipe(source)
            else:
                adapter.bind(application)
        assert caught.value.issue.code == "ADAPTER_ALREADY_COMPOSED"
    finally:
        adapter.close()


def test_close_closes_opened_resources(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
) -> None:
    adapter, application, lifecycle = built
    adapter.bind(application)
    adapter.close()
    assert lifecycle == ["open", "close"]


def test_duplicate_qualname_is_configuration_error(source: CompositionSource) -> None:
    from loom.aws_lambda.sqs import DuplicateJobName

    def make_job() -> type[Job[None]]:
        class Duplicate(Job[None]):
            def execute(self) -> None:
                return None

        return Duplicate

    adapter = LambdaSQSAdapter(jobs=[make_job(), make_job()])
    with pytest.raises(DuplicateJobName):
        adapter.recipe(source)


def test_failed_open_clears_event_loop(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
) -> None:
    adapter, application, _ = built
    _add_broken_resource(application)
    with pytest.raises(RuntimeError, match="open failed"):
        adapter.bind(application)
    assert adapter._loop is None


def test_job_config_is_applied(source: CompositionSource) -> None:
    from loom.core.config import ConfigContext
    from tests.helpers.composition import composition_source

    class ConfiguredJob(Job[None]):
        def execute(self) -> None:
            return None

    configured = composition_source(
        ConfigContext.from_dict(
            {"app": {"name": "lambda-sqs"}, "jobs": {"ConfiguredJob": {"retries": 7}}}
        ),
        app=source.app,
        code_path=source.code_path,
        config_root=source.config_root,
    )
    LambdaSQSAdapter(jobs=[ConfiguredJob]).recipe(configured)
    assert ConfiguredJob.__retries__ == 7


def test_job_span_is_emitted(
    built: tuple[LambdaSQSAdapter, Application, list[str]], record: RecordFactory
) -> None:
    from loom.core.observability.event import EventKind, LifecycleEvent, Scope
    from loom.core.observability.runtime import ObservabilityRuntime

    class Observer:
        def __init__(self) -> None:
            self.events: list[LifecycleEvent] = []

        def on_event(self, event: LifecycleEvent) -> None:
            self.events.append(event)

    adapter, application, _ = built
    observer = Observer()
    application.observability = ObservabilityRuntime([observer])
    handler = adapter.bind(application)
    try:
        result = handler({"Records": [record("1", "a", trace_id="trace")]}, None)
        job_events = [event for event in observer.events if event.scope is Scope.JOB]
        assert result["batchItemFailures"] == []
        assert [(event.kind, event.id) for event in job_events] == [
            (EventKind.START, "1"),
            (EventKind.END, "1"),
        ]
    finally:
        adapter.close()


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("unknown", [{"itemIdentifier": "1"}]),
        ("malformed", [{"itemIdentifier": "1"}]),
        ("null_payload", []),
        ("missing_id", [{"itemIdentifier": ""}]),
        ("non_mapping", [{"itemIdentifier": ""}]),
    ],
)
def test_record_edge_cases(
    handler: SQSHandler, record: RecordFactory, kind: str, expected: list[dict[str, str]]
) -> None:
    item: object = record("1", "a")
    if kind == "unknown":
        item = record("1", "a", body=json.dumps({"job": "UnknownJob"}))
    elif kind == "malformed":
        item = record("1", "a", body="{")
    elif kind == "null_payload":
        body = json.loads(item["body"])  # type: ignore[index]
        body["payload"] = None
        item = record("1", "a", body=json.dumps(body))
    elif kind == "missing_id":
        item = {"body": "{"}
    elif kind == "non_mapping":
        item = "bad"
    result = handler({"Records": [item]}, None)
    assert result["batchItemFailures"] == expected


@pytest.mark.parametrize("error", [MalformedJobMessage("{"), UnknownJob("MissingJob")])
def test_poison_errors_remain_retryable(error: Exception) -> None:
    assert error.retryable is True  # type: ignore[attr-defined]


def test_job_timeout_is_retryable(source: CompositionSource, record: RecordFactory) -> None:
    class SlowJob(Job[None]):
        __timeout__ = 0.001  # type: ignore[assignment]

        async def execute(self) -> None:
            await asyncio.sleep(0.1)

    class Context:
        def get_remaining_time_in_millis(self) -> int:
            return 5000

    adapter = LambdaSQSAdapter(jobs=[SlowJob])
    application = compose_application(source, adapter)
    handler = adapter.bind(application)
    try:
        result = handler(
            {"Records": [record("slow", "a", job_name=SlowJob.__qualname__)]}, Context()
        )
        assert result["batchItemFailures"] == [{"itemIdentifier": "slow"}]
    finally:
        adapter.close()


def test_malformed_record_emits_error_span(
    built: tuple[LambdaSQSAdapter, Application, list[str]], record: RecordFactory
) -> None:
    from loom.core.observability.event import EventKind, LifecycleEvent, Scope
    from loom.core.observability.runtime import ObservabilityRuntime

    class Observer:
        def __init__(self) -> None:
            self.events: list[LifecycleEvent] = []

        def on_event(self, event: LifecycleEvent) -> None:
            self.events.append(event)

    adapter, application, _ = built
    observer = Observer()
    application.observability = ObservabilityRuntime([observer])
    handler = adapter.bind(application)
    try:
        item = record("1", "a")
        item["body"] = "{"
        handler({"Records": [item]}, None)
        assert [(event.scope, event.kind) for event in observer.events] == [
            (Scope.JOB, EventKind.START),
            (Scope.JOB, EventKind.ERROR),
        ]
    finally:
        adapter.close()


def test_bind_reports_inline_dispatch(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter, application, _ = built
    adapter.bind(application)
    try:
        assert any("run inline" in message for message in caplog.messages)
    finally:
        adapter.close()


def test_bind_warns_for_non_inline_service(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    from loom.core.job.service import JobService

    adapter, application, _ = built
    application.kernel.container.register(JobService, lambda: object())
    adapter.bind(application)
    try:
        assert any("publish to the Celery broker" in message for message in caplog.messages)
    finally:
        adapter.close()


def test_recipe_warns_for_ignored_discovered_components(
    source: CompositionSource,
    probe: tuple[type[Job[None]], Probe],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import ModuleType

    from loom.core.config import ConfigContext
    from tests.helpers.composition import composition_source

    manifest = ModuleType("tests.unit.aws_lambda._warning_manifest")
    manifest.MODELS = [type("DiscoveredModel", (), {})]  # type: ignore[attr-defined]
    manifest.CALLBACKS = [type("DiscoveredCallback", (), {})]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, manifest.__name__, manifest)
    configured = composition_source(
        ConfigContext.from_dict(
            {
                "app": {
                    "name": "lambda-sqs",
                    "discovery": {
                        "mode": "manifest",
                        "manifest": {"module": manifest.__name__},
                    },
                }
            }
        ),
        app=source.app,
        code_path=source.code_path,
        config_root=source.config_root,
    )
    LambdaSQSAdapter(jobs=[probe[0]]).recipe(configured)
    assert any("ignores AutoCRUD" in message for message in caplog.messages)
    assert any("ignores discovered job callbacks" in message for message in caplog.messages)


def test_committed_failure_logs_final_outcome(
    handler: SQSHandler,
    record: RecordFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    handler({"Records": [record("42", "committed")]}, None)
    final_logs = [message for message in caplog.messages if "SQS job failed finally" in message]
    assert len(final_logs) == 1
    assert "PostCommitError" in final_logs[0]
    assert "message_id': '42'" in final_logs[0]
    assert "committed': True" in final_logs[0]
