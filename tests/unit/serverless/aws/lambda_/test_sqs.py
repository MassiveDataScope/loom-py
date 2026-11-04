from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from loom.composition import (
    AppConfig,
    Application,
    CompositionSource,
    CompositionStateError,
    compose_application,
)
from loom.composition.resources import Resource, ResourceKind
from loom.core.checks import REST_CHECKS
from loom.core.config import ConfigContext
from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.tracing import get_trace_id, reset_trace_id, set_trace_id
from loom.serverless.aws.lambda_ import LambdaSQSAdapter, LambdaUnservedConfigError
from loom.serverless.aws.lambda_.sqs import DuplicateJobName, SQSBatchFailedError, SQSHandler
from tests.helpers.composition import composition_source
from tests.helpers.loops import RecordedLoops
from tests.unit.serverless.aws.lambda_._probe import Probe
from tests.unit.serverless.aws.lambda_._records import RecordFactory


class _OtherJobService:
    """A job service the log must name: neither inline nor Celery."""


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


@pytest.fixture
def whole_batch_handler(tmp_path: Path, probe: tuple[type[Job[None]], Any]) -> Iterator[SQSHandler]:
    worker = {"sqs": True, "report_batch_item_failures": False}
    source = composition_source(
        ConfigContext.from_dict({"lambda": {"functions": {"worker": worker}}}),
        app=AppConfig(name="lambda-sqs", entry="worker"),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    adapter = LambdaSQSAdapter(jobs=[probe[0]])
    yield adapter.bind(compose_application(source, adapter))
    adapter.close()


@pytest.mark.parametrize("fifo", [False, True])
def test_without_batch_item_failures_a_failed_record_fails_the_whole_batch(
    whole_batch_handler: SQSHandler, record: RecordFactory, fifo: bool
) -> None:
    records = [record("1", "a", fifo=fifo), record("2", "fail", fifo=fifo)]
    with pytest.raises(SQSBatchFailedError, match="1 of 2 SQS records failed"):
        whole_batch_handler({"Records": records}, None)


def test_without_batch_item_failures_a_batch_that_ran_returns_no_report(
    whole_batch_handler: SQSHandler, record: RecordFactory
) -> None:
    assert whole_batch_handler({"Records": [record("1", "a")]}, None) == {}


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
from loom.serverless.aws.lambda_ import LambdaSQSAdapter
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
    event = {"Records": [record("1", "interrupt")]}

    with pytest.raises(KeyboardInterrupt):
        handler(event, None)


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
    token = set_trace_id("outer")
    try:
        records = [record("1", "a", trace_id="inner"), record("2", "b")]
        handler({"Records": records}, None)
        assert get_trace_id() == "outer"
        assert [item[2] for item in seen] == ["inner", "outer"]
    finally:
        reset_trace_id(token)


def test_cancelled_error_propagates(handler: SQSHandler, record: RecordFactory) -> None:
    event = {"Records": [record("1", "cancel")]}

    with pytest.raises(asyncio.CancelledError):
        handler(event, None)


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
    again = (
        partial(adapter.recipe, source)
        if action == "recipe"
        else partial(adapter.bind, application)
    )
    try:
        with pytest.raises(CompositionStateError) as caught:
            again()
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
    def make_job() -> type[Job[None]]:
        class Duplicate(Job[None]):
            def execute(self) -> None:
                return None

        return Duplicate

    adapter = LambdaSQSAdapter(jobs=[make_job(), make_job()])
    with pytest.raises(DuplicateJobName):
        adapter.recipe(source)


def test_failed_open_closes_its_loop(
    built: tuple[LambdaSQSAdapter, Application, list[str]], event_loops: RecordedLoops
) -> None:
    adapter, application, _ = built
    _add_broken_resource(application)
    with pytest.raises(RuntimeError, match="open failed"):
        adapter.bind(application)
    assert event_loops.created[0].is_closed()


def test_failed_open_unsets_its_loop(
    built: tuple[LambdaSQSAdapter, Application, list[str]], event_loops: RecordedLoops
) -> None:
    adapter, application, _ = built
    _add_broken_resource(application)
    with pytest.raises(RuntimeError, match="open failed"):
        adapter.bind(application)
    assert event_loops.made_current[-1] is None


@pytest.mark.parametrize("closes", [1, 2], ids=["once", "twice"])
def test_closing_the_handler_closes_its_loop(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
    event_loops: RecordedLoops,
    closes: int,
) -> None:
    adapter, application, _ = built
    handler = adapter.bind(application)

    for _ in range(closes):
        handler.close()

    assert event_loops.created[0].is_closed()


def test_closing_the_handler_closes_the_application(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
) -> None:
    adapter, application, lifecycle = built
    handler = adapter.bind(application)

    handler.close()

    assert lifecycle == ["open", "close"]


def test_the_composition_applies_the_job_config(source: CompositionSource) -> None:
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

    compose_application(configured, LambdaSQSAdapter(jobs=[ConfiguredJob]))

    assert ConfiguredJob.__retries__ == 7


def test_job_span_is_emitted(
    built: tuple[LambdaSQSAdapter, Application, list[str]], record: RecordFactory
) -> None:
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
    adapter, application, _ = built
    application.kernel.container.register(JobService, _OtherJobService)
    adapter.bind(application)
    try:
        assert any(
            "through the configured job service" in record.getMessage()
            and "_OtherJobService" in record.getMessage()
            for record in caplog.records
        )
    finally:
        adapter.close()


def test_recipe_warns_for_ignored_discovered_components(
    source: CompositionSource,
    probe: tuple[type[Job[None]], Probe],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = ModuleType("tests.unit.serverless.aws.lambda_._warning_manifest")
    manifest.MODELS = [type("DiscoveredModel", (), {})]  # type: ignore[attr-defined]
    manifest.CALLBACKS = [type("DiscoveredCallback", (), {})]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, manifest.__name__, manifest)
    configured = _manifest_source(source, manifest.__name__)
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


def _manifest_source(source: CompositionSource, module: str, **sections: Any) -> CompositionSource:
    """*source*, discovering from the manifest *module*, with extra top-level *sections*."""
    discovery = {"mode": "manifest", "manifest": {"module": module}}
    return composition_source(
        ConfigContext.from_dict(
            {"app": {"name": "lambda-sqs", "discovery": discovery}, **sections}
        ),
        app=source.app,
        code_path=source.code_path,
        config_root=source.config_root,
    )


@pytest.mark.parametrize(
    ("sections", "key"),
    [
        ({"ai": {"models": {}}}, "ai"),
        ({"observability": {"prometheus": {"enabled": True}}}, "observability.prometheus.enabled"),
    ],
    ids=["ai-section", "metrics"],
)
def test_what_lambda_does_not_serve_is_refused_before_discovery(
    source: CompositionSource, sections: dict[str, Any], key: str
) -> None:
    undiscoverable = _manifest_source(
        source, "tests.unit.serverless.aws.lambda_._no_such_manifest", **sections
    )

    adapter = LambdaSQSAdapter()

    with pytest.raises(LambdaUnservedConfigError) as raised:
        adapter.recipe(undiscoverable)
    assert raised.value.key == key


def test_manifest_agents_are_refused(
    source: CompositionSource,
    probe: tuple[type[Job[None]], Probe],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = ModuleType("tests.unit.serverless.aws.lambda_._agents_manifest")
    manifest.JOBS = [probe[0]]  # type: ignore[attr-defined]
    manifest.AGENTS = ["agents/*.yaml"]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, manifest.__name__, manifest)
    configured = _manifest_source(source, manifest.__name__)

    adapter = LambdaSQSAdapter()

    with pytest.raises(LambdaUnservedConfigError) as raised:
        adapter.recipe(configured)
    assert raised.value.key == "manifest.AGENTS"
