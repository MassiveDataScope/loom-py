from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig, Application, CompositionStateError, Project
from loom.core.checks import CheckScope
from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.scanner import PackageScanner
from loom.core.discovery.worker import DuplicateJobName
from loom.core.errors.log_events import ErrorLogField
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.job import Job
from loom.core.job.log_events import JobLogEvent
from loom.core.job.service import JobService
from loom.core.logger import LogField
from loom.core.model import BaseModel
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observability import Observability
from loom.core.resources import Resource, ResourceKind
from loom.core.stores import LAMBDA_PROFILE
from loom.core.tracing import get_trace_id, reset_trace_id, set_trace_id
from loom.serverless.aws.lambda_ import (
    LambdaHandler,
    LambdaRuntime,
    LambdaUnservedConfigError,
)
from loom.testing import RecordingJobTransport
from tests.helpers.boundary import left_loom
from tests.helpers.composition import composition_project
from tests.helpers.loggers import rendered
from tests.helpers.loops import RecordedLoops
from tests.unit.serverless.aws.lambda_._probe import Probe
from tests.unit.serverless.aws.lambda_._records import WORKER, RecordFactory, with_worker


def _logged(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Each record of *caplog*, message and fields in one string."""
    return [rendered(record) for record in caplog.records]


class _OtherJobTransport(RecordingJobTransport):
    """A job transport the log must name: neither inline nor Celery."""


def _add_broken_resource(application: Application) -> None:
    @asynccontextmanager
    async def broken() -> AsyncIterator[None]:
        raise RuntimeError("open failed")
        yield

    application.resources = (
        *application.resources,
        Resource(kind=ResourceKind.STORE, name="broken", manager=broken),
    )


def _manifest_project(project: Project, module: str, **sections: Any) -> Project:
    """Return a project configured to scan *module*."""
    return composition_project(
        ConfigContext.from_dict(
            with_worker({"app": {"name": "lambda-sqs", "packages": [module]}, **sections})
        ),
        app=AppConfig(name="lambda-sqs", entry=WORKER, packages=[module]),
        code_path=project.code_path,
        config_root=project.config_root,
    )


class TestSQSHandler:
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
        self,
        handler: LambdaHandler,
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

    @pytest.mark.parametrize("entry", [None, "worker"])
    def test_the_recipe_refuses_a_misdeclared_lambda_section(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Any], entry: str | None
    ) -> None:
        project = composition_project(
            ConfigContext.from_dict(with_worker({}, batch_size=11)),
            app=AppConfig(name="lambda-sqs", entry=entry),
            code_path=tmp_path,
            config_root=tmp_path,
        )
        adapter = LambdaRuntime(jobs=[probe[0]])
        with pytest.raises(ConfigError, match="needs batching_window_s"):
            adapter.recipe(project)

    @pytest.mark.parametrize("missing", ["messageId", "receiptHandle"])
    def test_a_record_that_cannot_be_settled_is_reported_and_never_run(
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        seen: list[tuple[str, str | None, str | None]],
        missing: str,
    ) -> None:
        runnable = record("1", "a")
        del runnable[missing]

        result = handler({"Records": [runnable]}, None)

        assert (len(result["batchItemFailures"]), seen) == (1, [])

    def test_a_malformed_body_is_abandoned_at_once_and_not_reported(
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        seen: list[tuple[str, str | None, str | None]],
    ) -> None:
        bad = record("1", "a")
        bad["body"] = "{"
        result = handler({"Records": [bad, record("2", "b")]}, None)
        assert result["batchItemFailures"] == []
        assert [item[0] for item in seen] == ["b"]

    def test_message_group_attribute_does_not_enable_fifo_rule(
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        seen: list[tuple[str, str | None, str | None]],
    ) -> None:
        first = record("1", "fail")
        first["attributes"] = {"MessageGroupId": "g"}
        result = handler({"Records": [first, record("2", "b")]}, None)
        assert result["batchItemFailures"] == [{"itemIdentifier": "1"}]
        assert [item[0] for item in seen] == ["fail", "b"]

    def test_each_message_sees_its_own_identity_and_trace(
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        seen: list[tuple[str, str | None, str | None]],
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
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        built: tuple[LambdaRuntime, Application, list[str]],
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

    def test_recipe_does_not_load_http_or_celery_libraries(self) -> None:
        code = """
import sys
from pathlib import Path
from loom.serverless.aws.lambda_ import LambdaRuntime
from loom.composition import AppConfig, Project
from loom.core.config import ConfigContext, ConfigError
from loom.core.environment import Environment
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.observability import Observability
from loom.core.job.job import Job
class Probe(Job[None]):
    def execute(self) -> None:
        pass
trigger = {"source": "jobs", "queue": {"name": "q"}}
project = Project(
    config=ConfigContext.from_dict({"lambda": {"functions": {"worker": {"trigger": trigger}}}}),
    environment=Environment.TEST,
    code_path=Path.cwd(),
    config_root=Path.cwd(),
    app=AppConfig(name="lambda-sqs", entry="worker"),
    observability=Observability.noop(),
    observability_config=ObservabilityConfig(),
)
LambdaRuntime(jobs=[Probe]).recipe(project)
loaded = ("fastapi", "starlette", "mangum", "pydantic", "celery")
print([name for name in loaded if name in sys.modules])
"""
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )
        assert result.stdout.strip() == "[]"

    def test_keyboard_interrupt_propagates(
        self, handler: LambdaHandler, record: RecordFactory
    ) -> None:
        event = {"Records": [record("1", "interrupt")]}

        with pytest.raises(KeyboardInterrupt):
            handler(event, None)

    def test_bind_requires_recipe(self) -> None:
        adapter = LambdaRuntime()
        with pytest.raises(CompositionStateError, match="call recipe first"):
            adapter.mount(None)  # type: ignore[arg-type]

    def test_recipe_uses_lambda_profile_and_skips_rest(self, project: Project) -> None:
        class LocalJob(Job[None]):
            def execute(self) -> None:
                return None

        adapter = LambdaRuntime(jobs=[LocalJob])
        recipe = adapter.recipe(project)
        assert recipe.profile is LAMBDA_PROFILE
        assert (recipe.agents, recipe.agent_endpoints) == (True, False)
        assert recipe.check_scope == CheckScope.without_rest()

    def test_bind_propagates_open_failure(
        self,
        built: tuple[LambdaRuntime, Application, list[str]],
    ) -> None:
        adapter, application, _ = built
        _add_broken_resource(application)
        with left_loom(RuntimeError, match="open failed"):
            adapter.mount(application)

    @pytest.mark.parametrize("fifo", [False, True])
    @pytest.mark.parametrize(
        ("marker", "failed"),
        [("committed", []), ("scope", []), ("uncertain", []), ("fail", ["2", "3"])],
    )
    def test_retry_policy_in_batch(
        self,
        handler: LambdaHandler,
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

    @pytest.mark.parametrize(
        ("poison", "fifo", "expected", "ran"),
        [
            ("body", False, [], ["a", "c"]),
            ("body", True, [], ["a", "c"]),
            ("job", False, ["2"], ["a", "c"]),
            ("job", True, ["2", "3"], ["a"]),
        ],
    )
    def test_a_malformed_message_is_abandoned_and_an_unknown_job_retried(
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        seen: list[tuple[str, str | None, str | None]],
        caplog: pytest.LogCaptureFixture,
        poison: str,
        fifo: bool,
        expected: list[str],
        ran: list[str],
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
        assert result["batchItemFailures"] == [{"itemIdentifier": item} for item in expected]
        assert [item[0] for item in seen] == ran
        error_class = "MalformedJobMessage" if poison == "body" else "UnknownJob"
        assert any(error_class in message for message in _logged(caplog))

    def test_timeout_marks_remaining_records_failed(
        self,
        handler: LambdaHandler,
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

    def test_failed_job_resets_identity_and_trace(
        self, handler: LambdaHandler, record: RecordFactory
    ) -> None:
        records = [record("1", "fail", identity={"subject": "alice"}, trace_id="child")]
        handler({"Records": records}, None)
        assert current_identity() is ANONYMOUS
        assert get_trace_id() is None

    def test_outer_trace_survives_batch(
        self,
        handler: LambdaHandler,
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

    def test_cancelled_error_propagates(
        self, handler: LambdaHandler, record: RecordFactory
    ) -> None:
        event = {"Records": [record("1", "cancel")]}

        with pytest.raises(asyncio.CancelledError):
            handler(event, None)

    @pytest.mark.parametrize("action", ["recipe", "mount", "mount_after_close"])
    def test_runtime_already_composed(
        self,
        built: tuple[LambdaRuntime, Application, list[str]],
        project: Project,
        action: str,
    ) -> None:
        adapter, application, _ = built
        if action != "recipe":
            adapter.mount(application)
        if action == "mount_after_close":
            adapter.close()
        again = (
            partial(adapter.recipe, project)
            if action == "recipe"
            else partial(adapter.mount, application)
        )
        try:
            with pytest.raises(CompositionStateError) as caught:
                again()
            assert caught.value.issue.code == "RUNTIME_ALREADY_COMPOSED"
        finally:
            adapter.close()

    def test_close_closes_opened_resources(
        self,
        built: tuple[LambdaRuntime, Application, list[str]],
    ) -> None:
        adapter, application, lifecycle = built
        adapter.mount(application)
        adapter.close()
        assert lifecycle == ["open", "close"]

    def test_duplicate_qualname_is_configuration_error(self, project: Project) -> None:
        def make_job() -> type[Job[None]]:
            class Duplicate(Job[None]):
                def execute(self) -> None:
                    return None

            return Duplicate

        adapter = LambdaRuntime(jobs=[make_job(), make_job()])
        with pytest.raises(DuplicateJobName):
            adapter.recipe(project)

    def test_failed_open_closes_its_loop(
        self, built: tuple[LambdaRuntime, Application, list[str]], event_loops: RecordedLoops
    ) -> None:
        adapter, application, _ = built
        _add_broken_resource(application)
        with left_loom(RuntimeError, match="open failed"):
            adapter.mount(application)
        assert event_loops.created[0].is_closed()

    def test_failed_open_unsets_its_loop(
        self, built: tuple[LambdaRuntime, Application, list[str]], event_loops: RecordedLoops
    ) -> None:
        adapter, application, _ = built
        _add_broken_resource(application)
        with left_loom(RuntimeError, match="open failed"):
            adapter.mount(application)
        assert event_loops.made_current[-1] is None

    @pytest.mark.parametrize("closes", [1, 2], ids=["once", "twice"])
    def test_closing_the_handler_closes_its_loop(
        self,
        built: tuple[LambdaRuntime, Application, list[str]],
        event_loops: RecordedLoops,
        closes: int,
    ) -> None:
        adapter, application, _ = built
        handler = adapter.mount(application)

        for _ in range(closes):
            handler.close()

        assert event_loops.created[0].is_closed()

    def test_closing_the_handler_closes_the_application(
        self,
        built: tuple[LambdaRuntime, Application, list[str]],
    ) -> None:
        adapter, application, lifecycle = built
        handler = adapter.mount(application)

        handler.close()

        assert lifecycle == ["open", "close"]

    def test_job_span_is_emitted(
        self, built: tuple[LambdaRuntime, Application, list[str]], record: RecordFactory
    ) -> None:
        class Observer:
            def __init__(self) -> None:
                self.events: list[LifecycleEvent] = []

            def on_event(self, event: LifecycleEvent) -> None:
                self.events.append(event)

        adapter, application, _ = built
        observer = Observer()
        application.observability = Observability([observer])
        handler = adapter.mount(application)
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
            ("malformed", []),
            ("null_payload", []),
            ("missing_id", [{"itemIdentifier": ""}]),
            ("non_mapping", [{"itemIdentifier": ""}]),
        ],
    )
    def test_record_edge_cases(
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        kind: str,
        expected: list[dict[str, str]],
    ) -> None:
        item: object = record("1", "a")
        if kind == "unknown":
            item = record("1", "a", job_name="UnknownJob")
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

    @pytest.mark.parametrize(
        ("jobs", "failed"),
        [({}, [{"itemIdentifier": "slow"}]), ({"SlowJob": {"timeout": 30}}, [])],
        ids=["declared timeout", "jobs section timeout"],
    )
    def test_a_record_runs_under_the_applications_job_timeout(
        self,
        project: Project,
        record: RecordFactory,
        jobs: dict[str, Any],
        failed: list[Any],
    ) -> None:
        """A timed-out record is retried; the ``jobs:`` section lifts the declared limit."""

        class SlowJob(Job[None]):
            __timeout__ = 0.001  # type: ignore[assignment]

            async def execute(self) -> None:
                await asyncio.sleep(0.1)

        class Context:
            def get_remaining_time_in_millis(self) -> int:
                return 5000

        configured = composition_project(
            ConfigContext.from_dict(with_worker({"app": {"name": "lambda-sqs"}, "jobs": jobs})),
            app=project.app,
            code_path=project.code_path,
            config_root=project.config_root,
        )
        adapter = LambdaRuntime(jobs=[SlowJob])
        handler = adapter.mount(configured.compose(adapter))
        try:
            result = handler(
                {"Records": [record("slow", "a", job_name=SlowJob.__qualname__)]}, Context()
            )
        finally:
            adapter.close()

        assert result["batchItemFailures"] == failed
        assert SlowJob.__timeout__ == 0.001

    def test_malformed_record_emits_error_span(
        self, built: tuple[LambdaRuntime, Application, list[str]], record: RecordFactory
    ) -> None:
        class Observer:
            def __init__(self) -> None:
                self.events: list[LifecycleEvent] = []

            def on_event(self, event: LifecycleEvent) -> None:
                self.events.append(event)

        adapter, application, _ = built
        observer = Observer()
        application.observability = Observability([observer])
        handler = adapter.mount(application)
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
        self,
        built: tuple[LambdaRuntime, Application, list[str]],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        adapter, application, _ = built
        adapter.mount(application)
        try:
            assert any("InlineJobTransport" in message for message in _logged(caplog))
        finally:
            adapter.close()

    def test_bind_warns_for_non_inline_service(
        self,
        project: Project,
        probe: tuple[type[Job[None]], Probe],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        adapter = LambdaRuntime(jobs=[probe[0]])
        from tests.helpers.composition import WithOverrides

        application = project.compose(
            WithOverrides(
                adapter,
                lambda container: container.register(
                    JobService, lambda: JobService(_OtherJobTransport())
                ),
            )
        )
        adapter.mount(application)
        try:
            assert any(
                "through the configured job transport" in message
                and "_OtherJobTransport" in message
                for message in _logged(caplog)
            )
        finally:
            adapter.close()

    def test_recipe_warns_for_ignored_discovered_components(
        self,
        project: Project,
        probe: tuple[type[Job[None]], Probe],
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class DiscoveredModel(BaseModel):
            __tablename__ = "discovered_models"

        monkeypatch.setattr(
            PackageScanner,
            "discover",
            lambda self, **kwargs: DiscoveryResult(models=(DiscoveredModel,), use_cases=()),
        )
        configured = _manifest_project(project, "acme_jobs")
        LambdaRuntime(jobs=[probe[0]]).recipe(configured)
        assert any("ignores AutoCRUD" in message for message in _logged(caplog))

    def test_committed_failure_logs_final_outcome(
        self,
        handler: LambdaHandler,
        record: RecordFactory,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        handler({"Records": [record("42", "committed")]}, None)
        [final] = [r for r in caplog.records if r.getMessage() == JobLogEvent.DELIVERY_ABANDONED]
        assert final.__dict__[ErrorLogField.EXCEPTION_TYPE].endswith("PostCommitError")
        assert final.__dict__[LogField.TASK_ID] == "42"

    @pytest.mark.parametrize(
        ("sections", "key"),
        [
            (
                {"observability": {"prometheus": {"enabled": True}}},
                "observability.prometheus.enabled",
            )
        ],
        ids=["metrics"],
    )
    def test_what_lambda_does_not_serve_is_refused_before_discovery(
        self, project: Project, sections: dict[str, Any], key: str
    ) -> None:
        undiscoverable = _manifest_project(
            project, "tests.unit.serverless.aws.lambda_._no_such_manifest", **sections
        )

        adapter = LambdaRuntime()

        with pytest.raises(LambdaUnservedConfigError) as raised:
            adapter.recipe(undiscoverable)
        assert raised.value.key == key
