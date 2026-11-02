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
from loom.composition import Application, CompositionSource, CompositionStateError
from loom.composition.resources import Resource, ResourceKind
from loom.core.checks import REST_CHECKS
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.job import Job
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.tracing import get_trace_id
from tests.unit.aws_lambda.conftest import RecordFactory


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


def test_message_group_attribute_enables_fifo_rule(
    handler: SQSHandler, record: RecordFactory, seen: list[tuple[str, str | None, str | None]]
) -> None:
    first = record("1", "fail")
    first["attributes"] = {"MessageGroupId": "g"}
    result = handler({"Records": [first, record("2", "b")]}, None)
    assert result["batchItemFailures"] == [
        {"itemIdentifier": "1"},
        {"itemIdentifier": "2"},
    ]
    assert [item[0] for item in seen] == ["fail"]


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


def test_unknown_job_is_a_record_failure(handler: SQSHandler, record: RecordFactory) -> None:
    unknown = record("1", "a")
    body = json.loads(unknown["body"])
    body["job"] = "UnknownJob"
    unknown["body"] = json.dumps(body)
    result = handler({"Records": [unknown]}, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "1"}]


def test_recipe_uses_lambda_profile_and_skips_rest(source: CompositionSource) -> None:
    class LocalJob(Job[None]):
        def execute(self) -> None:
            return None

    adapter = LambdaSQSAdapter(jobs=[LocalJob])
    recipe = adapter.recipe(source)
    assert recipe.profile is LAMBDA_PROFILE
    assert recipe.agents is False
    assert REST_CHECKS in recipe.skip_checks


def test_bind_propagates_open_failure(
    built: tuple[LambdaSQSAdapter, Application, list[str]],
) -> None:
    adapter, application, _ = built

    @asynccontextmanager
    async def broken() -> AsyncIterator[None]:
        raise RuntimeError("open failed")
        yield

    application.resources = (
        *application.resources,
        Resource(kind=ResourceKind.STORE, name="broken", manager=broken),
    )
    with pytest.raises(RuntimeError, match="open failed"):
        adapter.bind(application)
