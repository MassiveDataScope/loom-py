import importlib
from typing import TYPE_CHECKING, Any

from loom.testing.agents import ContractScenario, FakeAgentEngine, agent_engine_contract_suite
from loom.testing.golden import GoldenHarness, serialize_plan
from loom.testing.in_memory import InMemoryRepository
from loom.testing.jobs import (
    DispatchedJob,
    RecordingJobService,
    failed_ids,
    sqs_event,
    sqs_raw_record,
    sqs_record,
)
from loom.testing.runner import (
    AgentHandleDouble,
    McpHandleDouble,
    SqlGrantHandleDouble,
    UseCaseTest,
)

if TYPE_CHECKING:
    from loom.testing.http_harness import HttpTestHarness
    from loom.testing.lambda_http import LambdaResponse, LambdaTestClient
    from loom.testing.repository_harness import (
        RepositoryIntegrationHarness,
        ScenarioDict,
        build_repository_harness,
    )

__all__ = [
    "AgentHandleDouble",
    "ContractScenario",
    "DispatchedJob",
    "FakeAgentEngine",
    "GoldenHarness",
    "HttpTestHarness",
    "InMemoryRepository",
    "LambdaResponse",
    "LambdaTestClient",
    "McpHandleDouble",
    "RecordingJobService",
    "RepositoryIntegrationHarness",
    "ScenarioDict",
    "SqlGrantHandleDouble",
    "UseCaseTest",
    "agent_engine_contract_suite",
    "build_repository_harness",
    "failed_ids",
    "serialize_plan",
    "sqs_event",
    "sqs_raw_record",
    "sqs_record",
]

_REPOSITORY_HARNESS_EXPORTS = frozenset(
    {"RepositoryIntegrationHarness", "ScenarioDict", "build_repository_harness"}
)

_LAMBDA_HTTP_EXPORTS = frozenset({"LambdaResponse", "LambdaTestClient"})


def __getattr__(name: str) -> Any:
    """Resolve exports whose dependencies belong to optional extras on first access."""
    if name == "HttpTestHarness":
        return _http_test_harness()
    if name in _LAMBDA_HTTP_EXPORTS:
        return _lambda_http_export(name)
    if name in _REPOSITORY_HARNESS_EXPORTS:
        return _repository_harness_export(name)
    raise AttributeError(name)


def _http_test_harness() -> Any:
    """Import ``HttpTestHarness``, naming the ``rest`` extra when its dependencies are missing."""
    try:
        from loom.testing.http_harness import HttpTestHarness as _HttpTestHarness
    except ImportError as exc:  # pragma: no cover - exercised only when REST deps are absent
        raise ImportError(
            "HttpTestHarness requires the 'rest' extra (fastapi, pydantic, uvicorn). "
            "Install loom-kernel with the 'rest' extra to use HTTP test harnesses."
        ) from exc
    return _HttpTestHarness


def _repository_harness_export(name: str) -> Any:
    """Import one ``repository_harness`` export, naming the ``sqlalchemy`` extra when missing."""
    try:
        repository_harness = importlib.import_module("loom.testing.repository_harness")
    except ImportError as exc:
        raise ImportError(
            f"{name} requires SQLAlchemy. "
            "Install loom-kernel[sqlalchemy] to use repository integration harnesses."
        ) from exc
    return getattr(repository_harness, name)


def _lambda_http_export(name: str) -> Any:
    """Import one ``lambda_http`` export, naming the ``lambda`` extra when missing."""
    try:
        lambda_http = importlib.import_module("loom.testing.lambda_http")
    except ImportError as exc:
        raise ImportError(
            f"{name} requires the 'lambda' extra. "
            "Install loom-kernel[lambda] to test Lambda HTTP functions."
        ) from exc
    return getattr(lambda_http, name)
