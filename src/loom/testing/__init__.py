import importlib
from typing import TYPE_CHECKING, Any

from loom.testing.agents import ContractScenario, FakeAgentEngine, agent_engine_contract_suite
from loom.testing.golden import GoldenHarness, serialize_plan
from loom.testing.in_memory import InMemoryRepository
from loom.testing.jobs import DispatchedJob, JobTestData, ManualTimer, RecordingJobTransport
from loom.testing.overrides import Override
from loom.testing.runner import (
    AgentHandleDouble,
    McpHandleDouble,
    SqlGrantHandleDouble,
    UseCaseTest,
)
from loom.testing.serverless import TEST_QUEUE, LambdaEvent

if TYPE_CHECKING:
    from loom.testing.data import LoomFactory, make
    from loom.testing.http_harness import HttpTestHarness
    from loom.testing.jwt_auth import JwtTestAuth, jwt_test_auth
    from loom.testing.lambda_http import LambdaTestClient
    from loom.testing.lambda_jobs import LambdaWorker
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
    "JobTestData",
    "JwtTestAuth",
    "LoomFactory",
    "TEST_QUEUE",
    "LambdaEvent",
    "LambdaTestClient",
    "LambdaWorker",
    "ManualTimer",
    "McpHandleDouble",
    "Override",
    "RecordingJobTransport",
    "RepositoryIntegrationHarness",
    "ScenarioDict",
    "SqlGrantHandleDouble",
    "UseCaseTest",
    "agent_engine_contract_suite",
    "build_repository_harness",
    "jwt_test_auth",
    "make",
    "serialize_plan",
]

_DATA_EXPORTS = frozenset({"LoomFactory", "make"})

_JWT_EXPORTS = frozenset({"JwtTestAuth", "jwt_test_auth"})

_REPOSITORY_HARNESS_EXPORTS = frozenset(
    {"RepositoryIntegrationHarness", "ScenarioDict", "build_repository_harness"}
)


def __getattr__(name: str) -> Any:
    """Resolve exports whose dependencies belong to optional extras on first access."""
    if name == "HttpTestHarness":
        return _http_test_harness()
    if name == "LambdaTestClient":
        return _lambda_test_client()
    if name == "LambdaWorker":
        return importlib.import_module("loom.testing.lambda_jobs").LambdaWorker
    if name in _DATA_EXPORTS:
        return _data_export(name)
    if name in _JWT_EXPORTS:
        return getattr(importlib.import_module("loom.testing.jwt_auth"), name)
    if name in _REPOSITORY_HARNESS_EXPORTS:
        return _repository_harness_export(name)
    raise AttributeError(name)


def _data_export(name: str) -> Any:
    """Import one ``data`` export, naming the ``testing`` extra when polyfactory is missing."""
    try:
        data = importlib.import_module("loom.testing.data")
    except ImportError as exc:
        raise ImportError(
            f"{name} requires polyfactory. Install loom-kernel[testing] to build test data."
        ) from exc
    return getattr(data, name)


def _http_test_harness() -> Any:
    """Import ``HttpTestHarness``, naming the ``rest`` extra when its dependencies are missing."""
    try:
        from loom.testing.http_harness import HttpTestHarness as _HttpTestHarness
    except ImportError as exc:  # pragma: no cover
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


def _lambda_test_client() -> Any:
    """Import ``LambdaTestClient``, naming the extras it needs when they are missing."""
    try:
        from loom.testing.lambda_http import LambdaTestClient as _LambdaTestClient
    except ImportError as exc:
        raise ImportError(
            "LambdaTestClient requires the 'lambda' and 'testing' extras (mangum, httpx). "
            "Install loom-kernel[lambda,testing] to test Lambda HTTP functions."
        ) from exc
    return _LambdaTestClient
