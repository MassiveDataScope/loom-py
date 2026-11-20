import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from loom.testing.agents import ContractScenario, FakeAgentEngine, agent_engine_contract_suite
from loom.testing.golden import GoldenHarness, serialize_plan
from loom.testing.in_memory import InMemoryRepository
from loom.testing.jobs import (
    DispatchedJob,
    JobTestData,
    ManualTimer,
    NoopJob,
    RecordingJobTransport,
)
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
    "NoopJob",
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


@dataclass(frozen=True)
class _LazyExport:
    """Where a lazy export lives, and the dependencies whose absence is the caller's to fix.

    Args:
        modules: Modules to import; the export is read from the first.
        dependencies: Top-level modules an optional extra brings. A missing
            module outside them is a defect of loom or its environment and is
            raised as it is.
        hint: Message naming the extra, with ``{name}`` for the export.
    """

    modules: tuple[str, ...]
    dependencies: tuple[str, ...] = ()
    hint: str = ""


_DATA = _LazyExport(
    ("loom.testing.data", "loom.testing.job_factories", "loom.testing.store_factories"),
    ("polyfactory",),
    "{name} requires polyfactory. Install loom-kernel[testing] to build test data.",
)
_REPOSITORY_HARNESS = _LazyExport(
    ("loom.testing.repository_harness",),
    ("sqlalchemy",),
    "{name} requires SQLAlchemy. "
    "Install loom-kernel[sqlalchemy] to use repository integration harnesses.",
)
_JWT = _LazyExport(("loom.testing.jwt_auth",))
_LAZY_EXPORTS: Final = {
    "HttpTestHarness": _LazyExport(
        ("loom.testing.http_harness",),
        ("fastapi", "starlette", "pydantic", "uvicorn", "httpx"),
        "{name} requires the 'rest' extra (fastapi, pydantic, uvicorn). "
        "Install loom-kernel with the 'rest' extra to use HTTP test harnesses.",
    ),
    "LambdaTestClient": _LazyExport(
        ("loom.testing.lambda_http",),
        ("mangum", "httpx", "fastapi", "starlette"),
        "{name} requires the 'lambda' and 'testing' extras (mangum, httpx). "
        "Install loom-kernel[lambda,testing] to test Lambda HTTP functions.",
    ),
    "LambdaWorker": _LazyExport(("loom.testing.lambda_jobs",)),
    **dict.fromkeys(("LoomFactory", "make"), _DATA),
    **dict.fromkeys(("JwtTestAuth", "jwt_test_auth"), _JWT),
    **dict.fromkeys(
        ("RepositoryIntegrationHarness", "ScenarioDict", "build_repository_harness"),
        _REPOSITORY_HARNESS,
    ),
}


def __getattr__(name: str) -> Any:
    """Resolve exports whose dependencies belong to optional extras on first access."""
    export = _LAZY_EXPORTS.get(name)
    if export is None:
        raise AttributeError(name)
    return _load_export(name, export)


def _load_export(name: str, export: _LazyExport) -> Any:
    """Import the modules of *export* and return *name*.

    Raises:
        ImportError: Naming the extra to install when the missing module is one
            of the export's dependencies; any other ImportError as it is.
    """
    try:
        modules = [importlib.import_module(module) for module in export.modules]
    except ImportError as exc:
        if (exc.name or "").partition(".")[0] not in export.dependencies:
            raise
        raise ImportError(export.hint.format(name=name)) from exc
    return getattr(modules[0], name)
