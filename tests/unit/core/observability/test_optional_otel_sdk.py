"""Guards that keep ``loom.core.observability`` usable without the OTEL SDK.

``opentelemetry-sdk`` and the OTLP exporters are extras-only dependencies.
Core must import, build a runtime, and emit spans with nothing but
``opentelemetry-api`` installed. These tests fail loudly if an SDK import
creeps back into the core import graph, and they pin what a core-only user
actually sees when they ask for OTEL export anyway.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import loom

_BLOCKED_PREFIXES = ("opentelemetry.sdk", "opentelemetry.exporter")

_CORE_ROOT = Path(loom.__file__).parent / "core"

# Modules that subclass or configure SDK types and therefore cannot defer their
# own imports. They are safe only because nothing imports them at module scope,
# which ``test_sdk_only_modules_are_never_imported_at_module_scope`` enforces.
_SDK_ONLY_MODULES = frozenset({"otel_ids.py"})

# Makes ``opentelemetry.sdk`` and the OTLP exporters unimportable for the rest
# of the interpreter, reproducing a core-only install inside a fresh process.
_BLOCKER_PREAMBLE = """
import sys
from importlib.machinery import ModuleSpec

BLOCKED = ("opentelemetry.sdk", "opentelemetry.exporter")


class _BlockingFinder:
    def find_spec(self, fullname, path=None, target=None) -> ModuleSpec | None:
        if any(fullname == b or fullname.startswith(b + ".") for b in BLOCKED):
            raise ModuleNotFoundError(f"blocked by test: {fullname}", name=fullname)
        return None


sys.meta_path.insert(0, _BlockingFinder())


def sdk_was_imported() -> bool:
    return any(
        name == b or name.startswith(b + ".") for name in sys.modules for b in BLOCKED
    )
"""


def _run_without_sdk(body: str) -> subprocess.CompletedProcess[str]:
    """Run ``body`` in a fresh interpreter where the OTEL SDK cannot be imported."""
    env = {**os.environ, "PYTHONPATH": str(Path(loom.__file__).parents[1])}
    return subprocess.run(
        [sys.executable, "-c", _BLOCKER_PREAMBLE + body],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def _marked_lines(result: subprocess.CompletedProcess[str], key: str) -> list[str]:
    """Return the values the subprocess printed under ``key``.

    The structlog observer writes to stdout too, so results are tagged rather
    than read positionally.
    """
    assert result.returncode == 0, (
        f"core-only subprocess failed.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    prefix = f"{key}:"
    values = [line[len(prefix) :] for line in result.stdout.splitlines() if line.startswith(prefix)]
    assert values, f"subprocess printed no {key!r} line.\nstdout:\n{result.stdout}"
    return values


class TestImportsWithoutOtelSdk:
    def test_core_observability_imports_and_runs_without_the_sdk(self) -> None:
        result = _run_without_sdk(
            """
import loom.core.observability as obs

runtime = obs.ObservabilityRuntime.from_config(obs.ObservabilityConfig())
with runtime.span(obs.Scope.JOB, "sdk-less"):
    pass

assert not sdk_was_imported(), "core imported the OTEL SDK on a core-only install"
print("OBSERVERS:" + str([type(o).__name__ for o in runtime.observers]))
"""
        )

        # The default config builds exactly the structlog observer: no OTEL
        # observer, and nothing that needs the SDK.
        assert _marked_lines(result, "OBSERVERS") == ["['StructlogLifecycleObserver']"]

    def test_otel_with_shared_global_provider_works_without_the_sdk(self) -> None:
        """An absent endpoint shares whatever provider the process already has.

        That path only touches ``opentelemetry-api``, so a core-only install can
        enable OTEL and emit spans — they land in the API's no-op tracer unless
        an SDK distribution (logfire, an operator agent) installed a real one.
        """
        result = _run_without_sdk(
            """
from loom.core.config.observability import OtelConfig
from loom.core.observability import ObservabilityConfig, ObservabilityRuntime, Scope
from loom.core.observability.config import OtelObservabilityConfig

config = ObservabilityConfig(
    otel=OtelObservabilityConfig(enabled=True, config=OtelConfig())
)
runtime = ObservabilityRuntime.from_config(config)
with runtime.span(Scope.JOB, "sdk-less"):
    pass

assert not sdk_was_imported(), "the shared-provider path must not need the SDK"
print("OBSERVERS:" + str([type(o).__name__ for o in runtime.observers]))
print("TRACER:" + type(runtime.tracer).__name__)
"""
        )

        assert _marked_lines(result, "OBSERVERS") == ["['StructlogLifecycleObserver']"]
        # The API's proxy, not the API's no-op: it resolves to the host's
        # provider whenever one is installed, without importing the SDK here.
        assert _marked_lines(result, "TRACER") == ["ProxyTracer"]


class TestOtelRequestedWithoutTheSdk:
    """Pins what a core-only user hits when they ask for real OTEL export.

    Neither failure is silent, but they are not interchangeable: the exporter
    is loaded before the SDK in ``_build_tracer``, so the exporter's
    ``ValueError`` — not the SDK's ``ImportError`` — is what surfaces first for
    span export. A later PR reworks ``from_config``; if these messages change,
    that change should be deliberate.
    """

    def test_span_export_fails_on_the_missing_exporter_not_the_missing_sdk(self) -> None:
        result = _run_without_sdk(
            """
from loom.core.config.observability import OtelConfig
from loom.core.model.redaction import Dsn
from loom.core.observability import ObservabilityConfig, ObservabilityRuntime
from loom.core.observability.config import OtelObservabilityConfig

config = ObservabilityConfig(
    otel=OtelObservabilityConfig(
        enabled=True,
        config=OtelConfig(endpoint=Dsn("http://collector:4318/v1/traces")),
    )
)
try:
    ObservabilityRuntime.from_config(config)
except Exception as exc:
    print("ERROR_TYPE:" + type(exc).__name__)
    print("ERROR_MSG:" + str(exc))
else:
    print("ERROR_TYPE:NONE")
    print("ERROR_MSG:NONE")
"""
        )

        assert _marked_lines(result, "ERROR_TYPE") == ["ValueError"]
        assert _marked_lines(result, "ERROR_MSG") == [
            "OTel protocol='http/protobuf' requires 'opentelemetry-exporter-otlp-proto-http'."
        ]

    def test_log_export_fails_on_the_missing_sdk(self) -> None:
        result = _run_without_sdk(
            """
from loom.core.config.observability import OtelConfig
from loom.core.logger.config import LoggerConfig
from loom.core.observability import ObservabilityConfig, ObservabilityRuntime
from loom.core.observability.config import LogObservabilityConfig, OtelObservabilityConfig

config = ObservabilityConfig(
    log=LogObservabilityConfig(enabled=True, config=LoggerConfig()),
    otel=OtelObservabilityConfig(
        enabled=True, export_logs=True, config=OtelConfig()
    ),
)
try:
    ObservabilityRuntime.from_config(config)
except Exception as exc:
    print("ERROR_TYPE:" + type(exc).__name__)
    print("ERROR_MSG:" + str(exc))
else:
    print("ERROR_TYPE:NONE")
    print("ERROR_MSG:NONE")
"""
        )

        assert _marked_lines(result, "ERROR_TYPE") == ["ImportError"]
        assert _marked_lines(result, "ERROR_MSG") == [
            "OTEL span and log export requires 'opentelemetry-sdk'. "
            "Install it with: pip install 'loom-py[etl-otel]'"
        ]


def _module_scope_imports(tree: ast.Module) -> list[ast.Import | ast.ImportFrom]:
    """Collect imports executed at module import time.

    Function bodies are skipped — a deferred import is the fix, not the
    defect — and so are ``if TYPE_CHECKING:`` blocks, which never run. Class
    bodies are *not* skipped: they execute on import like any other
    module-scope statement.
    """
    found: list[ast.Import | ast.ImportFrom] = []

    def visit(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if isinstance(node, ast.Import | ast.ImportFrom):
                found.append(node)
                continue
            if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
                visit(node.orelse)
                continue
            for child_body in _nested_bodies(node):
                visit(child_body)

    visit(tree.body)
    return found


def _is_type_checking_guard(test: ast.expr) -> bool:
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


def _nested_bodies(node: ast.stmt) -> list[list[ast.stmt]]:
    bodies: list[list[ast.stmt]] = []
    for field in ("body", "orelse", "finalbody"):
        value = getattr(node, field, None)
        if isinstance(value, list):
            bodies.append(value)
    for handler in getattr(node, "handlers", []):
        bodies.append(handler.body)
    for case in getattr(node, "cases", []):  # ast.Match
        bodies.append(case.body)
    return bodies


def _imported_modules(node: ast.Import | ast.ImportFrom) -> list[str]:
    """Return every module an import statement pulls in.

    ``from X import Y`` binds ``X.Y`` when ``Y`` is a submodule, so the dotted
    child is reported alongside ``X``: ``from opentelemetry import sdk`` is a
    module-scope SDK import even though its ``module`` field is only
    ``"opentelemetry"``. That idiom is already used in this package
    (``from opentelemetry import _logs``), so ignoring it would leave the
    check blind to a real regression.
    """
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if node.level or node.module is None:
        return []
    return [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]


class TestCoreImportHygiene:
    def test_no_core_module_imports_the_otel_sdk_at_module_scope(self) -> None:
        offenders: list[str] = []
        scanned = sorted(_CORE_ROOT.rglob("*.py"))
        assert scanned, f"no source files found under {_CORE_ROOT}"
        for path in scanned:
            if path.name in _SDK_ONLY_MODULES:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in _module_scope_imports(tree):
                for module in _imported_modules(node):
                    if module.startswith(_BLOCKED_PREFIXES):
                        offenders.append(f"{path.name}:{node.lineno} -> {module}")

        assert not offenders, (
            "Modules under 'loom.core' must not import the OpenTelemetry SDK or the "
            "OTLP exporters at module scope — both are extras-only, so a core-only "
            f"install would fail to import. Offenders: {offenders}"
        )

    def test_sdk_only_modules_are_never_imported_at_module_scope(self) -> None:
        """The exemption above holds only while nothing imports those modules eagerly.

        ``otel_ids`` subclasses an SDK type, so it cannot defer its own
        imports. It stays harmless only because every reference to it sits
        inside a function body. If one moves to module scope, the exemption
        silently becomes a core-only import failure — which is what this pins.
        """
        exempt_paths = {
            f"loom.core.observability.{name.removesuffix('.py')}" for name in _SDK_ONLY_MODULES
        }
        offenders: list[str] = []
        for path in sorted(_CORE_ROOT.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in _module_scope_imports(tree):
                for module in _imported_modules(node):
                    if module in exempt_paths:
                        offenders.append(f"{path.name}:{node.lineno} -> {module}")

        assert not offenders, (
            "SDK-only modules must be imported from inside a function so a core-only "
            f"install never loads them. Offenders: {offenders}"
        )
