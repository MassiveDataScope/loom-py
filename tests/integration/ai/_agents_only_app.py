"""Boot an agents-only application with the SQLAlchemy extra blocked (AC1).

Run by ``test_auto_without_sqlalchemy.py`` in a fresh interpreter: the first
statement installs a ``sys.meta_path`` finder that refuses ``sqlalchemy`` and
every submodule, so any module-level SQLAlchemy import on the ``create_app``
path fails loudly.  Nothing from ``conftest`` is imported: the engine provider
is defined inline so the child's import graph is exactly the one an operator
without the extra would have.

Prints ``AGENTS_ONLY_APP_OK`` and exits 0 on success; prints the traceback and
exits 1 otherwise.
"""

from __future__ import annotations

import sys
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from types import ModuleType


class _BlockSqlAlchemy(MetaPathFinder):
    """Refuse ``sqlalchemy`` and ``sqlalchemy.*`` before any other finder runs."""

    def find_spec(
        self,
        fullname: str,
        path: object = None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        if fullname == "sqlalchemy" or fullname.startswith("sqlalchemy."):
            raise ModuleNotFoundError(f"blocked for this test: {fullname}")
        return None


sys.meta_path.insert(0, _BlockSqlAlchemy())

# The finder above must be installed before any loom import.
# ruff: noqa: E402
import shutil
import tempfile
import traceback
from collections.abc import Sequence
from importlib.metadata import entry_points as _real_entry_points
from pathlib import Path
from typing import Any

import yaml

from loom.core.plugins import entrypoints as entrypoints_module
from loom.rest.fastapi.auto import create_app

SENTINEL = "AGENTS_ONLY_APP_OK"
_ENGINE_NAME = "agentsonly-fake"
_AGENT = "minimal-agent"
_GROUP = "loom.ai.engines"
_MANIFEST_MODULE = "agentsonly.manifest"

_AGENT_SPEC: dict[str, Any] = {
    "spec_version": 1,
    "name": _AGENT,
    "description": "Answers plain product questions and returns a short summary.",
    "instructions": "Answer the user question using only the conversation.",
    "output": {
        "kind": "json_schema",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["answer"],
            "properties": {"answer": {"type": "string", "description": "Short answer."}},
        },
    },
}


class _UnbuiltEngine:
    """Placeholder engine.

    ``create_app`` compiles the plans but never enters the app lifespan, which
    is where ``AgentRuntime`` builds engines, so nothing here is instantiated.
    """

    def __init__(self) -> None:
        raise AssertionError("no engine is built while create_app only compiles")


class _EngineProvider:
    """Smallest provider ``resolve_engine_provider`` accepts."""

    LOOM_AI_ENGINE_API = 2

    def create_engine(self, plan: object, *, deps: object, container: object) -> _UnbuiltEngine:
        del plan, deps, container
        return _UnbuiltEngine()

    def supported_capability_kinds(self) -> frozenset[str]:
        return frozenset({"usecase"})


class _FakeDist:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    def __init__(self) -> None:
        self.name = _ENGINE_NAME
        self.group = _GROUP
        self.dist = _FakeDist("loom-agentsonly-tests")

    def load(self) -> object:
        return _EngineProvider


class _FakeEntryPoints:
    """Fakes ``_GROUP`` only; every other group hits the real registrations.

    ``create_app`` also resolves the persistence backend (``stores.default.backend:
    none``) through the same loader, so a fake that blinds every group would
    make that lookup fail too.
    """

    def select(self, *, group: str) -> Sequence[object]:
        if group == _GROUP:
            return (_FakeEntryPoint(),)
        return tuple(_real_entry_points().select(group=group))


def _write_project(root: Path) -> str:
    """Write the ``agentsonly`` package, one agent artifact and the YAML config."""
    package = root / "agentsonly"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "manifest.py").write_text('AGENTS = ["agents/*.yaml"]\n', encoding="utf-8")
    agents = root / "agents"
    agents.mkdir()
    (agents / f"{_AGENT}.yaml").write_text(yaml.safe_dump(_AGENT_SPEC), encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "agentsonly-demo",
            "code_path": str(root),
            "discovery": {
                "mode": "manifest",
                "manifest": {"module": _MANIFEST_MODULE},
            },
        },
        "stores": {"default": {"backend": "none"}},
        "ai": {
            "engine": _ENGINE_NAME,
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
        },
    }
    config_path = root / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _assert_block_effective() -> None:
    try:
        import loom.core.backend  # noqa: F401
    except ModuleNotFoundError:
        return
    raise RuntimeError("loom.core.backend imported although sqlalchemy is blocked")


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="loom-agentsonly-"))
    try:
        entrypoints_module.entry_points = _FakeEntryPoints  # type: ignore[attr-defined]
        config_path = _write_project(root)
        app = create_app(config_path)
        if app is None:
            raise RuntimeError("create_app returned no application")
        _assert_block_effective()
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print(SENTINEL)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
