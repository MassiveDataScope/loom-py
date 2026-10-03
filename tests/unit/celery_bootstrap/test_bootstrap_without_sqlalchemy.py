"""Worker bootstrap without SQLAlchemy installed.

The startup check runs in a clean subprocess so that modules imported by
unrelated suites inside the pytest interpreter cannot mask a dependency leak.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

import loom.celery.bootstrap as boot

_SRC = Path(__file__).resolve().parents[3] / "src"

_CREATE_APP_SCRIPT = """
import sys
sys.modules["sqlalchemy"] = None

from loom.celery.auto import create_app
from loom.core.job.job import Job


class PureJob(Job[int]):
    def execute(self, value: int = 0) -> int:
        return value * 2


app = create_app(sys.argv[1], jobs=[PureJob])
assert any(name.endswith("PureJob") for name in app.tasks), sorted(app.tasks)
"""


def _run_in_clean_interpreter(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run ``script`` in a fresh interpreter that can see the repository ``src``."""
    env = {**os.environ, "PYTHONPATH": str(_SRC)}
    return subprocess.run(
        [sys.executable, "-c", script, *args],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def _write_worker_config(tmp_path: Path, extra: dict[str, Any] | None = None) -> Path:
    """Write a worker YAML config with an in-memory broker and no database section."""
    cfg: dict[str, Any] = {
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
        **(extra or {}),
    }
    config_path = tmp_path / "worker.yaml"
    config_path.write_text(yaml.dump(cfg))
    return config_path


def test_create_app_without_database_section_starts_without_sqlalchemy(tmp_path: Path) -> None:
    """A worker whose config has no ``database`` section boots with ``sqlalchemy`` hidden."""
    config_path = _write_worker_config(tmp_path)

    result = _run_in_clean_interpreter(_CREATE_APP_SCRIPT, str(config_path))

    assert result.returncode == 0, f"create_app failed:\n{result.stderr}"


def test_resolve_uow_factory_names_sqlalchemy_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``database`` section without the SQLAlchemy extra raises a hinting ImportError."""
    monkeypatch.setitem(sys.modules, "loom.core.repository.sqlalchemy.session_manager", None)

    with pytest.raises(ImportError, match=r"loom-kernel\[sqlalchemy\]"):
        boot._resolve_uow_factory({"database": {"url": "sqlite+aiosqlite:///test.db"}})


def test_compile_db_layer_names_sqlalchemy_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compiling models for a configured database without the extra raises a hinting ImportError."""
    monkeypatch.setitem(sys.modules, "loom.core.backend.sqlalchemy", None)

    with pytest.raises(ImportError, match=r"loom-kernel\[sqlalchemy\]"):
        boot._compile_db_layer(
            object(),  # type: ignore[arg-type]
            [boot.BaseModel],
            (),
            naming_convention=None,
        )
