"""Benchmark the connection-config hot spots: app startup and per-operation cost.

Measures, each in a fresh interpreter so import caches never leak between runs:

* ``startup``: importing ``loom.rest.fastapi.auto`` plus ``create_app()`` for a
  one-model application with no ``celery`` section.
* ``db_op``: the median cost of opening a session and running ``SELECT 1``
  against a SQLite file through :class:`SessionManager`, the path every
  repository call takes.

Run it on two commits and compare the medians, or point the children at
another checkout's ``src`` to compare in one session::

    uv run python scripts/bench_connection_config.py
    LOOM_BENCH_SRC=../baseline/src uv run python scripts/bench_connection_config.py
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any

_STARTUP_RUNS = 10
_DB_OPS = 2000

_APP_MODULE = """\
from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase


class BenchItem(BaseModel):
    __tablename__ = "bench_items"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=40)


class GetBenchItem(UseCase[BenchItem, BenchItem | None]):
    async def execute(self, item_id: int, **kwargs: Any) -> BenchItem | None:
        return await self.main_repo.get_by_id(item_id)
"""

_APP_CONFIG = """\
app:
  name: bench
  code_path: "."
  discovery:
    mode: modules
    modules:
      include: [bench_app]
database:
  url: "sqlite+aiosqlite:///{db_path}"
persistence:
  cursors:
    signing: ephemeral
"""

_STARTUP_CHILD = textwrap.dedent(
    """
    import json, sys, time, warnings
    warnings.simplefilter("ignore")
    started = time.perf_counter()
    from loom.rest.fastapi.auto import create_app
    imported = time.perf_counter()
    create_app(sys.argv[1])
    built = time.perf_counter()
    print(json.dumps({
        "import_ms": (imported - started) * 1000,
        "create_app_ms": (built - imported) * 1000,
        "celery_imported": "loom.celery" in sys.modules,
    }))
    """
)

_DB_CHILD = textwrap.dedent(
    """
    import asyncio, json, sys, time
    from sqlalchemy import text
    from loom.core.repository.sqlalchemy.session_manager import SessionManager

    async def main() -> list[float]:
        manager = SessionManager(sys.argv[1])
        samples = []
        for _ in range(int(sys.argv[2])):
            started = time.perf_counter()
            async with manager.session() as session:
                await session.execute(text("SELECT 1"))
            samples.append((time.perf_counter() - started) * 1_000_000)
        await manager.dispose()
        return samples

    samples = asyncio.run(main())
    samples.sort()
    print(json.dumps({"db_op_median_us": samples[len(samples) // 2]}))
    """
)


def _run_child(source: str, *args: str, cwd: Path) -> dict[str, Any]:
    # LOOM_BENCH_SRC points the children at another checkout's src (A/B runs).
    paths = [os.environ["LOOM_BENCH_SRC"]] if os.environ.get("LOOM_BENCH_SRC") else []
    env = {
        **os.environ,
        "LOOM_ENV": "development",
        "PYTHONPATH": os.pathsep.join([*paths, str(cwd)]),
    }
    completed = subprocess.run(
        [sys.executable, "-c", source, *args],
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit(f"benchmark child failed:\n{completed.stderr}")
    return json.loads(completed.stdout.strip().splitlines()[-1])


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="loom-bench-") as tmp:
        project = Path(tmp)
        (project / "bench_app.py").write_text(_APP_MODULE, encoding="utf-8")
        config = project / "app.yaml"
        config.write_text(_APP_CONFIG.format(db_path=project / "bench.db"), encoding="utf-8")

        startups = [
            _run_child(_STARTUP_CHILD, str(config), cwd=project) for _ in range(_STARTUP_RUNS)
        ]
        db = _run_child(
            _DB_CHILD, f"sqlite+aiosqlite:///{project / 'ops.db'}", str(_DB_OPS), cwd=project
        )

    report = {
        "import_ms": statistics.median(float(run["import_ms"]) for run in startups),
        "create_app_ms": statistics.median(float(run["create_app_ms"]) for run in startups),
        "celery_imported": any(bool(run["celery_imported"]) for run in startups),
        "db_op_median_us": db["db_op_median_us"],
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
