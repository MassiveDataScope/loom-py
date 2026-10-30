"""Celery application started with ``--pool=prefork``.

The test process does not import this module. The worker process does, and
that import is the parent: it builds the engine, records that the pool is
still empty, and only then does Celery fork the children. Every connection
names the process that opened it in ``application_name``, and each run of the
startup checks is recorded, so the test can tell the parent's work from the
children's.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from celery import Celery
from sqlalchemy import event, text

import loom.core.repository.sqlalchemy.backend as sqlalchemy_backend
from loom.celery.bootstrap import bootstrap_worker
from loom.core.environment import Environment
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.model.types import Integer
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import get_active_session


class _Beat(BaseModel):
    __tablename__ = "prefork_beats"
    id: int = ColumnField(Integer, primary_key=True)


CHILDREN = int(os.environ.get("LOOM_PREFORK_CHILDREN", "2"))
_CONNECTIONS = (
    "SELECT application_name FROM pg_stat_activity "
    "WHERE datname = current_database() AND application_name LIKE 'prefork-%'"
)


class _PingJob(Job[dict[str, Any]]):
    async def execute(self) -> dict[str, Any]:
        session = get_active_session()
        if session is None:
            raise RuntimeError("prefork child has no unit-of-work session")
        one = int((await session.execute(text("SELECT 1"))).scalar_one())
        folder = Path(os.environ["LOOM_PREFORK_PIDS"])
        folder.mkdir(parents=True, exist_ok=True)
        (folder / str(os.getpid())).write_text("1")
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and len(list(folder.iterdir())) < CHILDREN:
            await asyncio.sleep(0.05)
        connections = (await session.execute(text(_CONNECTIONS))).scalars().all()
        return {
            "pid": os.getpid(),
            "one": one,
            "children": len(list(folder.iterdir())),
            "connections": list(connections),
        }


def _record_startup_checks(report: Path) -> None:
    """Append this process's pid to *report* each time the startup checks run."""
    run_checks = sqlalchemy_backend._run_startup_checks

    async def recorded(*args: Any, **kwargs: Any) -> None:
        with report.open("a") as handle:
            handle.write(f"{os.getpid()}\n")
        await run_checks(*args, **kwargs)

    sqlalchemy_backend._run_startup_checks = recorded  # type: ignore[assignment]


def _name_connections(manager: SessionManager) -> None:
    """Label every new connection with the pid of the process that opens it."""

    @event.listens_for(manager.engine.sync_engine, "do_connect")
    def _label(dialect: Any, record: Any, cargs: Any, cparams: dict[str, Any]) -> None:
        settings = cparams.setdefault("server_settings", {})
        settings["application_name"] = f"prefork-{os.getpid()}"


def _celery_app() -> Celery:
    config = os.environ.get("LOOM_PREFORK_CONFIG")
    if not config:
        return Celery("loom-prefork")
    checks = os.environ.get("LOOM_PREFORK_CHECKS")
    if checks:
        _record_startup_checks(Path(checks))
    result = bootstrap_worker(config, jobs=[_PingJob], environment=Environment.TEST)
    manager = result.container.resolve(SessionManager)
    _name_connections(manager)
    pool = manager.engine.sync_engine.pool
    report = Path(os.environ["LOOM_PREFORK_REPORT"])
    report.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "pool": type(pool).__name__,
                "checkedout": pool.checkedout(),
                "checkedin": pool.checkedin(),
            }
        )
    )
    return result.celery_app


celery_app = _celery_app()
