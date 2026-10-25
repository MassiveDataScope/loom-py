"""Celery application started with ``--pool=prefork``.

The test process does not import this module. The worker process does, and
that import is the parent: it builds the engine, records that the pool is
still empty, and only then does Celery fork the children.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

from celery import Celery
from sqlalchemy import text

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


class _PingJob(Job[dict[str, int]]):
    async def execute(self) -> dict[str, int]:
        session = get_active_session()
        if session is None:
            raise RuntimeError("prefork child has no unit-of-work session")
        one = int((await session.execute(text("SELECT 1"))).scalar_one())
        folder = Path(os.environ["LOOM_PREFORK_PIDS"])
        folder.mkdir(parents=True, exist_ok=True)
        (folder / str(os.getpid())).write_text("1")
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and len(list(folder.iterdir())) < 2:
            await asyncio.sleep(0.05)
        return {"pid": os.getpid(), "one": one, "children": len(list(folder.iterdir()))}


def _celery_app() -> Celery:
    config = os.environ.get("LOOM_PREFORK_CONFIG")
    if not config:
        return Celery("loom-prefork")
    result = bootstrap_worker(config, jobs=[_PingJob], environment=Environment.TEST)
    manager = result.container.resolve(SessionManager)
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
