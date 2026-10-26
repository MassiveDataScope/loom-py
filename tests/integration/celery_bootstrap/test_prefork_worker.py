"""A real Celery prefork worker opens its database connections in the children."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import pytest
import redis
import yaml
from celery import Celery

URI_ENV = "LOOM_PG_IT_URI"
BROKER = "redis://127.0.0.1:6379/15"

pytestmark = pytest.mark.integration


def _skip_without_services() -> str:
    database = os.environ.get(URI_ENV)
    if not database:
        pytest.skip(f"{URI_ENV} is not set")
    client = redis.Redis.from_url(BROKER, socket_connect_timeout=1)
    try:
        client.ping()
    except redis.RedisError as exc:
        pytest.skip(f"Redis at {BROKER} is not reachable ({exc})")
    client.flushdb()
    return database


def _ready(log: Path, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(log.read_text()[-4000:])
        if " ready." in log.read_text():
            return
        time.sleep(0.1)
    raise AssertionError(log.read_text()[-4000:])


def test_prefork_children_open_their_own_connections(tmp_path: Path) -> None:
    database = _skip_without_services()
    config = tmp_path / "worker.yaml"
    report = tmp_path / "parent.json"
    pids = tmp_path / "pids"
    log = tmp_path / "worker.log"
    pids.mkdir()
    config.write_text(
        yaml.dump(
            {
                "app": {
                    "name": "prefork",
                    "discovery": {
                        "mode": "modules",
                        "modules": {
                            "include": ["tests.integration.celery_bootstrap.prefork_worker_app"]
                        },
                    },
                },
                "celery": {
                    "broker_url": BROKER,
                    "result_backend": BROKER,
                    "task_always_eager": False,
                    "worker_prefetch_multiplier": 1,
                },
                "database": {"url": database},
                "persistence": {"cursors": {"signing": "ephemeral"}},
            }
        )
    )
    root = Path(__file__).resolve().parents[3]
    env = os.environ.copy()
    env["LOOM_ENV"] = "test"
    env["LOOM_PREFORK_CONFIG"] = str(config)
    env["LOOM_PREFORK_REPORT"] = str(report)
    env["LOOM_PREFORK_PIDS"] = str(pids)
    env["PYTHONPATH"] = os.pathsep.join([str(root), str(root / "src"), env.get("PYTHONPATH", "")])
    command = [
        str(root / ".venv" / "bin" / "celery"),
        "-A",
        "tests.integration.celery_bootstrap.prefork_worker_app:celery_app",
        "worker",
        "--pool=prefork",
        "--concurrency=2",
        "--loglevel=INFO",
        "--without-gossip",
        "--without-mingle",
        "--without-heartbeat",
    ]
    with log.open("w") as handle:
        process = subprocess.Popen(
            command,
            cwd=root,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
    try:
        _ready(log, process)
        parent = json.loads(report.read_text())
        client = Celery(broker=BROKER, backend=BROKER)
        results = [client.send_task("loom.job._PingJob", queue="default") for _ in range(2)]
        answers = [item.get(timeout=30, disable_sync_subtasks=False) for item in results]
    finally:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)

    assert parent["pool"] == "AsyncAdaptedQueuePool"
    assert parent["checkedout"] == 0
    assert parent["checkedin"] == 0
    assert {answer["one"] for answer in answers} == {1}
    child_pids = {answer["pid"] for answer in answers}
    assert len(child_pids) == 2
    assert parent["pid"] not in child_pids
