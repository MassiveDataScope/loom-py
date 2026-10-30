"""A real Celery prefork worker opens its database connections in the children."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


@dataclass(frozen=True)
class _Run:
    """What one prefork worker run reported."""

    parent: dict[str, Any]
    answers: list[dict[str, Any]]
    checks: list[int]


def _run_prefork_worker(tmp_path: Path, children: int) -> _Run:
    """Start a real prefork worker with *children* processes and run one job in each."""
    database = _skip_without_services()
    config = tmp_path / "worker.yaml"
    report = tmp_path / "parent.json"
    checks = tmp_path / "checks"
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
    env["LOOM_PREFORK_CHECKS"] = str(checks)
    env["LOOM_PREFORK_CHILDREN"] = str(children)
    env["PYTHONPATH"] = os.pathsep.join([str(root), str(root / "src"), env.get("PYTHONPATH", "")])
    command = [
        str(root / ".venv" / "bin" / "celery"),
        "-A",
        "tests.integration.celery_bootstrap.prefork_worker_app:celery_app",
        "worker",
        "--pool=prefork",
        f"--concurrency={children}",
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
        results = [client.send_task("loom.job._PingJob", queue="default") for _ in range(children)]
        answers = [item.get(timeout=30, disable_sync_subtasks=False) for item in results]
    finally:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
    recorded = [int(line) for line in checks.read_text().split()] if checks.exists() else []
    return _Run(parent=parent, answers=answers, checks=recorded)


def test_prefork_children_open_their_own_connections(tmp_path: Path) -> None:
    run = _run_prefork_worker(tmp_path, children=2)
    parent, answers = run.parent, run.answers

    assert parent["pool"] == "AsyncAdaptedQueuePool"
    assert parent["checkedout"] == 0
    assert parent["checkedin"] == 0
    assert {answer["one"] for answer in answers} == {1}
    child_pids = {answer["pid"] for answer in answers}
    assert len(child_pids) == 2
    assert parent["pid"] not in child_pids


def test_eight_children_open_after_one_check_in_the_parent(tmp_path: Path) -> None:
    """SC-006a: the parent checks once and none of its connections serves a child."""
    run = _run_prefork_worker(tmp_path, children=8)
    parent_pid = run.parent["pid"]

    assert run.checks == [parent_pid]
    assert len({answer["pid"] for answer in run.answers}) == 8
    assert {answer["one"] for answer in run.answers} == {1}
    seen = {name for answer in run.answers for name in answer["connections"]}
    assert f"prefork-{parent_pid}" not in seen
    assert {f"prefork-{answer['pid']}" for answer in run.answers} <= seen
