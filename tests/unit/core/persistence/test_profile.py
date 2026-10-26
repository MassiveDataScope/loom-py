"""Runtime profiles: the pool defaults each entry point contributes."""

from __future__ import annotations

import subprocess
import sys

import pytest

from loom.core.persistence.profile import (
    SERVER_PROFILE,
    STREAMING_PROFILE,
    WORKER_PROFILE,
    BackendRuntime,
    PoolDefaults,
    RuntimeProfile,
)


def test_server_profile_sizes_the_pool_for_a_concurrent_process() -> None:
    assert SERVER_PROFILE.name == "server"
    assert SERVER_PROFILE.pool == PoolDefaults(
        size=5, max_overflow=5, timeout_s=10, recycle_s=1800, pre_ping=True
    )


def test_worker_profile_sizes_the_pool_for_one_task_per_process() -> None:
    assert WORKER_PROFILE.name == "worker"
    assert WORKER_PROFILE.pool == PoolDefaults(
        size=2, max_overflow=0, timeout_s=30, recycle_s=1800, pre_ping=True
    )


def test_streaming_profile_shares_the_worker_pool_under_its_own_name() -> None:
    assert STREAMING_PROFILE.name == "streaming"
    assert STREAMING_PROFILE.pool == WORKER_PROFILE.pool


@pytest.mark.parametrize(
    "field_values",
    [
        {"size": 0},
        {"max_overflow": -1},
        {"timeout_s": 0},
        {"recycle_s": 0},
    ],
)
def test_pool_defaults_refuse_a_value_with_no_valid_meaning(field_values: dict[str, int]) -> None:
    values = {"size": 1, "max_overflow": 0, "timeout_s": 1, "recycle_s": 1, "pre_ping": True}
    with pytest.raises(ValueError, match=next(iter(field_values))):
        PoolDefaults(**{**values, **field_values})


def test_a_new_profile_needs_nothing_but_its_own_values() -> None:
    lambda_profile = RuntimeProfile(
        name="lambda",
        pool=PoolDefaults(size=1, max_overflow=0, timeout_s=5, recycle_s=300, pre_ping=True),
    )

    assert lambda_profile.pool.size == 1
    assert SERVER_PROFILE.pool.size == 5


def test_backend_runtime_carries_no_metrics_unless_given() -> None:
    assert BackendRuntime(SERVER_PROFILE).pool_metrics is None


def test_the_profile_module_imports_without_sqlalchemy() -> None:
    script = (
        "import sys\n"
        "import loom.core.persistence.profile\n"
        "import loom.core.persistence.database\n"
        "leaked = sorted(m for m in sys.modules if 'sqlalchemy' in m)\n"
        "assert not leaked, leaked\n"
    )
    subprocess.run([sys.executable, "-c", script], check=True)
