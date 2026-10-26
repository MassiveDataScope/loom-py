"""Operational sizes and timeouts where zero has no valid meaning are above zero.

A pool, a thread count or a timeout of zero or below fails when the section
decodes, naming the field.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import msgspec
import pytest

from loom.core.config.convert import convert_config
from loom.core.connections import ConnectionPool
from loom.core.persistence.database import PoolConfig
from loom.core.sql.config import SqlConnectionConfig
from tests.helpers.version_limited_extras import require_bytewax

_SQL_REQUIRED = {"backend": "clickhouse", "url": "http://localhost:8123"}


def _sql() -> type[Any]:
    return SqlConnectionConfig


def _pool() -> type[Any]:
    return PoolConfig


def _connection_pool() -> type[Any]:
    return ConnectionPool


def _celery_runtime() -> type[Any]:
    pytest.importorskip("celery")
    # Imported after the guard: celery is an optional extra.
    from loom.celery.config import CeleryRuntimeConfig

    return CeleryRuntimeConfig


def _bytewax_runtime() -> type[Any]:
    require_bytewax()
    # Imported after the guard: bytewax installs only on Python < 3.13.
    from loom.streaming.bytewax.runner import BytewaxRuntimeConfig

    return BytewaxRuntimeConfig


@pytest.mark.parametrize("value", [0, -1])
@pytest.mark.parametrize(
    ("struct", "required", "field"),
    [
        (_connection_pool, {}, "size"),
        (_sql, _SQL_REQUIRED, "executor_threads"),
        (_sql, _SQL_REQUIRED, "max_execution_time"),
        (_sql, _SQL_REQUIRED, "connect_timeout"),
        (_sql, _SQL_REQUIRED, "read_timeout"),
        (_pool, {}, "size"),
        (_pool, {}, "timeout"),
        (_pool, {}, "recycle"),
        (_celery_runtime, {}, "shutdown_timeout_ms"),
        (_bytewax_runtime, {}, "epoch_interval_ms"),
        (_bytewax_runtime, {}, "force_shutdown_timeout_ms"),
    ],
)
def test_zero_or_below_is_refused_at_decode_naming_the_field(
    struct: Callable[[], type[Any]], required: dict[str, Any], field: str, value: int
) -> None:
    with pytest.raises(msgspec.ValidationError, match=field):
        convert_config({**required, field: value}, struct(), strict=True)


def test_a_negative_pool_overflow_is_refused_at_decode_naming_the_field() -> None:
    with pytest.raises(msgspec.ValidationError, match="max_overflow"):
        msgspec.convert({"max_overflow": -1}, PoolConfig)


def test_a_zero_pool_overflow_is_accepted() -> None:
    assert msgspec.convert({"max_overflow": 0}, PoolConfig).max_overflow == 0
