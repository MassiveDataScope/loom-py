"""Operational sizes and timeouts a ``LOOM__`` variable may set are above zero.

Zero or a negative value makes no sense for a pool, a thread count or a
timeout, so it fails when the section decodes, naming the field.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

_SQL = ("loom.core.sql.config", "SqlConnectionConfig")
_SQL_REQUIRED = {"backend": "clickhouse", "url": "http://localhost:8123"}
_BYTEWAX = ("loom.streaming.bytewax.runner", "BytewaxRuntimeConfig")


@pytest.mark.parametrize("value", [0, -1])
@pytest.mark.parametrize(
    ("struct", "required", "field"),
    [
        (_SQL, _SQL_REQUIRED, "pool_size"),
        (_SQL, _SQL_REQUIRED, "executor_threads"),
        (_SQL, _SQL_REQUIRED, "max_execution_time"),
        (_SQL, _SQL_REQUIRED, "connect_timeout"),
        (_SQL, _SQL_REQUIRED, "send_receive_timeout"),
        (("loom.celery.config", "CeleryRuntimeConfig"), {}, "shutdown_timeout_ms"),
        (_BYTEWAX, {}, "epoch_interval_ms"),
        (_BYTEWAX, {}, "force_shutdown_timeout_ms"),
    ],
)
def test_zero_or_below_is_refused_at_decode_naming_the_field(
    struct: tuple[str, str], required: dict[str, Any], field: str, value: int
) -> None:
    module, name = struct
    target = getattr(pytest.importorskip(module, exc_type=ImportError), name)

    with pytest.raises(msgspec.ValidationError, match=field):
        msgspec.convert({**required, field: value}, target)
