"""Serialization matrix tests for the SQL envelope enc_hook (spec §3/§7.8)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.stores.sql.abc import SqlColumn, SqlQueryResult
from loom.core.stores.sql.config import SqlEndpointConfig
from loom.core.stores.sql.service import SqlQueryService
from loom.rest.fastapi.sql import mount_sql_endpoints
from tests.unit.core.sql._fakes import (
    FakeSqlExecutor,
    make_connection_config,
    make_sql_config,
)


class _ExoticPoint:
    """Type unsupported by msgspec: must fall back to ``str()``."""

    def __str__(self) -> str:
        return "point(1,2)"


def _single_value_result(value: Any, column_type: str = "Dynamic") -> SqlQueryResult:
    return SqlQueryResult(
        columns=(SqlColumn(name="v", type=column_type),),
        rows=((value,),),
        row_count=1,
        limit=10,
        offset=0,
        has_more=False,
        elapsed_ms=0.1,
    )


def _post(result: SqlQueryResult) -> Any:
    """Mount the endpoint with a fake executor returning *result* and POST to it."""
    config = make_sql_config(
        analytics=make_connection_config(
            allowed_roles=(),
            sql_endpoint=SqlEndpointConfig(enabled=True, auth="external"),
        )
    )
    app = FastAPI()
    service = SqlQueryService(
        executors={"analytics": FakeSqlExecutor(result=result)}, config=config
    )
    mount_sql_endpoints(app, service=service, config=config)
    client = TestClient(app, raise_server_exceptions=False)
    return client.post("/sql/analytics", json={"sql": "SELECT v"})


class TestSqlResultSerialization:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(datetime(2024, 1, 2, 3, 4, 5), "2024-01-02T03:04:05", id="datetime-naive"),
            pytest.param(
                datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
                "2024-01-02T03:04:05Z",
                id="datetime-utc",
            ),
            pytest.param(date(2024, 1, 2), "2024-01-02", id="date"),
            pytest.param(Decimal("123.45"), "123.45", id="decimal"),
            pytest.param(
                UUID("12345678-1234-5678-1234-567812345678"),
                "12345678-1234-5678-1234-567812345678",
                id="uuid",
            ),
            pytest.param(IPv4Address("10.0.0.1"), "10.0.0.1", id="ipv4"),
            pytest.param(IPv6Address("2001:db8::1"), "2001:db8::1", id="ipv6"),
            pytest.param(None, None, id="null"),
            pytest.param([1, 2, 3], [1, 2, 3], id="array"),
            pytest.param(b"\x01\x02", "AQI=", id="bytes-base64"),
        ],
    )
    def test_serializes_each_matrix_type_inside_a_row(self, value: Any, expected: Any) -> None:
        """Every type of the spec matrix produces the expected JSON in ``rows``."""
        response = _post(_single_value_result(value))
        assert response.json()["rows"][0][0] == expected

    def test_exotic_type_falls_back_to_str_without_a_500(
        self,
    ) -> None:
        """An unknown type never breaks the response: the enc_hook falls back to ``str()``."""
        response = _post(_single_value_result(_ExoticPoint(), column_type="Point"))
        assert (response.status_code, response.json()["rows"][0][0]) == (200, "point(1,2)")

    def test_columns_keep_backend_native_name_and_type(
        self,
    ) -> None:
        """``columns`` exposes the backend native name and type verbatim (spec §3)."""
        response = _post(
            _single_value_result(datetime(2024, 1, 2, 3, 4, 5), column_type="DateTime64(3)")
        )
        assert response.json()["columns"] == [{"name": "v", "type": "DateTime64(3)"}]
