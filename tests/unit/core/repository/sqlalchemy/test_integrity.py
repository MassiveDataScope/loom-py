"""``classify_integrity_error`` reads PostgreSQL diagnostics from the driver exception."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from loom.core.errors import Conflict
from loom.core.repository.sqlalchemy.integrity import classify_integrity_error


class _AsyncpgCause(Exception):
    def __init__(self, column_name: str, constraint_name: str) -> None:
        super().__init__("violation")
        self.column_name = column_name
        self.constraint_name = constraint_name


class _AdaptedAsyncpgError(Exception):
    def __init__(self, sqlstate: str, cause: _AsyncpgCause) -> None:
        super().__init__("violation")
        self.sqlstate = sqlstate
        self.__cause__ = cause


class _BareError(Exception):
    sqlstate = "23502"


class _Diag:
    column_name = "name"
    constraint_name = "assets_name_key"
    message_detail = ""


class _PsycopgError(Exception):
    pgcode = "23502"
    diag = _Diag()


def _asyncpg_error(sqlstate: str) -> IntegrityError:
    cause = _AsyncpgCause(column_name="name", constraint_name="assets_name_key")
    return IntegrityError("INSERT", {}, _AdaptedAsyncpgError(sqlstate, cause))


class TestPostgresNotNullColumn:
    def test_asyncpg_names_the_column(self) -> None:
        error = classify_integrity_error(_asyncpg_error("23502"))

        assert isinstance(error, Conflict)
        assert error.message == "Field 'name' is required."

    def test_psycopg_names_the_column(self) -> None:
        error = classify_integrity_error(IntegrityError("INSERT", {}, _PsycopgError()))

        assert error.message == "Field 'name' is required."

    def test_a_driver_exposing_no_column_reports_unknown(self) -> None:
        error = classify_integrity_error(IntegrityError("INSERT", {}, _BareError()))

        assert error.message == "Field 'unknown' is required."


class TestPostgresOtherViolationsKeepTheirMessage:
    @pytest.mark.parametrize(
        ("sqlstate", "message"),
        [
            ("23505", "A record with these values already exists."),
            ("23514", "Value violates check constraint."),
        ],
        ids=["unique", "check"],
    )
    def test_asyncpg_does_not_name_the_constraint(self, sqlstate: str, message: str) -> None:
        assert classify_integrity_error(_asyncpg_error(sqlstate)).message == message
