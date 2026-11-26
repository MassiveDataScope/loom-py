"""How core classifies each ETL error: ValueError, code and whether a job retries it."""

from __future__ import annotations

from datetime import date

import pytest

from loom.core.errors.codes import ErrorCode
from loom.core.job.outcome import Failed
from loom.core.job.policy import Backoff, Redelivery, RetryPolicy
from loom.core.job.runner import JobRunner
from loom.etl import (
    EmptyReplaceError,
    Format,
    HistorifyDateCollisionError,
    HistorifyDeleteMarkerError,
    HistorifyKeyConflictError,
    HistorifyTemporalConflictError,
    InvalidStageError,
    MaintenanceError,
    SchemaError,
    SchemaNotFoundError,
    UnsupportedFormatError,
    UnsupportedUpsertDeleteFlagError,
    UpsertDeleteFlagError,
)
from loom.etl.checkpoint import CheckpointCleanupError
from loom.etl.compiler import ETLCompilationError, ETLErrorCode
from loom.etl.ports import ConfigValueError
from loom.etl.ports._config_values import ConfigValueFailure

_POLICY = RetryPolicy(Backoff(first_s=1, factor=2, max_s=10, jitter=False), max_attempts=3)


async def _never_invoked(delivery: object) -> None:
    raise AssertionError("decide_unrunnable does not invoke the job")


_RUNNER = JobRunner(_never_invoked, redelivery=Redelivery.DELAYED)


class TestEtlErrorClassification:
    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            pytest.param(SchemaError("orders"), (False, None, True), id="SchemaError"),
            pytest.param(EmptyReplaceError("orders"), (True, None, True), id="EmptyReplaceError"),
            pytest.param(
                UpsertDeleteFlagError("deleted", "is missing"),
                (True, None, True),
                id="UpsertDeleteFlagError",
            ),
            pytest.param(
                InvalidStageError(frozenset({"LoadOrders"})),
                (True, None, True),
                id="InvalidStageError",
            ),
            pytest.param(
                HistorifyKeyConflictError("order_id"),
                (True, None, True),
                id="HistorifyKeyConflictError",
            ),
            pytest.param(
                HistorifyDateCollisionError("order_id"),
                (True, None, True),
                id="HistorifyDateCollisionError",
            ),
            pytest.param(
                HistorifyDeleteMarkerError("deleted", "is missing"),
                (True, None, True),
                id="HistorifyDeleteMarkerError",
            ),
            pytest.param(
                HistorifyTemporalConflictError(date(2024, 1, 5), date(2024, 1, 1)),
                (True, None, True),
                id="HistorifyTemporalConflictError",
            ),
            pytest.param(
                UnsupportedUpsertDeleteFlagError("FileWriter", "deleted"),
                (True, None, True),
                id="UnsupportedUpsertDeleteFlagError",
            ),
            pytest.param(
                UnsupportedFormatError(Format.XLSX, ()),
                (True, ErrorCode.UNSUPPORTED_FORMAT, False),
                id="UnsupportedFormatError",
            ),
            pytest.param(
                SchemaNotFoundError("orders"), (False, None, True), id="SchemaNotFoundError"
            ),
            pytest.param(MaintenanceError([]), (False, None, True), id="MaintenanceError"),
            pytest.param(
                CheckpointCleanupError("orders"), (False, None, True), id="CheckpointCleanupError"
            ),
            pytest.param(
                ETLCompilationError(ETLErrorCode.MISSING_TARGET, "LoadOrders", "no target"),
                (False, ETLErrorCode.MISSING_TARGET, True),
                id="ETLCompilationError",
            ),
            pytest.param(
                ConfigValueError("orders.limit", int, ConfigValueFailure.MISSING),
                (False, None, True),
                id="ConfigValueError",
            ),
        ],
    )
    def test_error_keeps_its_value_error_base_code_and_retryability(
        self, error: Exception, expected: tuple[bool, object, bool]
    ) -> None:
        outcome = _RUNNER.decide_unrunnable(error, 1, _POLICY)

        assert (
            isinstance(error, ValueError),
            getattr(error, "code", None),
            isinstance(outcome, Failed),
        ) == expected
