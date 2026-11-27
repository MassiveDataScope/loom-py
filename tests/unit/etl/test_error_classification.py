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


_CLASSIFIED = {
    "SchemaError": (SchemaError("orders"), (True, None, True)),
    "EmptyReplaceError": (EmptyReplaceError("orders"), (True, None, True)),
    "UpsertDeleteFlagError": (UpsertDeleteFlagError("deleted", "is missing"), (True, None, True)),
    "InvalidStageError": (InvalidStageError(frozenset({"LoadOrders"})), (True, None, True)),
    "HistorifyKeyConflictError": (HistorifyKeyConflictError("order_id"), (True, None, True)),
    "HistorifyDateCollisionError": (HistorifyDateCollisionError("order_id"), (True, None, True)),
    "HistorifyDeleteMarkerError": (
        HistorifyDeleteMarkerError("deleted", "is missing"),
        (True, None, True),
    ),
    "HistorifyTemporalConflictError": (
        HistorifyTemporalConflictError(date(2024, 1, 5), date(2024, 1, 1)),
        (True, ErrorCode.CONFLICT, False),
    ),
    "UnsupportedUpsertDeleteFlagError": (
        UnsupportedUpsertDeleteFlagError("FileWriter", "deleted"),
        (True, None, True),
    ),
    "UnsupportedFormatError": (
        UnsupportedFormatError(Format.XLSX, ()),
        (True, ErrorCode.UNSUPPORTED_FORMAT, False),
    ),
    "SchemaNotFoundError": (SchemaNotFoundError("orders"), (False, None, True)),
    "MaintenanceError": (MaintenanceError([]), (False, ErrorCode.SYSTEM_ERROR, True)),
    "CheckpointCleanupError": (
        CheckpointCleanupError("orders"),
        (False, ErrorCode.SYSTEM_ERROR, True),
    ),
    "ETLCompilationError": (
        ETLCompilationError(ETLErrorCode.MISSING_TARGET, "LoadOrders", "no target"),
        (False, ETLErrorCode.MISSING_TARGET, True),
    ),
    "ConfigValueError": (
        ConfigValueError("orders.limit", int, ConfigValueFailure.MISSING),
        (False, None, True),
    ),
}


class TestEtlErrorClassification:
    @pytest.mark.parametrize(("error", "expected"), _CLASSIFIED.values(), ids=list(_CLASSIFIED))
    def test_error_keeps_its_value_error_base_code_and_retryability(
        self, error: Exception, expected: tuple[bool, object, bool]
    ) -> None:
        outcome = _RUNNER.decide_unrunnable(error, 1, _POLICY)

        assert (
            isinstance(error, ValueError),
            getattr(error, "code", None),
            isinstance(outcome, Failed),
        ) == expected
