"""The logging standard, enforced across the whole suite.

Every record a ``loom.*`` logger emits during a test may carry only
:class:`~loom.core.logger.LogField` keys in its ``extra``. Modules not yet
migrated are tolerated through :data:`UNMIGRATED_MODULES`, an explicit list that
only shrinks: ``tests/unit/test_log_standard.py`` fails when a module on it
already complies, or when one that does not is missing from it.
"""

from __future__ import annotations

import ast
import logging
from collections.abc import Iterator, Mapping
from typing import Final

import pytest

from loom.core.logger import LogEvent, LogField
from loom.core.logger.formats import TextFormatter, record_fields
from tests.helpers.source_tree import SRC, parsed, source_files

_LOGGING_METHODS: Final = frozenset(
    {"debug", "info", "warning", "error", "exception", "critical", "log"}
)
ALLOWED_KEYS: Final = frozenset(field.value for field in LogField)

UNMIGRATED_MODULES: Final[tuple[str, ...]] = (
    "loom.celery._discovery",
    "loom.celery.runtime",
    "loom.celery.service",
    "loom.composition._jobs",
    "loom.composition.project",
    "loom.core.async_bridge",
    "loom.core.cache.calls",
    "loom.core.cache.repository",
    "loom.core.cache.wiring",
    "loom.core.engine.compiler",
    "loom.core.engine.observation",
    "loom.core.engine.post_commit",
    "loom.core.execution.context",
    "loom.core.identity.context",
    "loom.core.job.runner",
    "loom.core.observability.observability",
    "loom.core.observability.observer.log",
    "loom.core.observability.observer.otel",
    "loom.core.observability.otel_ids",
    "loom.core.stores.uow.coordinator",
    "loom.etl.engines._write_policy",
    "loom.etl.engines.polars._writer",
    "loom.etl.io.sources._clickhouse",
    "loom.etl.io.sources._dynamodb",
    "loom.etl.io.sources._mongo",
    "loom.etl.io.targets._clickhouse",
    "loom.etl.maintenance._runner",
    "loom.etl.maintenance.backends._delta_rs",
    "loom.jobs.aws.settlement",
    "loom.jobs.aws.sqs",
    "loom.prefect.flow._hooks",
    "loom.prefect.notify._slack",
    "loom.prefect.notify._sns",
    "loom.serverless.aws.lambda_._jobs",
    "loom.serverless.aws.lambda_._refusals",
    "loom.serverless.aws.lambda_.handler",
    "loom.serverless.aws.lambda_.triggers.sqs",
    "loom.stores.mongo.repository",
    "loom.stores.sqlalchemy.alembic",
    "loom.stores.sqlalchemy.repository",
    "loom.stores.sqlalchemy.session_manager",
    "loom.stores.sqlalchemy.transactional",
    "loom.streaming.bytewax._adapter",
    "loom.streaming.bytewax._error_boundary",
    "loom.streaming.bytewax.handlers.storage",
    "loom.streaming.kafka._wire",
    "loom.streaming.nodes._table._clickhouse",
    "loom.streaming.nodes._table._delta",
    "loom.streaming.nodes._table._sqlalchemy",
)
"""Modules whose ``extra`` still carries keys outside ``LogField``; shrinks until empty."""


class _Seen(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def foreign_keys(record: logging.LogRecord) -> set[str]:
    """The ``extra`` keys of *record* that are not a ``LogField`` member."""
    return set(record_fields(record)) - ALLOWED_KEYS


@pytest.fixture(autouse=True)
def loom_records_use_log_fields() -> Iterator[None]:
    """Fail a test when a ``loom.*`` record carries a key outside ``LogField``."""
    seen = _Seen()
    loom_logger = logging.getLogger("loom")
    loom_logger.addHandler(seen)
    yield
    loom_logger.removeHandler(seen)
    offences = sorted(
        {
            f"{record.name}: {sorted(foreign_keys(record))}"
            for record in seen.records
            if foreign_keys(record) and record.name not in UNMIGRATED_MODULES
        }
    )
    if offences:
        pytest.fail("records outside the LogField standard:\n" + "\n".join(offences))


@pytest.fixture(autouse=True)
def caplog_shows_fields(request: pytest.FixtureRequest) -> None:
    """Make ``caplog.text`` show each record with its fields, as the text format writes it."""
    if "caplog" in request.fixturenames:
        request.getfixturevalue("caplog").handler.setFormatter(TextFormatter())


def _is_log_field(node: ast.expr | None) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "LogField"
    )


def _extra_is_standard(value: ast.expr) -> bool:
    return isinstance(value, ast.Dict) and all(_is_log_field(key) for key in value.keys)


def _is_logging_call(call: ast.Call) -> bool:
    return isinstance(call.func, ast.Attribute) and call.func.attr in _LOGGING_METHODS


def modules_with_foreign_extra() -> tuple[str, ...]:
    """The modules of ``src/loom`` that pass an ``extra`` the standard does not allow."""
    found: set[str] = set()
    for path in source_files():
        for node in ast.walk(parsed(path)):
            if not (isinstance(node, ast.Call) and _is_logging_call(node)):
                continue
            extras = [keyword.value for keyword in node.keywords if keyword.arg == "extra"]
            if any(not _extra_is_standard(extra) for extra in extras):
                found.add(".".join(path.relative_to(SRC.parent).with_suffix("").parts))
    return tuple(sorted(found))


def assert_logged(
    caplog: pytest.LogCaptureFixture,
    event: LogEvent,
    *,
    level: int,
    fields: Mapping[LogField, object] | None = None,
) -> logging.LogRecord:
    """Assert that exactly one record of *caplog* is *event* at *level* with the *fields*.

    The record may carry more fields than the ones given (the filter stamps
    several); each one given must be on it with that value.

    Args:
        caplog: The fixture that captured the records.
        event: The ``LogEvent`` member the call must have written.
        level: The numeric level, such as ``logging.WARNING``.
        fields: The ``LogField`` keys and values the record must carry.

    Returns:
        The record, for a further assertion on its exception.
    """
    emitted = [record for record in caplog.records if record.getMessage() == event.value]
    if len(emitted) != 1:
        pytest.fail(f"expected one {event.value!r} record, got {len(emitted)}")
    [record] = emitted
    if record.levelno != level:
        pytest.fail(f"{event.value!r} was logged at {record.levelname}")
    carried = record_fields(record)
    wanted = {key.value: value for key, value in (fields or {}).items()}
    wrong = {key: carried.get(key) for key in wanted if carried.get(key) != wanted[key]}
    if wrong:
        pytest.fail(f"{event.value!r} carries {wrong}, expected {wanted}")
    return record
