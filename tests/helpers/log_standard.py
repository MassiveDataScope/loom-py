"""The logging standard of ``src/loom``: the static call-site guard and its shrinking allow-list.

Every logging call in ``src/loom`` follows :mod:`loom.testing.log_calls`,
except in :data:`UNMIGRATED_PACKAGES`, a list of packages and modules that only
shrinks: ``tests/unit/test_log_standard.py`` fails when an entry already
complies, or when code that does not is missing from it.
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Final

import pytest

from loom.core.logger import LogIdentity
from loom.core.logger.filter import LogFilter
from loom.core.logger.formats import TextFormatter
from loom.core.logger.record import LoomRecordFactory
from loom.testing.log_calls import (
    LogCallOffence,
    exception_text_offences,
    free_text_offences,
    log_call_offences,
)
from tests.helpers.source_tree import SRC, source_files

UNMIGRATED_PACKAGES: Final[tuple[str, ...]] = (
    "loom.ai",
    "loom.celery",
    "loom.composition",
    "loom.core.async_bridge",
    "loom.core.authz",
    "loom.core.cache",
    "loom.core.checks",
    "loom.core.config",
    "loom.core.discovery",
    "loom.core.engine.compiler",
    "loom.core.engine.execution_observer",
    "loom.core.engine.post_commit",
    "loom.core.execution",
    "loom.core.identity",
    "loom.core.plugins",
    "loom.core.runner",
    "loom.core.stores",
    "loom.etl",
    "loom.jobs",
    "loom.prefect",
    "loom.rest",
    "loom.serverless.aws.lambda_._jobs",
    "loom.serverless.aws.lambda_._refusals",
    "loom.serverless.aws.lambda_.triggers",
    "loom.stores",
    "loom.streaming",
)
"""Packages and modules whose logging calls predate the standard; shrinks until empty."""

_VALIDATION_TEXT = "validation text about the data the client sent, answered to that client"

EXCEPTION_TEXT_EXITS: Final[dict[str, tuple[int, str]]] = {
    "loom.ai.a2a._rpc": (1, _VALIDATION_TEXT),
    "loom.ai.fastapi.endpoints": (2, _VALIDATION_TEXT),
    "loom.core.model._loom_type": (3, _VALIDATION_TEXT),
    "loom.core.model.keys": (2, _VALIDATION_TEXT),
    "loom.rest.fastapi.sql": (1, _VALIDATION_TEXT),
    "loom.streaming.mongo._bytewax_source": (
        1,
        "reads the message to classify it; formats nothing",
    ),
    "loom.stores.clickhouse.registry": (1, "reads the message to classify it; formats nothing"),
    "loom.stores.sqlalchemy.rls.bootstrap": (
        1,
        "reads the message to classify it; formats nothing",
    ),
    "loom.testing.agents": (1, "a test helper's assertion, read by the developer running it"),
}
"""Modules that format an exception's text where it may: how many places, and why. Permanent."""

UNMIGRATED_EXCEPTION_TEXT: Final[dict[str, int]] = {
    "loom.ai.compiler.phases._capabilities": 2,
    "loom.ai.compiler.phases._output": 2,
    "loom.ai.compiler.phases._state": 2,
    "loom.ai.declarative.__init__": 1,
    "loom.ai.declarative._envelope": 4,
    "loom.ai.engines.pydantic_ai._errors": 1,
    "loom.ai.engines.pydantic_ai._instructions": 1,
    "loom.ai.engines.pydantic_ai._output": 1,
    "loom.ai.etl._validation": 1,
    "loom.ai.execution._grants": 1,
    "loom.ai.execution._lifecycle": 2,
    "loom.ai.execution._turn": 1,
    "loom.ai.remote_auth": 2,
    "loom.cli.check": 1,
    "loom.cli.deploy": 1,
    "loom.cli.schema": 2,
    "loom.composition._jobs": 1,
    "loom.composition.application": 1,
    "loom.composition.phases": 1,
    "loom.composition.recipe": 1,
    "loom.core.bootstrap.kernel": 1,
    "loom.core.cache.calls": 5,
    "loom.core.checks": 2,
    "loom.core.config._includes": 1,
    "loom.core.config.binder": 1,
    "loom.core.config.convert": 1,
    "loom.core.config.loader": 5,
    "loom.core.di.compiler": 1,
    "loom.core.di.container": 2,
    "loom.core.di.installer": 4,
    "loom.core.discovery.scanner": 1,
    "loom.core.engine.declared_observers": 2,
    "loom.core.engine.post_commit": 1,
    "loom.core.errors.jobs": 1,
    "loom.core.job.config": 1,
    "loom.core.job.envelope": 1,
    "loom.core.model._loom_type": 2,
    "loom.core.model.introspection": 1,
    "loom.core.stores.uow.coordinator": 1,
    "loom.etl.compiler._validators_config": 1,
    "loom.etl.io.sources._dynamodb": 1,
    "loom.etl.runner.config_loader": 1,
    "loom.etl.schema._contract": 1,
    "loom.etl.storage._config": 2,
    "loom.etl.testing._runners": 1,
    "loom.jobs.aws.queue": 1,
    "loom.prefect._flow_yaml": 1,
    "loom.prefect.deploy._discovery": 1,
    "loom.prefect.deploy._yaml_etls": 2,
    "loom.prefect.flow._backfill_body": 1,
    "loom.prefect.notify._slack": 1,
    "loom.rest.compiler": 2,
    "loom.rest.config": 1,
    "loom.rest.fastapi.response": 1,
    "loom.serverless.aws.lambda_._jobs": 1,
    "loom.serverless.aws.lambda_.manifest": 2,
    "loom.stores.clickhouse.executor": 1,
    "loom.stores.clickhouse.registry": 2,
    "loom.stores.mongo.query_compiler": 1,
    "loom.stores.sqlalchemy.alembic": 2,
    "loom.stores.sqlalchemy.migrations.runners": 2,
    "loom.stores.sqlalchemy.rls.bootstrap": 2,
    "loom.stores.sqlalchemy.rls.partitions": 1,
    "loom.stores.sqlalchemy.rls.schema": 1,
    "loom.stores.sqlalchemy.schema": 2,
    "loom.streaming.bytewax._runtime_io": 1,
    "loom.streaming.kafka.client._consumer": 5,
    "loom.streaming.kafka.client._producer": 2,
    "loom.streaming.kafka.message._consumer": 1,
}
"""Modules that put an exception's text into a new error's message: how many places.

Rule 2 of spec 019: the message of an exception is never a payload. These
messages carry foreign text into a loom error; L2c rewrites them. Shrinks
until empty."""

UNMIGRATED_CONSTRUCTIONS: Final[tuple[str, ...]] = (
    "loom.ai",
    "loom.celery",
    "loom.cli",
    "loom.composition",
    "loom.connections",
    "loom.core.authz",
    "loom.core.bootstrap",
    "loom.core.cache",
    "loom.core.checks",
    "loom.core.config",
    "loom.core.connections",
    "loom.core.di",
    "loom.core.discovery",
    "loom.core.engine",
    "loom.core.environment",
    "loom.core.errors",
    "loom.core.execution",
    "loom.core.expr",
    "loom.core.identity",
    "loom.core.introspection",
    "loom.core.job",
    "loom.core.logger",
    "loom.core.model",
    "loom.core.observability",
    "loom.core.projection",
    "loom.core.routing",
    "loom.core.schema_names",
    "loom.core.stores",
    "loom.core.symbols",
    "loom.core.use_case",
    "loom.etl",
    "loom.jobs",
    "loom.prefect",
    "loom.prometheus",
    "loom.rest",
    "loom.serverless",
    "loom.stores",
    "loom.streaming",
    "loom.testing",
)
"""Packages that still raise errors with text built from values, not declared fields.

Rule 2 of spec 019: a ``LoomError`` is built as ``Cls(**fields)``. L2c migrates
each package's constructions and its own ``raise ValueError/TypeError``; the
list shrinks until empty, and 12.0 ships when it is."""


def module_of(offence: LogCallOffence) -> str:
    """The dotted module an offence was found in."""
    return ".".join(Path(offence.path).relative_to(SRC.parent).with_suffix("").parts)


def is_unmigrated(module: str, entries: tuple[str, ...] = UNMIGRATED_PACKAGES) -> bool:
    """Whether *module* is, or is inside, one of *entries*."""
    return any(module == entry or module.startswith(f"{entry}.") for entry in entries)


def loom_offences() -> list[LogCallOffence]:
    """Every logging call of ``src/loom`` that breaks the standard, unmigrated code included."""
    return log_call_offences(source_files())


@pytest.fixture(autouse=True)
def caplog_shows_fields(request: pytest.FixtureRequest) -> None:
    """Make ``caplog`` see each record as a loom handler does, and show it as the text format.

    Records are made by loom's record factory, as ``configure_logging`` makes
    them in production, and its handler gets the
    :class:`~loom.core.logger.filter.LogFilter` loom puts on every root handler,
    so a record's stamp and failure fields are its attributes.
    """
    if "caplog" in request.fixturenames:
        LoomRecordFactory.install(LogIdentity(environment="test"))
        handler = request.getfixturevalue("caplog").handler
        handler.setFormatter(TextFormatter())
        handler.addFilter(LogFilter())


def exception_text_sites() -> dict[str, int]:
    """How many places each module of ``src/loom`` formats an exception's text."""
    sites: dict[str, int] = {}
    for offence in exception_text_offences(source_files()):
        module = module_of(offence)
        sites[module] = sites.get(module, 0) + 1
    return sites


@functools.cache
def free_text_modules() -> frozenset[str]:
    """The modules of ``src/loom`` that raise an error with text built from values, read once."""
    return frozenset(module_of(offence) for offence in free_text_offences(source_files()))
