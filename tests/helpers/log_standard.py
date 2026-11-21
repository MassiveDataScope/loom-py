"""The logging standard of ``src/loom``: the static call-site guard and its shrinking allow-list.

Every logging call in ``src/loom`` follows :mod:`loom.testing.log_calls`,
except in :data:`UNMIGRATED_PACKAGES`, a list of packages and modules that only
shrinks: ``tests/unit/test_log_standard.py`` fails when an entry already
complies, or when code that does not is missing from it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pytest

from loom.core.logger import LogIdentity
from loom.core.logger.filter import LogFilter
from loom.core.logger.formats import TextFormatter
from loom.core.logger.install import install_record_factory
from loom.testing.log_calls import LogCallOffence, log_call_offences
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


def module_of(offence: LogCallOffence) -> str:
    """The dotted module an offence was found in."""
    return ".".join(Path(offence.path).relative_to(SRC.parent).with_suffix("").parts)


def is_unmigrated(module: str) -> bool:
    """Whether *module* is, or is inside, an entry of :data:`UNMIGRATED_PACKAGES`."""
    return any(module == entry or module.startswith(f"{entry}.") for entry in UNMIGRATED_PACKAGES)


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
        install_record_factory(LogIdentity(environment="test"))
        handler = request.getfixturevalue("caplog").handler
        handler.setFormatter(TextFormatter())
        handler.addFilter(LogFilter())
