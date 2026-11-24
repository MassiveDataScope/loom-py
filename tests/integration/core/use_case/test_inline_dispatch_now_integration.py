"""A job a writer sends now, on the inline transport, runs detached from its sender.

The sender still holds its invocation scope open when the job's task first
runs; the job must open a scope of its own and log as itself.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.logger import LogField
from loom.core.logger.stamp import ContextFilter, LogIdentity
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import OrderedCommits
from loom.core.use_case.use_case import UseCase
from loom.stores.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.testing import RepositoryIntegrationHarness, make
from tests.helpers.jobs import JobKernel
from tests.helpers.loggers import logged_events
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.schemas import CreateProduct


class WriterJob(Job[None]):
    def __init__(self, harness: RepositoryIntegrationHarness) -> None:
        self._products = harness.product.repository

    async def execute(self) -> None:
        await self._products.create(make(CreateProduct, name="from the job"))


class SendsWriterJobNow(UseCase[Product, None]):
    def __init__(self, harness: RepositoryIntegrationHarness, jobs: JobService) -> None:
        self._products = harness.product.repository
        self._jobs = jobs

    async def execute(self) -> None:
        await self._products.create(make(CreateProduct, name="from the sender"))
        await self._jobs.dispatch_now(WriterJob)
        await asyncio.sleep(0)


async def test_a_job_sent_now_writes_in_its_own_scope_and_logs_as_itself(
    integration_context: RepositoryIntegrationHarness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    caplog.handler.addFilter(ContextFilter(LogIdentity(environment="test")))
    kernel = JobKernel()
    kernel.container.register_instance(RepositoryIntegrationHarness, integration_context)
    kernel.executor = UseCaseExecutor(
        UseCaseCompiler(),
        coordinator=OrderedCommits(
            {DEFAULT_STORE: SQLAlchemyUnitOfWorkFactory(integration_context.session_manager)}
        ),
    )
    jobs = kernel.serve_inline(WriterJob)
    kernel.factory.register(SendsWriterJobNow)

    await kernel.executor.execute(kernel.factory.build(SendsWriterJobNow))
    await jobs.wait_until_idle()

    products = integration_context.product.repository
    assert [
        await products.exists_by("name", name) for name in ("from the sender", "from the job")
    ] == [True, True]
    job_records = [entry for entry in logged_events(caplog) if LogField.JOB in entry]
    assert {entry[LogField.USE_CASE] for entry in job_records} == {WriterJob.__qualname__}
