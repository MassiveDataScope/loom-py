"""``AwsClients`` has one owner: the recipe, which the testing harness may set."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.celery.runtime import CeleryRuntime
from loom.composition import AppConfig, Project, Recipe, phases
from loom.composition.errors import CompositionErrorCode, ReservedBindingError
from loom.connections.aws import AwsClients
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.resources import ResourceKind
from loom.core.stores import SERVER_PROFILE
from loom.testing import HttpTestHarness, Override
from loom.testing.overrides import OverrideRuntime
from tests.helpers import acme_aws_installer
from tests.helpers.composition import composition_project

_QUEUE_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/default"


class _PingJob(Job[int]):
    __queue__ = "default"

    def execute(self) -> int:
        return 1


class AcmeOwnerNote(BaseModel):
    __tablename__ = "owner_notes"

    id: str = ColumnField(primary_key=True)


class RecordingAwsClients(AwsClients):
    """Hands out one placeholder client per service and records every request."""

    def __init__(self) -> None:
        super().__init__()
        self.requested: list[str] = []
        self.closed = 0

    def client(self, service: str, config: Any) -> Any:
        self.requested.append(service)
        return object()

    def close(self) -> None:
        self.closed += 1


def _worker_project(tmp_path: Path, packages: list[str] | None = None) -> Project:
    config = {
        "celery": {"broker_url": "memory://", "result_backend": "cache+memory://"},
        "sqs": {"queues": {"default": {"url": _QUEUE_URL}}},
        "stores": {"default": {"backend": "dynamodb", "region": "eu-west-1", "table": "t"}},
    }
    return composition_project(
        ConfigContext.from_dict(config),
        app=AppConfig(name="worker", job_service="sqs", packages=packages or []),
        code_path=tmp_path,
        config_root=tmp_path,
    )


class TestOneAwsClientsOwner:
    def test_the_supplied_clients_serve_the_transport_and_the_container(
        self, tmp_path: Path
    ) -> None:
        aws = RecordingAwsClients()
        runtime = OverrideRuntime(CeleryRuntime(jobs=[_PingJob]), (), aws=aws)

        application = _worker_project(tmp_path).compose(runtime)

        assert application.kernel.container.resolve(AwsClients) is aws
        assert "sqs" in aws.requested

    async def test_the_supplied_clients_are_the_ones_closed_with_the_application(
        self, tmp_path: Path
    ) -> None:
        aws = RecordingAwsClients()
        runtime = OverrideRuntime(CeleryRuntime(jobs=[_PingJob]), (), aws=aws)
        application = _worker_project(tmp_path).compose(runtime)

        async with application.running(check=False):
            pass

        assert aws.closed == 1
        assert [r.kind for r in application.resources if r.kind is ResourceKind.AWS]

    def test_the_http_harness_composes_over_the_supplied_clients(self) -> None:
        aws = RecordingAwsClients()
        harness = HttpTestHarness(aws=aws)

        harness.build_app(interfaces=[])

        assert harness.application is not None
        assert harness.application.kernel.container.resolve(AwsClients) is aws

    def test_the_stores_take_their_clients_from_the_recipe(self, tmp_path: Path) -> None:
        aws = RecordingAwsClients()
        recipe = Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(models=(AcmeOwnerNote,), use_cases=()),
            aws=aws,
        )

        phases.build_stores(_worker_project(tmp_path), recipe)

        assert aws.requested == ["dynamodb"]

    def test_without_supplied_clients_the_container_holds_the_composition_own(self) -> None:
        harness = HttpTestHarness()

        harness.build_app(interfaces=[])

        assert harness.application is not None
        assert type(harness.application.kernel.container.resolve(AwsClients)) is AwsClients


class TestAwsClientsIsReserved:
    def test_an_override_binding_it_is_refused_naming_the_harness_parameter(
        self, tmp_path: Path
    ) -> None:
        runtime = OverrideRuntime(
            CeleryRuntime(jobs=[_PingJob]), (Override.instance(AwsClients, RecordingAwsClients()),)
        )

        with pytest.raises(ReservedBindingError, match="aws parameter") as caught:
            _worker_project(tmp_path).compose(runtime)

        assert caught.value.issue.code is CompositionErrorCode.RESERVED_AWS_CLIENTS
        assert caught.value.key is AwsClients

    def test_an_installer_binding_it_is_refused(self, tmp_path: Path) -> None:
        project = _worker_project(tmp_path, packages=[acme_aws_installer.MODULE])

        with pytest.raises(ReservedBindingError, match="AcmeAwsInstaller") as caught:
            project.compose(CeleryRuntime(jobs=[_PingJob]))

        assert caught.value.issue.code is CompositionErrorCode.RESERVED_AWS_CLIENTS
