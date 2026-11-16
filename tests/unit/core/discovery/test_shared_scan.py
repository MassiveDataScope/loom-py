from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.celery.adapter import CeleryAdapter
from loom.composition import Application
from loom.composition.project import Project
from loom.core.config import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.scanner import PackageScanner
from loom.core.discovery.worker import resolve_worker_components
from loom.core.job.job import Job
from loom.core.stores.repository import RepositoryCatalog
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.serverless.aws.lambda_ import LambdaAdapter
from loom.serverless.aws.lambda_.trigger_jobs import check_triggers
from tests.unit.core.discovery.conftest import PackageWriter

_MODEL = (
    "from loom.core.model import BaseModel, ColumnField\n"
    "class AcmeProduct(BaseModel):\n"
    "    __tablename__ = 'acme_products'\n"
    "    id: int = ColumnField(primary_key=True, autoincrement=True)\n"
)
_JOB = (
    "from loom.core.command import Command\n"
    "from loom.core.job.job import Job\n"
    "from loom.core.use_case.markers import Input\n"
    "class AcmeNotice(Command, frozen=True):\n"
    "    text: str\n"
    "class AcmeJob(Job[None]):\n"
    "    async def execute(self, notice: AcmeNotice = Input()) -> None: ...\n"
)
_USE_CASE = (
    "from loom.core.use_case.use_case import UseCase\n"
    "class AcmePing(UseCase[object, str]):\n"
    "    async def execute(self) -> str:\n"
    "        return 'pong'\n"
)
_STORES = "stores:\n  default:\n    backend: sqlalchemy\n    url: sqlite+aiosqlite:///\n"
_CELERY = "celery:\n  broker_url: memory://\n  result_backend: cache+memory://\n"
_LAMBDA = (
    "lambda:\n"
    "  handler: acme.handler\n"
    "  functions:\n"
    "    worker:\n"
    "      trigger:\n"
    "        source: jobs\n"
    "    notify:\n"
    "      trigger:\n"
    "        source: eventbridge\n"
    "        job: AcmeJob\n"
    "        identity:\n"
    "          subject: acme-system\n"
)


class _AcmeExplicitJob(Job[None]):
    async def execute(self) -> None: ...


@pytest.fixture
def scans(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """One entry per call of ``PackageScanner.discover``."""
    calls: list[int] = []
    scan = PackageScanner.discover

    def counted(self: PackageScanner, **kwargs: Any) -> DiscoveryResult:
        calls.append(1)
        return scan(self, **kwargs)

    monkeypatch.setattr(PackageScanner, "discover", counted)
    return calls


def _config(tmp_path: Path, packages: str, sections: str = "") -> str:
    config = tmp_path / "app.yaml"
    config.write_text(f"app:\n  name: acme\n  packages: {packages}\n{sections}")
    return str(config)


class TestWorkerDiscoveryErrors:
    @pytest.mark.parametrize(
        "adapter",
        [
            pytest.param(lambda: CeleryAdapter(), id="celery"),
            pytest.param(lambda: LambdaAdapter(function="worker"), id="lambda-jobs"),
        ],
    )
    @pytest.mark.parametrize(
        ("files", "cause"),
        [
            pytest.param({"broken.py": "raise RuntimeError('boom')\n"}, RuntimeError, id="raises"),
            pytest.param({"broken.py": "import acme_absent\n"}, ImportError, id="missing-import"),
        ],
    )
    def test_a_package_that_cannot_be_scanned_fails_the_start_with_the_cause_chained(
        self,
        tmp_path: Path,
        write_package: PackageWriter,
        adapter: Callable[[], Any],
        files: dict[str, str],
        cause: type[Exception],
    ) -> None:
        write_package("acme_failing", files)
        config = _config(tmp_path, "[acme_failing]", _CELERY + _LAMBDA)
        project = Project.load(config, environment="test")

        with pytest.raises(ConfigError, match="acme_failing.broken") as error:
            project.compose(adapter())

        assert isinstance(error.value.__cause__, cause)

    def test_an_unknown_package_fails_the_start_naming_it(self, tmp_path: Path) -> None:
        config = _config(tmp_path, "[acme_unknown]", _CELERY)

        with pytest.raises(ConfigError, match="acme_unknown") as error:
            Project.load(config, environment="test").compose(CeleryAdapter())

        assert isinstance(error.value.__cause__, ImportError)

    def test_no_packages_yield_only_the_explicit_jobs(self, tmp_path: Path) -> None:
        project = Project.load(_config(tmp_path, "[]", _CELERY), environment="test")

        resolved = resolve_worker_components(project.discovered, [_AcmeExplicitJob], [])

        assert resolved.jobs == (_AcmeExplicitJob,)
        assert resolved.models == ()


class TestOneScanAcrossEntryPoints:
    def test_every_entry_point_sees_the_same_models_from_one_scan(
        self, tmp_path: Path, write_package: PackageWriter, scans: list[int]
    ) -> None:
        write_package("acme_shared", {"models.py": _MODEL, "jobs.py": _JOB, "cases.py": _USE_CASE})
        config = _config(tmp_path, "[acme_shared]", _STORES + _CELERY + _LAMBDA)
        project = Project.load(config, environment="test")
        adapters: dict[str, Callable[[], Any]] = {
            "rest": lambda: FastAPIAdapter(),
            "celery": lambda: CeleryAdapter(),
            "lambda": lambda: LambdaAdapter(function="worker"),
        }

        applications: dict[str, Application] = {
            name: project.compose(adapter()) for name, adapter in adapters.items()
        }
        check_triggers(project)

        (expected,) = {app.discovered.models for app in applications.values()}
        assert [model.__name__ for model in expected] == ["AcmeProduct"]
        for application in applications.values():
            catalog = application.kernel.container.resolve(RepositoryCatalog)
            assert catalog.for_table("acme_products") is not None
        assert len(scans) == 1

    def test_loom_check_reads_the_jobs_from_one_scan(
        self, tmp_path: Path, write_package: PackageWriter, scans: list[int]
    ) -> None:
        write_package("acme_shared", {"models.py": _MODEL, "jobs.py": _JOB})
        config = _config(tmp_path, "[acme_shared]", _LAMBDA)
        checked = Project.check(config, environment="test", skip=("loom.rest",))

        check_triggers(checked)
        check_triggers(checked)

        assert [job.__name__ for job in checked.discovered.jobs] == ["AcmeJob"]
        assert len(scans) == 1

    def test_loom_check_and_the_project_discover_the_same_classes(
        self, tmp_path: Path, write_package: PackageWriter
    ) -> None:
        write_package("acme_shared", {"models.py": _MODEL, "jobs.py": _JOB})
        config = _config(tmp_path, "[acme_shared]", _LAMBDA)

        checked = Project.check(config, environment="test", skip=("loom.rest",))
        project = Project.load(config, environment="test")

        assert checked.discovered.models == project.discovered.models
        assert checked.discovered.jobs == project.discovered.jobs

    def test_an_event_trigger_naming_an_unscanned_job_is_refused(
        self, tmp_path: Path, write_package: PackageWriter
    ) -> None:
        write_package("acme_shared", {"models.py": _MODEL})
        config = _config(tmp_path, "[acme_shared]", _LAMBDA)
        checked = Project.check(config, environment="test", skip=("loom.rest",))

        with pytest.raises(ConfigError, match="AcmeJob"):
            check_triggers(checked)
