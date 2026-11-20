"""A ``ConfiguredInstaller`` reads its own section alike at start-up and in ``loom check``."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from loom.celery.adapter import CeleryAdapter
from loom.composition.project import Project
from loom.core.config import ConfigError
from tests.helpers.acme_stores_settings import AcmeBucket, AcmeStoresSettings
from tests.helpers.package_writer import PackageWriter

_PLAIN = """\
from loom.core.di.installer import Installer
from tests.helpers.acme_stores_settings import AcmeBucket


class AcmePlainInstaller(Installer):
    def install(self, container):
        container.register_instance(AcmeBucket, AcmeBucket("plain", "memory"))
"""
_CONFIGURED = """\
from loom.core.di.installer import ConfiguredInstaller
from tests.helpers.acme_stores_settings import AcmeBucket, AcmeStoresSettings


class AcmeStoresInstaller(ConfiguredInstaller[AcmeStoresSettings]):
    section = "acme.stores"
    settings_type = AcmeStoresSettings

    def install(self, container):
        container.register_instance(
            AcmeBucket, AcmeBucket(self.settings.bucket, self.settings.backend)
        )
"""
_APP = "app:\n  name: acme\n  job_service: inline\n  packages: [acme_installers_pkg]\n"
_RUNTIME = (
    "stores:\n  default:\n    backend: sqlalchemy\n    url: sqlite+aiosqlite:///\n"
    "celery:\n  broker_url: memory://\n  result_backend: cache+memory://\n"
)
_STORES_SECTION = "acme:\n  stores:\n    bucket: acme-data\n    backend: gcs\n"
_BROKEN_SECTIONS = {
    "missing-section": "",
    "mistyped-value": "acme:\n  stores:\n    bucket: [acme-data]\n",
    "missing-field": "acme:\n  stores:\n    backend: gcs\n",
}


@pytest.fixture(autouse=True)
def decoded() -> Iterator[list[str]]:
    AcmeStoresSettings.decoded.clear()
    yield AcmeStoresSettings.decoded
    AcmeStoresSettings.decoded.clear()


@pytest.fixture
def project_config(write_package: PackageWriter, tmp_path: Path) -> Callable[[str, str], str]:
    """Write a package holding *source* and a config with *sections*; return the config path."""

    def write(source: str, sections: str) -> str:
        write_package("acme_installers_pkg", {"installers.py": source})
        config = tmp_path / "app.yaml"
        config.write_text(f"{_APP}{_RUNTIME}{sections}")
        return str(config)

    return write


def _bucket(config: str) -> AcmeBucket:
    application = Project.load(config, environment="test").compose(CeleryAdapter())
    return application.kernel.container.resolve(AcmeBucket)


class TestInstallerFromConfig:
    def test_a_plain_installer_is_built_without_configuration(
        self, project_config: Callable[[str, str], str]
    ) -> None:
        config = project_config(_PLAIN, "")

        assert _bucket(config) == AcmeBucket("plain", "memory")

    def test_a_configured_installer_receives_its_decoded_section(
        self, project_config: Callable[[str, str], str]
    ) -> None:
        config = project_config(_CONFIGURED, _STORES_SECTION)

        assert _bucket(config) == AcmeBucket("acme-data", "gcs")

    def test_the_section_is_decoded_once_per_composition(
        self, project_config: Callable[[str, str], str], decoded: list[str]
    ) -> None:
        config = project_config(_CONFIGURED, _STORES_SECTION)

        _bucket(config)

        assert decoded == ["acme-data"]


class TestInstallerSectionValidation:
    @pytest.mark.parametrize("sections", _BROKEN_SECTIONS.values(), ids=_BROKEN_SECTIONS)
    def test_startup_and_check_refuse_a_broken_section_with_the_same_error(
        self, project_config: Callable[[str, str], str], sections: str
    ) -> None:
        config = project_config(_CONFIGURED, sections)

        with pytest.raises(ConfigError) as at_startup:
            _bucket(config)
        with pytest.raises(ConfigError) as in_check:
            Project.check(config, environment="test", excluded=("app.rest",))

        assert str(in_check.value) == str(at_startup.value)
        assert "AcmeStoresInstaller" in str(in_check.value)
        assert "'acme.stores'" in str(in_check.value)

    def test_check_accepts_a_valid_section_and_decodes_it_once(
        self, project_config: Callable[[str, str], str], decoded: list[str]
    ) -> None:
        config = project_config(_CONFIGURED, _STORES_SECTION)

        Project.check(config, environment="test", excluded=("app.rest",))

        assert decoded == ["acme-data"]

    def test_check_accepts_a_plain_installer_with_no_section(
        self, project_config: Callable[[str, str], str]
    ) -> None:
        config = project_config(_PLAIN, "")

        checked = Project.check(config, environment="test", excluded=("app.rest",))

        assert not checked.report.errors
