"""Contract tests for the optional-dependency extras declared by ``loom-kernel``.

Each extra must install what the subsystem it names imports, so the checks read
the installed distribution metadata (``Requires-Dist``) rather than the source
tree: that is what ``pip install "loom-kernel[<extra>]"`` resolves against.
"""

from __future__ import annotations

from importlib import metadata

import pytest
from packaging.requirements import Requirement


def _extra_requirements(extra: str, python: str | None = None) -> dict[str, Requirement]:
    """Return the requirements published under ``extra``, keyed by project name.

    Markers are evaluated for the running interpreter unless ``python`` names
    another version, so a version-bounded requirement is checked from the
    metadata on every Python instead of from what happens to be installed.
    """
    environment = {"extra": extra}
    if python is not None:
        environment |= {"python_version": python, "python_full_version": f"{python}.0"}
    declared = metadata.requires("loom-kernel") or []
    found: dict[str, Requirement] = {}
    for line in declared:
        requirement = Requirement(line)
        marker = requirement.marker
        if marker is None or not marker.evaluate(environment):
            continue
        found[requirement.name.lower()] = requirement
    return found


class TestPackagingExtras:
    def test_mongo_extra_installs_pymongo(self) -> None:
        requirements = _extra_requirements("mongo")

        assert "pymongo" in requirements
        assert requirements["pymongo"].specifier.contains("4.18.3")
        assert not requirements["pymongo"].specifier.contains("4.18.2")
        assert not requirements["pymongo"].specifier.contains("5.0")

    @pytest.mark.parametrize("extra", ["aws", "config-ssm", "prefect"])
    def test_the_extras_that_call_aws_install_boto3(self, extra: str) -> None:
        assert "boto3" in _extra_requirements(extra)

    @pytest.mark.parametrize("extra", ["dynamodb", "sqs"])
    def test_the_per_service_aws_extras_are_gone(self, extra: str) -> None:
        assert _extra_requirements(extra) == {}

    def test_sqlalchemy_extra_pins_greenlet_explicitly(self) -> None:
        requirements = _extra_requirements("sqlalchemy")

        assert "sqlalchemy" in requirements
        assert "greenlet" in requirements
        assert requirements["greenlet"].specifier.contains("3.0")

    def test_streaming_extra_skips_bytewax_on_python_313(self) -> None:
        assert "bytewax" in _extra_requirements("streaming", python="3.12")
        assert "bytewax" not in _extra_requirements("streaming", python="3.13")
        assert "bytewax" not in _extra_requirements("streaming", python="3.14")
        assert "confluent-kafka" in _extra_requirements("streaming", python="3.13")

    @pytest.mark.parametrize(
        ("extra", "packages"),
        [
            ("etl-spark", ("pyspark", "delta-spark")),
            ("pyspark", ("pyspark",)),
        ],
    )
    def test_spark_extras_skip_pyspark_on_python_313(
        self, extra: str, packages: tuple[str, ...]
    ) -> None:
        for package in packages:
            assert package in _extra_requirements(extra, python="3.12")
            assert package not in _extra_requirements(extra, python="3.13")
            assert package not in _extra_requirements(extra, python="3.14")

    @pytest.mark.parametrize("package", ["fastapi", "pydantic", "prometheus-client", "mangum"])
    def test_lambda_extra_installs_the_rest_stack_and_mangum(self, package: str) -> None:
        assert package in _extra_requirements("lambda")

    @pytest.mark.parametrize(("version", "accepted"), [("0.22", True), ("0.21", False)])
    def test_lambda_extra_needs_mangum_022_or_later(self, version: str, accepted: bool) -> None:
        assert _extra_requirements("lambda")["mangum"].specifier.contains(version) is accepted
