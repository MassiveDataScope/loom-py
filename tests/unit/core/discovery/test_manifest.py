from __future__ import annotations

import sys
import types

import pytest

from loom.core.discovery.manifest import ManifestDiscoveryEngine
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy

_MANIFEST = "tests.unit.core.discovery._manifest"


class _ManifestProduct(BaseModel):
    __tablename__ = "manifest_products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)


class _ManifestProductRepository(RepositorySQLAlchemy[_ManifestProduct, int]):
    pass


class _ManifestJob(Job[None]):
    def execute(self) -> None:
        return None


@pytest.fixture
def manifest(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """An empty manifest module, importable by name for the test only."""
    module = types.ModuleType(_MANIFEST)
    monkeypatch.setitem(sys.modules, _MANIFEST, module)
    return module


def test_manifest_discovery_accepts_repositories_list(manifest: types.ModuleType) -> None:
    manifest.REPOSITORIES = [_ManifestProductRepository]  # type: ignore[attr-defined]

    result = ManifestDiscoveryEngine(_MANIFEST).discover()

    assert result.models == ()
    assert result.use_cases == ()
    assert result.interfaces == ()


def test_manifest_discovery_rejects_invalid_repository_type(manifest: types.ModuleType) -> None:
    manifest.REPOSITORIES = [object]  # type: ignore[attr-defined]
    engine = ManifestDiscoveryEngine(_MANIFEST)

    with pytest.raises(TypeError, match="must implement the Repository protocol"):
        engine.discover()


def test_manifest_discovery_accepts_agents_only_manifest(manifest: types.ModuleType) -> None:
    """A manifest that exposes only agent artifacts is a complete manifest."""
    manifest.AGENTS = ["ai/agents/*/agent.yaml"]  # type: ignore[attr-defined]

    result = ManifestDiscoveryEngine(_MANIFEST).discover()

    assert result.agent_specs == ("ai/agents/*/agent.yaml",)


def test_manifest_discovery_carries_the_jobs(manifest: types.ModuleType) -> None:
    """A manifest that exposes only jobs is complete; the composition applies their config."""
    manifest.JOBS = [_ManifestJob]  # type: ignore[attr-defined]

    result = ManifestDiscoveryEngine(_MANIFEST).discover()

    assert result.jobs == (_ManifestJob,)


@pytest.mark.parametrize("attribute", ["AGENTS", "JOBS"])
def test_manifest_discovery_rejects_empty_manifest_naming_every_attribute(
    manifest: types.ModuleType, attribute: str
) -> None:
    """The 'no components' message lists AGENTS and JOBS among the expected attributes."""
    _ = manifest
    engine = ManifestDiscoveryEngine(_MANIFEST)

    with pytest.raises(ValueError, match=attribute):
        engine.discover()


@pytest.mark.parametrize(
    "spec",
    [object, ""],
    ids=["a type, never a path or glob", "an empty string, which is never a grant"],
)
def test_manifest_discovery_rejects_an_invalid_agent_spec(
    manifest: types.ModuleType, spec: object
) -> None:
    manifest.AGENTS = [spec]  # type: ignore[attr-defined]
    engine = ManifestDiscoveryEngine(_MANIFEST)

    with pytest.raises(TypeError):
        engine.discover()
