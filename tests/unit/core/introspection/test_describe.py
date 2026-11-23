"""Data-driven self-description core (US7).

``loom.core.introspection`` is the neutral half of the feature: it knows how
to assemble one JSON-encodable document out of an application identity plus a
set of *references* to pillar contributors.  It resolves those references by
``importlib`` at call time, which is exactly what keeps ``loom.core`` free of
any pillar import (principle I, FR-050, SC-013) — the last test of this module
guards that containment in a clean interpreter.
"""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.core.introspection import (
    AppIntrospection,
    ContributorRef,
    IntrospectionError,
    describe_app,
)
from tests.helpers.imports import import_delta

_SRC = Path(__file__).resolve().parents[4] / "src"

_MODULE_NAME = "tests.unit.core.introspection._describe_contributors"

_APP_NAME = "introspection-demo"
_APP_VERSION = "3.2.1"


def _echo(subject: Any, /) -> dict[str, Any]:
    """Contributor projecting its subject into a builtin mapping."""
    return {"seen": subject}


@pytest.fixture
def contributor_module() -> Iterator[str]:
    """Register an importable module exposing one callable and one plain value."""
    module = types.ModuleType(_MODULE_NAME)
    module.echo = _echo
    module.not_callable = "plain value"
    sys.modules[_MODULE_NAME] = module
    try:
        yield _MODULE_NAME
    finally:
        sys.modules.pop(_MODULE_NAME, None)


def _introspection(*contributors: ContributorRef) -> AppIntrospection:
    """Build the application introspection under test."""
    return AppIntrospection(
        name=_APP_NAME,
        version=_APP_VERSION,
        contributors=contributors,
    )


class TestDescriptionWithoutContributors:
    """An application that contributes nothing still describes itself."""

    def test_publishes_only_the_identity_when_there_are_no_contributors(self) -> None:
        """The identity section is the whole document when no pillar contributes."""
        assert describe_app(_introspection()) == {
            "app": {"name": _APP_NAME, "version": _APP_VERSION}
        }


class TestContributorResolution:
    """A contributor is a string until ``describe_app`` is actually called."""

    def test_places_the_contribution_under_its_section_when_the_ref_resolves(
        self, contributor_module: str
    ) -> None:
        """The resolved callable's return value lands under its declared section."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:echo",
            subject=("alpha", "beta"),
        )

        assert describe_app(_introspection(ref))["agents"] == {"seen": ("alpha", "beta")}

    def test_keeps_the_identity_when_there_is_a_contribution(self, contributor_module: str) -> None:
        """A contribution never displaces the reserved ``app`` section."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:echo",
            subject=(),
        )

        assert describe_app(_introspection(ref))["app"] == {
            "name": _APP_NAME,
            "version": _APP_VERSION,
        }


class TestInvalidContributors:
    """Every unusable reference is reported as an ``IntrospectionError``."""

    def test_fails_when_the_ref_lacks_the_module_colon_callable_shape(self) -> None:
        """A reference without ``:`` names no callable at all."""
        ref = ContributorRef(section="agents", contributor="loom.core.introspection", subject=())

        app = _introspection(ref)

        with pytest.raises(IntrospectionError):
            describe_app(app)

    def test_fails_when_the_module_does_not_exist(self) -> None:
        """An unimportable module is a wiring error, reported as such."""
        ref = ContributorRef(
            section="agents",
            contributor="loom.core.introspection_absent_module:describe",
            subject=(),
        )

        app = _introspection(ref)

        with pytest.raises(IntrospectionError):
            describe_app(app)

    def test_fails_when_the_attribute_does_not_exist(self, contributor_module: str) -> None:
        """A module that does not expose the named attribute is a wiring error."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:absent",
            subject=(),
        )

        app = _introspection(ref)

        with pytest.raises(IntrospectionError):
            describe_app(app)

    def test_fails_when_the_attribute_is_not_callable(self, contributor_module: str) -> None:
        """A contributor must be callable; a plain value cannot project anything."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:not_callable",
            subject=(),
        )

        app = _introspection(ref)

        with pytest.raises(IntrospectionError):
            describe_app(app)

    def test_fails_when_two_contributors_declare_the_same_section(
        self, contributor_module: str
    ) -> None:
        """Silently overwriting one pillar's contribution with another is not an option."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:echo",
            subject=(),
        )

        app = _introspection(ref, ref)

        with pytest.raises(IntrospectionError):
            describe_app(app)

    def test_fails_when_the_section_is_app(self, contributor_module: str) -> None:
        """``app`` is reserved for the application identity."""
        ref = ContributorRef(
            section="app",
            contributor=f"{contributor_module}:echo",
            subject=(),
        )

        app = _introspection(ref)

        with pytest.raises(IntrospectionError):
            describe_app(app)


_FORBIDDEN_ROOTS: tuple[str, ...] = (
    "loom.ai",
    "loom.rest",
    "loom.streaming",
    "loom.etl",
)

_FORBIDDEN_THIRD_PARTY: tuple[str, ...] = ("fastapi", "pydantic")


class TestImportContainment:
    """Importing the core package must not drag a single pillar in (principle I)."""

    def test_importing_introspection_pulls_in_no_pillar(self) -> None:
        """No ``loom.ai`` / ``loom.rest`` / ``loom.streaming`` / ``loom.etl`` module appears."""
        leaked = [
            name
            for name in import_delta("loom.core.introspection").modules
            if any(name == root or name.startswith(f"{root}.") for root in _FORBIDDEN_ROOTS)
        ]

        assert leaked == []

    def test_importing_introspection_does_not_pull_in_fastapi_or_pydantic(self) -> None:
        """The REST stack is not a dependency of describing an application."""
        leaked = sorted(
            import_delta("loom.core.introspection").third_party & set(_FORBIDDEN_THIRD_PARTY)
        )

        assert leaked == []
