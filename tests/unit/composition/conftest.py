"""Fixtures shared by the tests that compose an application."""

from tests.helpers.composition import application_openings, fixture_project
from tests.helpers.loops import event_loops

__all__ = ["application_openings", "event_loops", "fixture_project"]
