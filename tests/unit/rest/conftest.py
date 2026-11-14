"""Fixtures shared by the REST unit tests."""

from __future__ import annotations

import pytest

from tests.helpers.composition import application_openings, fixture_project

__all__ = ["application_openings", "fixture_project"]


@pytest.fixture
def aws_test_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dummy credentials satisfy boto3's default chain against a local endpoint."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
