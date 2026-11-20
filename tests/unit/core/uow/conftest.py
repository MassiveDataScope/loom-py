"""Arrangements for the unit-of-work suite.

The session double and the recording session manager come from the shared
``tests/unit/core`` conftest; only what the unit-of-work tests alone need
lives here.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from ..conftest import RecordingSession, RecordingSessionManager


@dataclass(frozen=True)
class SessionPair:
    """A session double together with the recording manager that hands it out."""

    session: RecordingSession
    manager: RecordingSessionManager


@pytest.fixture
def broken_rollback(session_double: RecordingSession) -> SessionPair:
    """A session whose ``rollback`` fails, with the manager that hands it out."""
    session_double.fail_on("rollback", ConnectionError("pool gone"))
    return SessionPair(session=session_double, manager=RecordingSessionManager(session_double))
