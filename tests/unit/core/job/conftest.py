"""Job test fixtures — pending dispatch context isolation."""

from __future__ import annotations

from collections.abc import Generator

import pytest

from loom.core.job.context import clear_pending_dispatches


@pytest.fixture(autouse=True)
def _reset_pending_dispatches() -> Generator[None, None, None]:
    """Clear the pending dispatch queue before and after every job test.

    Sync tests that call ``dispatch_after_commit()`` modify ``_pending`` in the main
    thread's context directly.  Without cleanup, these entries accumulate and
    are inherited by subsequent async tasks via context copy, causing
    cross-test contamination.
    """
    clear_pending_dispatches()
    yield
    clear_pending_dispatches()
