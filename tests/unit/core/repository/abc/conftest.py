from __future__ import annotations

import pytest

from loom.core.stores.repository import PageParams


@pytest.fixture
def page_params() -> PageParams:
    return PageParams(page=2, limit=10)
