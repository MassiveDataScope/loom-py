from __future__ import annotations

import pytest

from loom.core.stores.repository import PageParams
from loom.testing import make


@pytest.fixture
def page_params() -> PageParams:
    return make(PageParams, page=2, limit=10)
