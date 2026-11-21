"""Contract ``test_business_keys`` against a real MongoDB; see ``conftest.py``."""

from __future__ import annotations

import pytest

from tests.unit.core.repository.contract.test_business_keys import *  # noqa: F403

pytestmark = [pytest.mark.integration, pytest.mark.mongo]
