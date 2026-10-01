from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

pytestmark = pytest.mark.integration


async def test_each_module_gets_its_own_database(
    module_database_uri: str, admin_connection
) -> None:
    current = (await admin_connection.execute(text("SELECT current_database()"))).scalar_one()
    assert current == make_url(module_database_uri).database
    assert current.startswith("loom_rls_")
