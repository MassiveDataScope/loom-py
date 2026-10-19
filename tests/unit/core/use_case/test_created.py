from __future__ import annotations

from loom.core.use_case import Created


def test_created_carries_the_created_value() -> None:
    assert Created("item-1").value == "item-1"
