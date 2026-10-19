from __future__ import annotations

import dataclasses

import pytest

from loom.core.use_case import Created


class TestCreated:
    def test_carries_the_created_value(self) -> None:
        assert Created("grant-1").value == "grant-1"

    def test_is_equal_by_value(self) -> None:
        assert Created(1) == Created(1)
        assert Created(1) != Created(2)

    def test_is_frozen(self) -> None:
        created = Created(1)

        with pytest.raises(dataclasses.FrozenInstanceError):
            created.value = 2  # type: ignore[misc]

    def test_is_generic(self) -> None:
        assert Created[int].__origin__ is Created  # type: ignore[attr-defined]
