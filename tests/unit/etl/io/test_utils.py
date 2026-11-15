"""Unit tests for low-level I/O helpers."""

from __future__ import annotations

from loom.etl.declarative._utils import _clone_slots


class _SlotObj:
    __slots__ = ("a", "b")

    def __init__(self, a: int, b: str) -> None:
        self.a = a
        self.b = b


class TestCloneSlots:
    def test_clone_slots_copies_all_declared_slots(self) -> None:
        src = _SlotObj(1, "x")

        cloned = _clone_slots(src, _SlotObj, _SlotObj.__slots__)

        assert isinstance(cloned, _SlotObj)
        assert cloned is not src
        assert cloned.a == 1
        assert cloned.b == "x"

    def test_clone_slots_is_shallow_copy(self) -> None:
        src = _SlotObj(1, "x")
        cloned = _clone_slots(src, _SlotObj, _SlotObj.__slots__)

        cloned.a = 99
        assert src.a == 1
