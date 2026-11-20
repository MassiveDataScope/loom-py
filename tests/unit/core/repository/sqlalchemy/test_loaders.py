"""Unit tests for relation-based projection loaders."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from loom.core.projection.loaders import (
    _MemoryCountLoader,
    _MemoryExistsLoader,
    _MemoryJoinFieldsLoader,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeNote:
    def __init__(self, note_id: int, title: str) -> None:
        self.id = note_id
        self.title = title


def _obj_with_relation(relation_name: str, related: list[Any]) -> Any:
    return SimpleNamespace(**{relation_name: related})


# ---------------------------------------------------------------------------
# load_from_object — happy path
# ---------------------------------------------------------------------------


class TestMemoryLoadersLoadFromObject:
    def test_count_applied_to_related_list(self) -> None:
        notes = [_FakeNote(1, "a"), _FakeNote(2, "b")]
        loader = _MemoryCountLoader(relation="notes")
        obj = _obj_with_relation("notes", notes)

        assert loader.load_from_object(obj) == 2

    def test_join_fields_receives_full_list(self) -> None:
        notes = [_FakeNote(1, "x"), _FakeNote(2, "y"), _FakeNote(3, "z")]
        loader = _MemoryJoinFieldsLoader(relation="notes", value_columns=("title",))
        obj = _obj_with_relation("notes", notes)

        assert loader.load_from_object(obj) == [
            {"title": "x"},
            {"title": "y"},
            {"title": "z"},
        ]

    def test_exists_false_for_empty_relation(self) -> None:
        loader = _MemoryExistsLoader(relation="notes")
        obj = _obj_with_relation("notes", [])

        assert loader.load_from_object(obj) is False

    def test_count_none_relation_treated_as_empty(self) -> None:
        loader = _MemoryCountLoader(relation="notes")
        obj = SimpleNamespace(notes=None)

        assert loader.load_from_object(obj) == 0

    def test_join_fields_single_object_wrapped_in_list(self) -> None:
        owner = _FakeNote(7, "owner")
        loader = _MemoryJoinFieldsLoader(relation="owner", value_columns=("id",))
        obj = SimpleNamespace(owner=owner)

        assert loader.load_from_object(obj) == [{"id": 7}]


# ---------------------------------------------------------------------------
# load_from_object — safety check
# ---------------------------------------------------------------------------


class TestMemoryLoadersSafetyCheck:
    def test_raises_when_relation_not_present(self) -> None:
        loader = _MemoryCountLoader(relation="notes")

        class _Bare:
            pass

        bare = _Bare()
        with pytest.raises(RuntimeError, match="relation 'notes' on _Bare"):
            loader.load_from_object(bare)

    def test_error_includes_model_class_name(self) -> None:
        loader = _MemoryCountLoader(relation="items")

        class MyEntity:
            pass

        entity = MyEntity()
        with pytest.raises(RuntimeError, match="MyEntity"):
            loader.load_from_object(entity)

    def test_error_includes_relation_name(self) -> None:
        loader = _MemoryCountLoader(relation="attachments")

        class SomeModel:
            pass

        model = SomeModel()
        with pytest.raises(RuntimeError, match="attachments"):
            loader.load_from_object(model)
