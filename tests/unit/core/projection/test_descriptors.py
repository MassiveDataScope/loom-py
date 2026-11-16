"""Unit tests for the projection descriptor registry and its two sides."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

from loom.core.projection.loaders import (
    _DESCRIPTOR_TYPES,
    CountLoader,
    ExistsLoader,
    JoinFieldsLoader,
    _MemoryCountLoader,
    is_projection_descriptor,
    make_memory_loader,
    projection_descriptor,
    projection_descriptor_types,
    resolve_model_reference,
)
from loom.stores.sqlalchemy.loaders import (
    _SQL_LOADER_FACTORIES,
    _SqlCountLoader,
    _SqlExistsLoader,
    _SqlJoinFieldsLoader,
    make_sql_loader,
)
from tests.unit.core.doubles import AcmeNote


class _CustomLoader:
    """A user-supplied loader that is not a registered descriptor."""

    model = AcmeNote

    def load_from_object(self, obj: Any) -> int:
        _ = obj
        return 0


class _CountSubclass(CountLoader):
    """A descriptor specialisation, to pin subclass resolution on both sides."""


class _FakeRelationStep:
    def __init__(self) -> None:
        self.target_table = object()
        self.fk_col = "note_id"
        self.attr = "notes"


@dataclass(frozen=True, slots=True)
class _UnbackedLoader:
    """A descriptor with no SQL-path factory, registered only for one test."""

    model: type
    via: str | None = None

    def build_memory_loader(self, relation: str) -> Any:
        return relation


@pytest.fixture
def unbacked_descriptor() -> Iterator[type[_UnbackedLoader]]:
    """Register :class:`_UnbackedLoader` for the duration of one test."""
    registered = list(_DESCRIPTOR_TYPES)
    try:
        yield projection_descriptor(_UnbackedLoader)
    finally:
        _DESCRIPTOR_TYPES[:] = registered


# Every registered descriptor needs one sample instance here: the completeness
# test below fails when a new descriptor is added without one.
_DESCRIPTOR_SAMPLES: tuple[Any, ...] = (
    CountLoader(model=AcmeNote),
    ExistsLoader(model=AcmeNote),
    JoinFieldsLoader(model=AcmeNote, value_columns=("id",)),
)


class TestRegistryCompleteness:
    def test_every_descriptor_has_a_sql_factory(self) -> None:
        missing = [
            descriptor.__name__
            for descriptor in projection_descriptor_types()
            if descriptor not in _SQL_LOADER_FACTORIES
        ]

        assert missing == []

    def test_every_descriptor_builds_a_memory_loader(self) -> None:
        for descriptor in _DESCRIPTOR_SAMPLES:
            memory_loader = descriptor.build_memory_loader("notes")

            assert callable(getattr(memory_loader, "load_from_object", None))

    def test_every_descriptor_is_covered_by_a_sample(self) -> None:
        covered = {type(sample) for sample in _DESCRIPTOR_SAMPLES}

        assert covered == set(projection_descriptor_types())

    def test_sql_factory_table_has_no_entry_without_a_descriptor(self) -> None:
        unregistered = [
            descriptor.__name__
            for descriptor in _SQL_LOADER_FACTORIES
            if descriptor not in projection_descriptor_types()
        ]

        assert unregistered == []


class TestIsProjectionDescriptor:
    @pytest.mark.parametrize("descriptor", _DESCRIPTOR_SAMPLES)
    def test_true_for_a_registered_descriptor(self, descriptor: Any) -> None:
        assert is_projection_descriptor(descriptor) is True

    def test_true_for_a_descriptor_subclass(self) -> None:
        assert is_projection_descriptor(_CountSubclass(model=AcmeNote)) is True

    def test_false_for_a_custom_loader(self) -> None:
        assert is_projection_descriptor(_CustomLoader()) is False


class TestMakeMemoryLoader:
    def test_delegates_to_the_descriptor(self) -> None:
        loader = make_memory_loader(CountLoader(model=AcmeNote), "notes")

        assert loader == _MemoryCountLoader(relation="notes")

    def test_join_fields_carries_its_value_columns(self) -> None:
        loader = make_memory_loader(
            JoinFieldsLoader(model=AcmeNote, value_columns=("id", "title")), "notes"
        )

        assert loader.value_columns == ("id", "title")

    def test_returns_a_custom_loader_unchanged(self) -> None:
        custom = _CustomLoader()

        assert make_memory_loader(custom, "notes") is custom


class TestMakeSqlLoader:
    def test_count_descriptor_selects_the_count_loader(self) -> None:
        loader = make_sql_loader(CountLoader(model=AcmeNote), _FakeRelationStep())

        assert isinstance(loader, _SqlCountLoader)

    def test_exists_descriptor_selects_the_exists_loader(self) -> None:
        loader = make_sql_loader(ExistsLoader(model=AcmeNote), _FakeRelationStep())

        assert isinstance(loader, _SqlExistsLoader)

    def test_join_fields_descriptor_selects_the_join_fields_loader(self) -> None:
        loader = make_sql_loader(
            JoinFieldsLoader(model=AcmeNote, value_columns=("id",)), _FakeRelationStep()
        )

        assert isinstance(loader, _SqlJoinFieldsLoader)

    def test_descriptor_subclass_reuses_the_base_factory(self) -> None:
        loader = make_sql_loader(_CountSubclass(model=AcmeNote), _FakeRelationStep())

        assert isinstance(loader, _SqlCountLoader)

    def test_returns_a_custom_loader_unchanged(self) -> None:
        custom = _CustomLoader()

        assert make_sql_loader(custom, _FakeRelationStep()) is custom


class TestResolveModelReference:
    def test_returns_a_direct_class(self) -> None:
        assert resolve_model_reference(AcmeNote) is AcmeNote

    def test_calls_a_lambda_wrapped_forward_reference(self) -> None:
        assert resolve_model_reference(lambda: AcmeNote) is AcmeNote

    def test_returns_none_when_the_callable_raises(self) -> None:
        def broken() -> type:
            raise RuntimeError("not importable yet")

        assert resolve_model_reference(broken) is None

    def test_returns_none_for_a_non_type_value(self) -> None:
        assert resolve_model_reference("Note") is None


class TestUnbackedDescriptor:
    def test_registration_is_idempotent(self, unbacked_descriptor: type[Any]) -> None:
        projection_descriptor(unbacked_descriptor)

        assert projection_descriptor_types().count(unbacked_descriptor) == 1

    def test_make_sql_loader_raises_for_a_descriptor_without_a_factory(
        self, unbacked_descriptor: type[Any]
    ) -> None:
        loader = unbacked_descriptor(model=AcmeNote)
        step = _FakeRelationStep()

        with pytest.raises(ValueError, match="no SQL-path loader factory"):
            make_sql_loader(loader, step)
