from __future__ import annotations

from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry, scoped_tables
from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer, String


class First(BaseModel, RowScoped):
    __tablename__ = "first"
    key: str = ScopedField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)


class Second(BaseModel, RowScoped):
    __tablename__ = "second"
    region: int = ScopedField(Integer, primary_key=True, scope="region")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)


def test_compiling_into_a_given_metadata_leaves_the_shared_one_untouched() -> None:
    reset_registry()
    own = MetaData()

    compile_all(First, metadata=own)

    assert "first" in own.tables
    assert "first" not in get_metadata().tables


def test_two_applications_in_one_process_keep_separate_tables_and_registries() -> None:
    reset_registry()
    first_metadata = MetaData()
    second_metadata = MetaData()

    compile_all(First, metadata=first_metadata)
    compile_all(Second, metadata=second_metadata)

    assert set(first_metadata.tables) == {"first"}
    assert set(second_metadata.tables) == {"second"}
    assert set(scoped_tables(first_metadata)) == {(None, "first")}
    assert set(scoped_tables(second_metadata)) == {(None, "second")}


def test_the_same_model_can_be_compiled_into_two_metadatas() -> None:
    reset_registry()
    one = MetaData()
    two = MetaData()

    compile_all(First, metadata=one)
    compile_all(First, metadata=two)

    assert one.tables["first"] is not two.tables["first"]


def test_omitting_metadata_keeps_the_shared_registry_behaviour() -> None:
    reset_registry()

    compile_all(First)

    assert "first" in get_metadata().tables
    assert set(scoped_tables()) == {(None, "first")}
    reset_registry()
