import msgspec
import pytest
from msgspec import UnsetType

from loom.core.model import BaseModel, Cardinality, ColumnField, ProjectionField, RelationField


def _field_types(model: type[msgspec.Struct]) -> dict[str, object]:
    return {field.name: field.type for field in msgspec.structs.fields(model)}


class TestAnnotationResolution:
    def test_annotations_become_fields_and_relations_are_widened(self) -> None:
        class Page(msgspec.Struct):
            number: int

        class Chapter(BaseModel):
            __tablename__ = "annotation_resolution_chapters"
            id: int = ColumnField(primary_key=True, autoincrement=True)
            pages: list[Page] = RelationField(
                foreign_key="chapter_id",
                cardinality=Cardinality.ONE_TO_MANY,
            )
            page_count: int = ProjectionField(loader=None, default=0)

        assert _field_types(Chapter) == {
            "id": int,
            "pages": list[Page] | UnsetType,
            "page_count": int | UnsetType,
        }
        assert getattr(Chapter, "__annotate__", None) is None

    def test_subclass_without_annotations_keeps_inherited_fields(self) -> None:
        class Shelf(BaseModel):
            __tablename__ = "annotation_resolution_shelves"
            id: int = ColumnField(primary_key=True, autoincrement=True)

        class ArchivedShelf(Shelf):
            pass

        assert _field_types(ArchivedShelf) == _field_types(Shelf)

    @pytest.mark.parametrize("header", ["", "from __future__ import annotations\n"])
    def test_an_undefined_annotation_name_fails_at_class_creation(self, header: str) -> None:
        source = (
            f"{header}"
            "class Broken(BaseModel):\n"
            "    loans: list[Missing] = RelationField(\n"
            "        foreign_key='broken_id', cardinality=ONE_TO_MANY\n"
            "    )\n"
        )
        namespace = {
            "BaseModel": BaseModel,
            "RelationField": RelationField,
            "ONE_TO_MANY": Cardinality.ONE_TO_MANY,
        }
        with pytest.raises(NameError, match="Missing"):
            exec(source, namespace)
