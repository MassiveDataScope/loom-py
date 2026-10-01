from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Literal, cast

import msgspec

from loom.core.model.enums import OnDelete, ServerDefault, ServerOnUpdate

Reach = Literal["read", "write", "both"]


@dataclass(frozen=True, slots=True)
class ColumnType:
    """Backend column type descriptor stored inside ``Annotated[...]``."""

    type_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = dataclass_field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Field:
    """Column constraint metadata stored inside ``Annotated[...]``."""

    primary_key: bool = False
    unique: bool = False
    index: bool = False
    nullable: bool = False
    autoincrement: bool = False
    server_default: ServerDefault | None = None
    server_onupdate: ServerOnUpdate | str | None = None
    foreign_key: str | None = None
    on_delete: OnDelete | None = None
    default: Any = msgspec.UNSET
    length: int | None = None
    scope: str | None = None
    on: Reach = "both"
    elevable: bool = False


@dataclass(frozen=True, slots=True)
class ColumnFieldSpec:
    """Column declaration object for ``BaseModel`` class attributes."""

    column_type: ColumnType | None = None
    field: Field = dataclass_field(default_factory=Field)


def ColumnField(
    column_type: ColumnType | None = None,
    *,
    primary_key: bool = False,
    unique: bool = False,
    index: bool = False,
    nullable: bool = False,
    autoincrement: bool = False,
    server_default: ServerDefault | None = None,
    server_onupdate: ServerOnUpdate | str | None = None,
    foreign_key: str | None = None,
    on_delete: OnDelete | None = None,
    default: Any = msgspec.UNSET,
    length: int | None = None,
    db_type: ColumnType | None = None,
    scope: str | None = None,
    on: Reach = "both",
    elevable: bool = False,
) -> Any:
    """Declare column metadata without using explicit ``Annotated`` syntax."""
    selected_type = db_type or column_type
    return cast(
        Any,
        ColumnFieldSpec(
            column_type=selected_type,
            field=Field(
                primary_key=primary_key,
                unique=unique,
                index=index,
                nullable=nullable,
                autoincrement=autoincrement,
                server_default=server_default,
                server_onupdate=server_onupdate,
                foreign_key=foreign_key,
                on_delete=on_delete,
                default=default,
                length=length,
                scope=scope,
                on=on,
                elevable=elevable,
            ),
        ),
    )


column_field = ColumnField
