from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Literal, cast

import msgspec

from loom.core.model.enums import OnDelete, ServerDefault, ServerOnUpdate
from loom.core.model.keys import KeyHash

Reach = Literal["read", "write", "both"]


@dataclass(frozen=True, slots=True)
class ColumnType:
    """Backend column type descriptor stored inside ``Annotated[...]``.

    It is hashable, as ``Annotated`` metadata must be: the keyword options are
    held as ``(name, value)`` pairs.
    """

    type_name: str
    args: tuple[Any, ...] = ()
    kwargs: tuple[tuple[str, Any], ...] = ()

    def option(self, name: str, default: Any = None) -> Any:
        """Return the keyword option *name*, or *default* when it is not set."""
        return dict(self.kwargs).get(name, default)


@dataclass(frozen=True, slots=True)
class Field:
    """Column constraint metadata stored inside ``Annotated[...]``.

    ``nullable`` left as ``None`` follows the annotation: the column is nullable
    when the field admits ``None`` and is not part of the primary key. A
    declared ``True`` or ``False`` overrides that.
    """

    primary_key: bool = False
    unique: bool = False
    index: bool = False
    nullable: bool | None = None
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
    derived: KeyHash | None = None


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
    nullable: bool | None = None,
    autoincrement: bool = False,
    server_default: ServerDefault | None = None,
    server_onupdate: ServerOnUpdate | str | None = None,
    foreign_key: str | None = None,
    on_delete: OnDelete | None = None,
    default: Any = msgspec.UNSET,
    length: int | None = None,
    db_type: ColumnType | None = None,
    derived: KeyHash | None = None,
) -> Any:
    """Declare column metadata without using explicit ``Annotated`` syntax.

    ``derived`` marks the single primary-key field the repository fills from the
    model's ``__business_key__`` with that formula.
    """
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
                derived=derived,
            ),
        ),
    )


def CreatedAtField() -> Any:
    """Declare the column the store stamps with the time of the row's insertion."""
    return ColumnField(server_default=ServerDefault.NOW)


def UpdatedAtField() -> Any:
    """Declare the column the store stamps with the time of the row's insertion and every update."""
    return ColumnField(server_default=ServerDefault.NOW, server_onupdate=ServerOnUpdate.NOW)


def ScopedField(
    column_type: ColumnType | None = None,
    *,
    scope: str,
    on: Reach = "both",
    elevable: bool = False,
    primary_key: bool = False,
    nullable: bool | None = None,
    foreign_key: str | None = None,
    on_delete: OnDelete | None = None,
    length: int | None = None,
    db_type: ColumnType | None = None,
) -> Any:
    """Declare the column that scopes the rows of a ``RowScoped`` model.

    ``scope`` names the session value the column is compared with; ``on`` limits
    the comparison to reads or writes, and ``elevable`` lets an authorized
    execution lift a write-only scope within the table's boundary.
    """
    selected_type = db_type or column_type
    return cast(
        Any,
        ColumnFieldSpec(
            column_type=selected_type,
            field=Field(
                primary_key=primary_key,
                nullable=nullable,
                foreign_key=foreign_key,
                on_delete=on_delete,
                length=length,
                scope=scope,
                on=on,
                elevable=elevable,
            ),
        ),
    )


column_field = ColumnField
