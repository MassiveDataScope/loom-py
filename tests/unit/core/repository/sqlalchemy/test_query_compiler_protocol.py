from __future__ import annotations

from typing import Any

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import CursorCodec, FilterGroup, FilterOp, FilterSpec
from loom.core.repository.abc.query_compiler import QueryCompiler
from loom.core.repository.sqlalchemy.query_compiler.compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.query_compiler.errors import UnsafeFilterError


class _Row(BaseModel):
    __tablename__ = "protocol_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    rank: int = ColumnField()


def _compiler(allowed: frozenset[str] = frozenset()) -> QuerySpecCompiler:
    compile_all(_Row)
    sa_model = get_compiled(_Row)
    assert sa_model is not None
    return QuerySpecCompiler(
        sa_model,
        sa_model.id,
        allowed,
        backend="sqlalchemy",
        model=_Row,
        cursor_codec=CursorCodec(signing="ephemeral"),
        scope="",
        columns={},
    )


def test_sqlalchemy_compiler_satisfies_query_compiler() -> None:
    compiler: QueryCompiler[Any, Any] = _compiler()

    clause = compiler.compile_filter(FilterGroup(filters=(FilterSpec("rank", FilterOp.EQ, 1),)))

    assert "rank" in str(clause)


def test_compile_filter_honours_allowed_fields() -> None:
    compiler = _compiler(frozenset({"id"}))
    group = FilterGroup(filters=(FilterSpec("rank", FilterOp.EQ, 1),))

    with pytest.raises(UnsafeFilterError):
        compiler.compile_filter(group)
