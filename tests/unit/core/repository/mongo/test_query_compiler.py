from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from uuid import UUID

import msgspec
import pytest

from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import (
    Cursor,
    FilterGroup,
    FilterOp,
    FilterSpec,
    SortSpec,
    UnsupportedQuery,
)
from loom.core.repository.abc.query_compiler import QueryCompiler
from loom.core.repository.mongo.query_compiler import MongoFilter, MongoQueryCompiler, MongoSort

from .conftest import Article

_REF = UUID("b7cdfa6d-0805-4c13-8071-af9317c08254")


class Ledger(BaseModel):
    """Every type the repository stores as a string."""

    __tablename__ = "ledger"

    id: UUID = ColumnField(primary_key=True)
    amount: Decimal = ColumnField()
    day: date = ColumnField()
    at: time = ColumnField()
    ref: UUID = ColumnField()


class _Pending(BaseModel):
    __tablename__ = "pending_ledger"

    id: UUID = ColumnField(primary_key=True)
    amount: Decimal | msgspec.UnsetType = msgspec.UNSET


def _single(spec: FilterSpec) -> FilterGroup:
    return FilterGroup(filters=(spec,))


@pytest.fixture
def ledger() -> MongoQueryCompiler:
    return MongoQueryCompiler(Ledger, "id")


class TestFieldMapping:
    def test_primary_key_maps_to_underscore_id(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("slug", FilterOp.EQ, "hello"))

        assert compiler.compile_filter(group) == {"$and": [{"_id": {"$eq": "hello"}}]}

    def test_other_fields_keep_their_name(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("published_at", FilterOp.EQ, datetime(2026, 1, 1, tzinfo=UTC)))

        assert compiler.compile_filter(group) == {
            "$and": [{"published_at": {"$eq": datetime(2026, 1, 1, tzinfo=UTC)}}]
        }

    def test_unknown_field_is_unsupported(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("author", FilterOp.EQ, "x"))

        with pytest.raises(UnsupportedQuery, match="'author'") as info:
            compiler.compile_filter(group)

        assert info.value.backend == "mongo"
        assert info.value.model == "Article"

    def test_dotted_path_is_unsupported(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("category.name", FilterOp.EQ, "x"))

        with pytest.raises(UnsupportedQuery, match="'category.name'"):
            compiler.compile_filter(group)


class TestOperators:
    def test_eq(self, compiler: MongoQueryCompiler) -> None:
        assert compiler.compile_filter(_single(FilterSpec("views", FilterOp.EQ, 3))) == {
            "$and": [{"views": {"$eq": 3}}]
        }

    def test_eq_never_lets_a_mapping_value_become_an_operator(
        self, compiler: MongoQueryCompiler
    ) -> None:
        group = _single(FilterSpec("title", FilterOp.EQ, {"$gt": ""}))

        assert compiler.compile_filter(group) == {"$and": [{"title": {"$eq": {"$gt": ""}}}]}

    @pytest.mark.parametrize(
        ("op", "mongo_op"),
        [
            (FilterOp.GT, "$gt"),
            (FilterOp.GTE, "$gte"),
            (FilterOp.LT, "$lt"),
            (FilterOp.LTE, "$lte"),
        ],
    )
    def test_comparisons(self, compiler: MongoQueryCompiler, op: FilterOp, mongo_op: str) -> None:
        assert compiler.compile_filter(_single(FilterSpec("views", op, 3))) == {
            "$and": [{"views": {mongo_op: 3}}]
        }

    def test_ne_guards_null_like_sql(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("published_at", FilterOp.NE, "x"))

        assert compiler.compile_filter(group) == {
            "$and": [{"$and": [{"published_at": {"$ne": None}}, {"published_at": {"$ne": "x"}}]}]
        }

    def test_in_coerces_value_to_list(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("views", FilterOp.IN, (1, 2)))

        assert compiler.compile_filter(group) == {"$and": [{"views": {"$in": [1, 2]}}]}

    def test_like_translates_sql_wildcards_and_escapes_the_rest(
        self, compiler: MongoQueryCompiler
    ) -> None:
        group = _single(FilterSpec("title", FilterOp.LIKE, "a%b_c.d(e"))

        assert compiler.compile_filter(group) == {
            "$and": [{"title": {"$regex": r"^a.*b.c\.d\(e$"}}]
        }

    def test_like_collapses_runs_of_percent(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("title", FilterOp.LIKE, "%%%%x"))

        assert compiler.compile_filter(group) == {"$and": [{"title": {"$regex": "^.*x$"}}]}

    @pytest.mark.parametrize("op", [FilterOp.LIKE, FilterOp.ILIKE])
    def test_like_caps_the_number_of_wildcards(
        self, compiler: MongoQueryCompiler, op: FilterOp
    ) -> None:
        assert compiler.compile_filter(_single(FilterSpec("title", op, "%a" * 8)))
        group = _single(FilterSpec("title", op, "_a" * 9))

        with pytest.raises(UnsupportedQuery, match="wildcards"):
            compiler.compile_filter(group)

    def test_ilike_adds_case_insensitive_option(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("title", FilterOp.ILIKE, "%loom%"))

        assert compiler.compile_filter(group) == {
            "$and": [{"title": {"$regex": "^.*loom.*$", "$options": "i"}}]
        }

    @pytest.mark.parametrize("value", [None, True, False])
    def test_is_null_ignores_the_value_like_sqlalchemy(
        self, compiler: MongoQueryCompiler, value: object
    ) -> None:
        group = _single(FilterSpec("published_at", FilterOp.IS_NULL, value))

        assert compiler.compile_filter(group) == {"$and": [{"published_at": None}]}

    @pytest.mark.parametrize("op", [FilterOp.EXISTS, FilterOp.NOT_EXISTS])
    def test_relation_operators_are_unsupported(
        self, compiler: MongoQueryCompiler, op: FilterOp
    ) -> None:
        group = _single(FilterSpec("title", op))

        with pytest.raises(UnsupportedQuery, match=f"{op.value}.*mongo|mongo.*{op.value}") as info:
            compiler.compile_filter(group)

        assert info.value.backend == "mongo"
        assert op.value in info.value.reason


class TestStorageValues:
    """Filter values take the form the document stores (FR: same conversion as writes)."""

    @pytest.mark.parametrize(
        ("field", "value", "stored"),
        [
            ("day", date(2026, 1, 3), "2026-01-03"),
            ("at", time(12, 30), "12:30:00"),
            ("amount", Decimal("19.90"), "19.90"),
            ("ref", _REF, str(_REF)),
        ],
    )
    def test_eq_converts_the_value(
        self, ledger: MongoQueryCompiler, field: str, value: object, stored: object
    ) -> None:
        group = _single(FilterSpec(field, FilterOp.EQ, value))

        assert ledger.compile_filter(group) == {"$and": [{field: {"$eq": stored}}]}

    def test_datetime_takes_the_bson_form(self, compiler: MongoQueryCompiler) -> None:
        """Naive means UTC and BSON keeps milliseconds, so the value is compared as stored."""
        group = _single(
            FilterSpec("published_at", FilterOp.GT, datetime(2026, 1, 3, 12, 0, 0, 123456))
        )

        assert compiler.compile_filter(group) == {
            "$and": [{"published_at": {"$gt": datetime(2026, 1, 3, 12, 0, 0, 123000, tzinfo=UTC)}}]
        }

    def test_in_converts_every_member(self, ledger: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("day", FilterOp.IN, (date(2026, 1, 3), date(2026, 1, 4))))

        assert ledger.compile_filter(group) == {
            "$and": [{"day": {"$in": ["2026-01-03", "2026-01-04"]}}]
        }

    def test_ne_converts_the_value(self, ledger: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("ref", FilterOp.NE, _REF))

        assert ledger.compile_filter(group) == {
            "$and": [{"$and": [{"ref": {"$ne": None}}, {"ref": {"$ne": str(_REF)}}]}]
        }

    def test_id_to_storage_converts_key_values_only(self) -> None:
        """The id policy's storage form applies to ``EQ`` and every ``IN`` member of the key."""
        compiler = MongoQueryCompiler(Article, "slug", id_to_storage=lambda value: f"key:{value}")
        group = FilterGroup(
            filters=(
                FilterSpec("slug", FilterOp.EQ, "a"),
                FilterSpec("slug", FilterOp.IN, ("b", "c")),
                FilterSpec("title", FilterOp.EQ, "d"),
                FilterSpec("title", FilterOp.IN, ("e",)),
            )
        )

        assert compiler.compile_filter(group) == {
            "$and": [
                {"_id": {"$eq": "key:a"}},
                {"_id": {"$in": ["key:b", "key:c"]}},
                {"title": {"$eq": "d"}},
                {"title": {"$in": ["e"]}},
            ]
        }

    def test_date_range_compares_iso_strings(self, ledger: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("day", FilterOp.GTE, date(2026, 1, 3)))

        assert ledger.compile_filter(group) == {"$and": [{"day": {"$gte": "2026-01-03"}}]}

    @pytest.mark.parametrize("op", [FilterOp.GT, FilterOp.GTE, FilterOp.LT, FilterOp.LTE])
    def test_decimal_range_is_unsupported(self, ledger: MongoQueryCompiler, op: FilterOp) -> None:
        group = _single(FilterSpec("amount", op, Decimal("1")))

        with pytest.raises(UnsupportedQuery, match="'amount'.*Decimal") as info:
            ledger.compile_filter(group)

        assert info.value.backend == "mongo"

    def test_decimal_sort_is_unsupported(self, ledger: MongoQueryCompiler) -> None:
        sort = (SortSpec("amount"),)

        with pytest.raises(UnsupportedQuery, match="'amount'.*Decimal"):
            ledger.compile_sort(sort)

    def test_an_unset_decimal_range_and_sort_are_unsupported_like_a_decimal(self) -> None:
        compiler = MongoQueryCompiler(_Pending, "id")
        group = _single(FilterSpec("amount", FilterOp.GT, Decimal("1")))

        with pytest.raises(UnsupportedQuery, match="'amount'.*Decimal"):
            compiler.compile_filter(group)
        with pytest.raises(UnsupportedQuery, match="'amount'.*Decimal"):
            compiler.compile_sort((SortSpec("amount"),))


class TestGroups:
    def test_and_group(self, compiler: MongoQueryCompiler) -> None:
        group = FilterGroup(
            filters=(FilterSpec("views", FilterOp.GTE, 1), FilterSpec("views", FilterOp.LTE, 9))
        )

        assert compiler.compile_filter(group) == {
            "$and": [{"views": {"$gte": 1}}, {"views": {"$lte": 9}}]
        }

    def test_or_group(self, compiler: MongoQueryCompiler) -> None:
        group = FilterGroup(
            filters=(FilterSpec("title", FilterOp.EQ, "a"), FilterSpec("slug", FilterOp.EQ, "b")),
            op="OR",
        )

        assert compiler.compile_filter(group) == {
            "$or": [{"title": {"$eq": "a"}}, {"_id": {"$eq": "b"}}]
        }

    def test_empty_group_matches_everything(self, compiler: MongoQueryCompiler) -> None:
        assert compiler.compile_filter(FilterGroup(filters=())) == {}


class TestSort:
    def test_sort_uses_pymongo_pairs_with_pk_mapped(self, compiler: MongoQueryCompiler) -> None:
        sort = (SortSpec("views", "DESC"), SortSpec("slug"))

        assert compiler.compile_sort(sort) == [("views", -1), ("_id", 1)]

    def test_empty_sort(self, compiler: MongoQueryCompiler) -> None:
        assert compiler.compile_sort(()) == []

    def test_unknown_sort_field_is_unsupported(self, compiler: MongoQueryCompiler) -> None:
        sort = (SortSpec("rank"),)

        with pytest.raises(UnsupportedQuery, match="'rank'"):
            compiler.compile_sort(sort)


class TestCursorFilter:
    def test_keyset_predicate_expands_lexicographically(self, compiler: MongoQueryCompiler) -> None:
        sort = (SortSpec("views", "DESC"), SortSpec("title"))
        cursor = Cursor(backend="mongo", keys=(10, "m"), tie_breaker="k")

        assert compiler.compile_cursor_filter(sort, cursor) == {
            "$or": [
                {"$and": [{"views": {"$lt": 10}}]},
                {"$and": [{"views": 10}, {"title": {"$gt": "m"}}]},
                {"$and": [{"views": 10}, {"title": "m"}, {"_id": {"$gt": "k"}}]},
            ]
        }

    def test_without_sort_only_the_tie_breaker_positions(
        self, compiler: MongoQueryCompiler
    ) -> None:
        cursor = Cursor(backend="mongo", keys=(), tie_breaker="k")

        assert compiler.compile_cursor_filter((), cursor) == {
            "$or": [{"$and": [{"_id": {"$gt": "k"}}]}]
        }

    def test_sort_naming_the_key_steps_in_its_own_direction(
        self, compiler: MongoQueryCompiler
    ) -> None:
        """No ``_id`` tie-breaker is appended when the sort already names the key."""
        sort = (SortSpec("views", "DESC"), SortSpec("slug", "DESC"))
        cursor = Cursor(backend="mongo", keys=(10, "k"), tie_breaker="k")

        assert compiler.compile_cursor_filter(sort, cursor) == {
            "$or": [
                {"$and": [{"views": {"$lt": 10}}]},
                {"$and": [{"views": 10}, {"_id": {"$lt": "k"}}]},
            ]
        }


class TestProtocol:
    def test_mongo_compiler_satisfies_query_compiler(self) -> None:
        compiler: QueryCompiler[MongoFilter, MongoSort] = MongoQueryCompiler(Article, "slug")

        assert compiler.compile_sort((SortSpec("views", "DESC"),)) == [("views", -1)]
