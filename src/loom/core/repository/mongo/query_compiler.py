"""QuerySpec compilation for MongoDB.

Emits plain filter documents and pymongo-style sort lists; no driver import
is needed here, so the compiler is testable without the ``mongo`` extra.

Null semantics follow SQL three-valued logic so every backend selects the
same rows for one ``QuerySpec``: ``NE`` never matches a ``null`` field, hence
it is emitted with a ``{field: {"$ne": None}}`` guard (MongoDB's bare ``$ne``
would match ``null`` and missing fields).

Values are compared in their storage form (:mod:`loom.core.repository.mongo.values`);
primary-key values take the form the repository's ``IdPolicy`` writes to
``_id``, supplied as ``id_to_storage``. ``Decimal`` columns are stored as
strings, whose order is lexicographic, so range operators and sorts on them
are refused; ISO dates order correctly as strings and are allowed. Every
value is emitted under an explicit operator
(``$eq`` for equality) so a mapping supplied as a value can never act as one.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from decimal import Decimal
from typing import Any, ClassVar

from loom.core.errors import UnsupportedQuery
from loom.core.model.introspection import get_column_fields
from loom.core.repository.abc.cursor import Cursor
from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec, SortSpec
from loom.core.repository.mongo.values import to_storage_value

MongoFilter = dict[str, Any]
MongoSort = list[tuple[str, int]]
StorageConverter = Callable[[object], object]

_ID = "_id"
_ASCENDING = 1
_DESCENDING = -1
_COMPARISON_OPS: dict[FilterOp, str] = {
    FilterOp.GT: "$gt",
    FilterOp.GTE: "$gte",
    FilterOp.LT: "$lt",
    FilterOp.LTE: "$lte",
}
_STEP_OPS: dict[str, str] = {"ASC": "$gt", "DESC": "$lt"}
_WILDCARDS: dict[str, str] = {"%": ".*", "_": "."}
_PERCENT_RUN = re.compile(r"%+")
_MAX_WILDCARDS = 8


class _TooManyWildcards(ValueError):
    """Raised by the LIKE builders; ``_compile_spec`` turns it into ``UnsupportedQuery``."""


def _like_to_regex(pattern: str) -> str:
    """Translate a SQL ``LIKE`` pattern into an anchored regular expression.

    Runs of ``%`` collapse to one ``.*`` and the wildcard count is capped, so
    a pattern cannot compile into a regex whose backtracking grows unbounded.
    """
    collapsed = _PERCENT_RUN.sub("%", pattern)
    wildcards = sum(char in _WILDCARDS for char in collapsed)
    if wildcards > _MAX_WILDCARDS:
        raise _TooManyWildcards(
            f"LIKE pattern has {wildcards} wildcards; at most {_MAX_WILDCARDS} are supported"
        )
    body = "".join(_WILDCARDS.get(char) or re.escape(char) for char in collapsed)
    return f"^{body}$"


def _like(value: Any) -> MongoFilter:
    return {"$regex": _like_to_regex(str(value))}


def _ilike(value: Any) -> MongoFilter:
    return {"$regex": _like_to_regex(str(value)), "$options": "i"}


def _identity(value: object) -> object:
    return value


def _compare(operator: str) -> Callable[[Any, StorageConverter], MongoFilter]:
    return lambda value, convert: {operator: convert(value)}


# Each builder receives the column's storage converter; IS_NULL, LIKE and
# ILIKE ignore it (IS_NULL also ignores the value, like the SQLAlchemy compiler).
_VALUE_OPS: dict[FilterOp, Callable[[Any, StorageConverter], Any]] = {
    FilterOp.EQ: lambda value, convert: {"$eq": convert(value)},
    FilterOp.IN: lambda value, convert: {"$in": [convert(item) for item in value]},
    FilterOp.LIKE: lambda value, _convert: _like(value),
    FilterOp.ILIKE: lambda value, _convert: _ilike(value),
    FilterOp.IS_NULL: lambda _value, _convert: None,
    **{op: _compare(key) for op, key in _COMPARISON_OPS.items()},
}


class MongoQueryCompiler:
    """Compiles :class:`QuerySpec` parts into MongoDB filter and sort documents.

    The primary key is stored as ``_id``; every other field keeps its model
    name. Only flat AND/OR groups are supported, per the ``FilterGroup``
    contract.

    Args:
        model: Loom model the collection is bound to; its column fields are
            the only filterable and sortable names.
        id_field: Name of the model's primary-key field, mapped to ``_id``.
        id_to_storage: Converts a model-level key into the value stored in
            ``_id`` (the repository's ``IdPolicy.to_storage``); defaults to
            identity. Every key value in a filter or cursor goes through it.

    Example::

        compiler = MongoQueryCompiler(Article, "slug")
        compiler.compile_filter(FilterGroup(filters=(FilterSpec("slug", FilterOp.EQ, "a"),)))
        # {"$and": [{"_id": {"$eq": "a"}}]}
    """

    backend: ClassVar[str] = "mongo"

    def __init__(
        self, model: type, id_field: str, id_to_storage: StorageConverter = _identity
    ) -> None:
        self._model_name = model.__qualname__
        self._id_field = id_field
        self._id_to_storage = id_to_storage
        columns = get_column_fields(model)
        self._fields = frozenset(columns)
        self._unordered = frozenset(
            name for name, info in columns.items() if info.python_type is Decimal
        )

    def compile_filter(self, group: FilterGroup) -> MongoFilter:
        """Compile a filter group into a Mongo filter document.

        Args:
            group: Flat AND/OR group of field conditions.

        Returns:
            ``{"$and": [...]}`` or ``{"$or": [...]}``; ``{}`` for an empty
            group.

        Raises:
            UnsupportedQuery: On an unknown field, a relation operator, a
                range operator on a ``Decimal`` column or a ``LIKE`` pattern
                with too many wildcards.
        """
        if not group.filters:
            return {}
        clauses = [self._compile_spec(spec) for spec in group.filters]
        return {"$or" if group.op == "OR" else "$and": clauses}

    def compile_sort(self, sort: tuple[SortSpec, ...]) -> MongoSort:
        """Compile sort directives into pymongo ``(field, direction)`` pairs.

        Args:
            sort: Ordered sort directives.

        Returns:
            List of ``(field, 1 | -1)`` pairs; empty when ``sort`` is empty.

        Raises:
            UnsupportedQuery: On an unknown sort field or a ``Decimal`` one.
        """
        for spec in sort:
            self._require_ordered(spec.field)
        return [
            (self._column(spec.field), _DESCENDING if spec.direction == "DESC" else _ASCENDING)
            for spec in sort
        ]

    def compile_cursor_filter(self, sort: tuple[SortSpec, ...], cursor: Cursor) -> MongoFilter:
        """Build the keyset predicate positioning a page after ``cursor``.

        The row-value comparison is expanded as an ``$or`` of ``$and``
        branches: ``k1 > v1 OR (k1 = v1 AND k2 < v2) OR (... AND _id > vid)``.
        ``_id`` ascending is the final tie-breaker unless the sort already
        names the primary key, in which case that directive's direction is
        the last step.

        Args:
            sort: Sort directives the page is ordered by.
            cursor: Decoded cursor whose keys match ``sort`` one to one.

        Returns:
            Mongo filter document.

        Raises:
            UnsupportedQuery: If a sort field is unknown.
        """
        steps = [(self._column(spec.field), _STEP_OPS[spec.direction]) for spec in sort]
        values = list(cursor.keys)
        if all(column != _ID for column, _ in steps):
            steps.append((_ID, _STEP_OPS["ASC"]))
            values.append(cursor.tie_breaker)
        branches: list[MongoFilter] = []
        for index, ((column, step), value) in enumerate(zip(steps, values, strict=True)):
            equal_prefix = [{steps[j][0]: values[j]} for j in range(index)]
            branches.append({"$and": [*equal_prefix, {column: {step: value}}]})
        return {"$or": branches}

    def storage_value(self, field: str, value: object) -> object:
        """Return ``value`` as the document stores it in ``field``.

        The primary key takes the ``id_to_storage`` form; any other column
        takes :func:`~loom.core.repository.mongo.values.to_storage_value`.

        Args:
            field: Model field name.
            value: Model-level value.

        Returns:
            The value to compare against the stored document.
        """
        return self._converter(field)(value)

    def _converter(self, field: str) -> StorageConverter:
        return self._id_to_storage if field == self._id_field else to_storage_value

    def _compile_spec(self, spec: FilterSpec) -> MongoFilter:
        column = self._column(spec.field)
        if spec.op in _COMPARISON_OPS:
            self._require_ordered(spec.field)
        convert = self._converter(spec.field)
        if spec.op is FilterOp.NE:
            value = convert(spec.value)
            return {"$and": [{column: {"$ne": None}}, {column: {"$ne": value}}]}
        build = _VALUE_OPS.get(spec.op)
        if build is None:
            raise self._unsupported(
                f"operator '{spec.op.value}' is not supported by the {self.backend} backend"
            )
        try:
            return {column: build(spec.value, convert)}
        except _TooManyWildcards as exc:
            raise self._unsupported(str(exc)) from exc

    def _column(self, field: str) -> str:
        if field == self._id_field:
            return _ID
        if field not in self._fields:
            raise self._unsupported(f"unknown field '{field}'")
        return field

    def _require_ordered(self, field: str) -> None:
        """Refuse ordering on a column stored as a string with no natural order."""
        if field in self._unordered:
            raise self._unsupported(
                f"field '{field}' is a Decimal, stored as a string; the {self.backend} "
                "backend cannot order or range-compare it"
            )

    def _unsupported(self, reason: str) -> UnsupportedQuery:
        return UnsupportedQuery(reason, backend=self.backend, model=self._model_name)
