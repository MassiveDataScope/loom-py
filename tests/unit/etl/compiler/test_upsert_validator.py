"""Unit tests for compiler._upsert_validator — direct function coverage."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoTable
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._validators import validate_upsert_spec


class _P(ETLParams):
    pass


def _spec(step_type: type[ETLStep[Any]]):  # type: ignore[return]
    """Extract TargetSpec from a step's IntoTable declaration."""
    return step_type.target._to_spec()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Non-UPSERT specs are always valid
# ---------------------------------------------------------------------------


class _AppendStep(ETLStep[_P]):
    target = IntoTable("t.out").append()

    def execute(self, params: _P) -> Any:
        return None


def test_non_upsert_spec_skipped() -> None:
    validate_upsert_spec(_AppendStep, _spec(_AppendStep))


# ---------------------------------------------------------------------------
# Valid UPSERT
# ---------------------------------------------------------------------------


class _ValidUpsert(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",))

    def execute(self, params: _P) -> Any:
        return None


def test_valid_upsert_passes() -> None:
    validate_upsert_spec(_ValidUpsert, _spec(_ValidUpsert))


class _UpsertWithExclude(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("created_at",))

    def execute(self, params: _P) -> Any:
        return None


def test_valid_upsert_with_exclude_passes() -> None:
    validate_upsert_spec(_UpsertWithExclude, _spec(_UpsertWithExclude))


class _UpsertWithInclude(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), include=("amount",))

    def execute(self, params: _P) -> Any:
        return None


def test_valid_upsert_with_include_passes() -> None:
    validate_upsert_spec(_UpsertWithInclude, _spec(_UpsertWithInclude))


# ---------------------------------------------------------------------------
# Error: empty keys
# ---------------------------------------------------------------------------


class _NoKeys(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=())

    def execute(self, params: _P) -> Any:
        return None


def test_empty_keys_raises() -> None:
    spec = _spec(_NoKeys)
    with pytest.raises(ETLCompilationError, match="at least one key"):
        validate_upsert_spec(_NoKeys, spec)


# ---------------------------------------------------------------------------
# Error: both exclude and include
# ---------------------------------------------------------------------------


class _BothExcludeInclude(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("x",), include=("y",))

    def execute(self, params: _P) -> Any:
        return None


def test_exclude_and_include_raises() -> None:
    spec = _spec(_BothExcludeInclude)
    with pytest.raises(ETLCompilationError, match="mutually exclusive"):
        validate_upsert_spec(_BothExcludeInclude, spec)


# ---------------------------------------------------------------------------
# Error: exclude overlaps keys
# ---------------------------------------------------------------------------


class _ExcludeOverlapsKey(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("id", "amount"))

    def execute(self, params: _P) -> Any:
        return None


def test_exclude_overlaps_key_raises() -> None:
    spec = _spec(_ExcludeOverlapsKey)
    with pytest.raises(ETLCompilationError, match="id"):
        validate_upsert_spec(_ExcludeOverlapsKey, spec)


# ---------------------------------------------------------------------------
# update() shares the merge checks and names itself in errors
# ---------------------------------------------------------------------------


class _ValidUpdate(ETLStep[_P]):
    target = IntoTable("t.out").update(keys=("id",))

    def execute(self, params: _P) -> Any:
        return None


def test_valid_update_passes() -> None:
    validate_upsert_spec(_ValidUpdate, _spec(_ValidUpdate))


class _UpdateNoKeys(ETLStep[_P]):
    target = IntoTable("t.out").update(keys=())

    def execute(self, params: _P) -> Any:
        return None


def test_update_empty_keys_raises_naming_update() -> None:
    spec = _spec(_UpdateNoKeys)
    with pytest.raises(
        ETLCompilationError, match=r"update\(\) requires at least one key"
    ) as exc_info:
        validate_upsert_spec(_UpdateNoKeys, spec)

    assert exc_info.value.code is ETLErrorCode.UPSERT_NO_KEYS


class _UpdateBothExcludeInclude(ETLStep[_P]):
    target = IntoTable("t.out").update(keys=("id",), exclude=("x",), include=("y",))

    def execute(self, params: _P) -> Any:
        return None


def test_update_exclude_and_include_raises_naming_update() -> None:
    spec = _spec(_UpdateBothExcludeInclude)
    with pytest.raises(ETLCompilationError, match=r"update\(\) exclude= and include=") as exc_info:
        validate_upsert_spec(_UpdateBothExcludeInclude, spec)

    assert exc_info.value.code is ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT


class _UpdateIncludeAbsorbed(ETLStep[_P]):
    target = IntoTable("t.out").update(
        keys=("id",), partition_cols=("year",), include=("id", "year")
    )

    def execute(self, params: _P) -> Any:
        return None


def test_update_include_absorbed_by_keys_and_partitions_raises() -> None:
    spec = _spec(_UpdateIncludeAbsorbed)
    with pytest.raises(ETLCompilationError, match="guaranteed no-op") as exc_info:
        validate_upsert_spec(_UpdateIncludeAbsorbed, spec)

    assert exc_info.value.code is ETLErrorCode.UPDATE_EMPTY_UPDATE_SET


class _UpdateIncludeWithRealColumn(ETLStep[_P]):
    target = IntoTable("t.out").update(keys=("id",), include=("id", "status"))

    def execute(self, params: _P) -> Any:
        return None


def test_update_include_with_one_real_column_passes() -> None:
    validate_upsert_spec(_UpdateIncludeWithRealColumn, _spec(_UpdateIncludeWithRealColumn))


class _UpsertIncludeAbsorbed(ETLStep[_P]):
    # upsert still inserts unmatched rows, so an absorbed include is not a no-op.
    target = IntoTable("t.out").upsert(keys=("id",), include=("id",))

    def execute(self, params: _P) -> Any:
        return None


def test_upsert_include_absorbed_is_not_flagged() -> None:
    validate_upsert_spec(_UpsertIncludeAbsorbed, _spec(_UpsertIncludeAbsorbed))
