"""Unit tests for compiler._upsert_validator — direct function coverage."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl import ETLStep, IntoTable
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._validators import validate_upsert_spec
from tests.unit.etl.engines.doubles import NoParams


def _spec(step_type: type[ETLStep[Any]]):  # type: ignore[return]
    """Extract TargetSpec from a step's IntoTable declaration."""
    return step_type.target._to_spec()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Non-UPSERT specs are always valid
# ---------------------------------------------------------------------------


class _AppendStep(ETLStep[NoParams]):
    target = IntoTable("t.out").append()

    def execute(self, params: NoParams) -> Any:
        return None


class TestNonUpsertSpecSkipped:
    def test_non_upsert_spec_skipped(self) -> None:
        validate_upsert_spec(_AppendStep, _spec(_AppendStep))


# ---------------------------------------------------------------------------
# Valid UPSERT
# ---------------------------------------------------------------------------


class IdValidUpsert(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",))

    def execute(self, params: NoParams) -> Any:
        return None


class TestValidUpsert:
    def test_valid_upsert_passes(self) -> None:
        validate_upsert_spec(IdValidUpsert, _spec(IdValidUpsert))


class IdUpsertExcludingCreatedAt(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("created_at",))

    def execute(self, params: NoParams) -> Any:
        return None


class TestValidUpsertWithExclude:
    def test_valid_upsert_with_exclude_passes(self) -> None:
        validate_upsert_spec(IdUpsertExcludingCreatedAt, _spec(IdUpsertExcludingCreatedAt))


class IdUpsertIncludingAmount(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), include=("amount",))

    def execute(self, params: NoParams) -> Any:
        return None


class TestValidUpsertWithInclude:
    def test_valid_upsert_with_include_passes(self) -> None:
        validate_upsert_spec(IdUpsertIncludingAmount, _spec(IdUpsertIncludingAmount))


# ---------------------------------------------------------------------------
# Error: empty keys
# ---------------------------------------------------------------------------


class _NoKeys(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=())

    def execute(self, params: NoParams) -> Any:
        return None


class TestEmptyUpsertKeys:
    def test_empty_keys_raises(self) -> None:
        spec = _spec(_NoKeys)
        with pytest.raises(ETLCompilationError, match="at least one key"):
            validate_upsert_spec(_NoKeys, spec)


# ---------------------------------------------------------------------------
# Error: both exclude and include
# ---------------------------------------------------------------------------


class _BothExcludeInclude(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("x",), include=("y",))

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpsertExcludeAndInclude:
    def test_exclude_and_include_raises(self) -> None:
        spec = _spec(_BothExcludeInclude)
        with pytest.raises(ETLCompilationError, match="mutually exclusive"):
            validate_upsert_spec(_BothExcludeInclude, spec)


# ---------------------------------------------------------------------------
# Error: exclude overlaps keys
# ---------------------------------------------------------------------------


class _ExcludeOverlapsKey(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("id", "amount"))

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpsertExcludeOverlapsKey:
    def test_exclude_overlaps_key_raises(self) -> None:
        spec = _spec(_ExcludeOverlapsKey)
        with pytest.raises(ETLCompilationError, match="id"):
            validate_upsert_spec(_ExcludeOverlapsKey, spec)


# ---------------------------------------------------------------------------
# update() shares the merge checks and names itself in errors
# ---------------------------------------------------------------------------


class _ValidUpdate(ETLStep[NoParams]):
    target = IntoTable("t.out").update(keys=("id",))

    def execute(self, params: NoParams) -> Any:
        return None


class TestValidUpdate:
    def test_valid_update_passes(self) -> None:
        validate_upsert_spec(_ValidUpdate, _spec(_ValidUpdate))


class _UpdateNoKeys(ETLStep[NoParams]):
    target = IntoTable("t.out").update(keys=())

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpdateEmptyKeys:
    def test_update_empty_keys_raises_naming_update(self) -> None:
        spec = _spec(_UpdateNoKeys)
        with pytest.raises(
            ETLCompilationError, match=r"update\(\) requires at least one key"
        ) as exc_info:
            validate_upsert_spec(_UpdateNoKeys, spec)

        assert exc_info.value.code is ETLErrorCode.UPSERT_NO_KEYS


class _UpdateBothExcludeInclude(ETLStep[NoParams]):
    target = IntoTable("t.out").update(keys=("id",), exclude=("x",), include=("y",))

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpdateExcludeAndInclude:
    def test_update_exclude_and_include_raises_naming_update(self) -> None:
        spec = _spec(_UpdateBothExcludeInclude)
        with pytest.raises(
            ETLCompilationError, match=r"update\(\) exclude= and include="
        ) as exc_info:
            validate_upsert_spec(_UpdateBothExcludeInclude, spec)

        assert exc_info.value.code is ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT


class _UpdateIncludeAbsorbed(ETLStep[NoParams]):
    target = IntoTable("t.out").update(
        keys=("id",), partition_cols=("year",), include=("id", "year")
    )

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpdateIncludeAbsorbed:
    def test_update_include_absorbed_by_keys_and_partitions_raises(self) -> None:
        spec = _spec(_UpdateIncludeAbsorbed)
        with pytest.raises(ETLCompilationError, match="guaranteed no-op") as exc_info:
            validate_upsert_spec(_UpdateIncludeAbsorbed, spec)

        assert exc_info.value.code is ETLErrorCode.UPDATE_EMPTY_UPDATE_SET


class _UpdateIncludeWithRealColumn(ETLStep[NoParams]):
    target = IntoTable("t.out").update(keys=("id",), include=("id", "status"))

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpdateIncludeWithRealColumn:
    def test_update_include_with_one_real_column_passes(self) -> None:
        validate_upsert_spec(_UpdateIncludeWithRealColumn, _spec(_UpdateIncludeWithRealColumn))


class _UpsertIncludeAbsorbed(ETLStep[NoParams]):
    # upsert still inserts unmatched rows, so an absorbed include is not a no-op.
    target = IntoTable("t.out").upsert(keys=("id",), include=("id",))

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpsertIncludeAbsorbed:
    def test_upsert_include_absorbed_is_not_flagged(self) -> None:
        validate_upsert_spec(_UpsertIncludeAbsorbed, _spec(_UpsertIncludeAbsorbed))


class _UpsertWithDeleteFlag(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), delete_flag="_deleted")

    def execute(self, params: NoParams) -> Any:
        return None


class TestUpsertDeleteFlag:
    def test_upsert_delete_flag_passes(self) -> None:
        validate_upsert_spec(_UpsertWithDeleteFlag, _spec(_UpsertWithDeleteFlag))


class _DeleteFlagIsKey(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), delete_flag="id")

    def execute(self, params: NoParams) -> Any:
        return None


class TestDeleteFlagEqualToKey:
    def test_upsert_delete_flag_equal_to_a_key_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="conflicts with keys") as exc_info:
            validate_upsert_spec(_DeleteFlagIsKey, _spec(_DeleteFlagIsKey))

        assert exc_info.value.code is ETLErrorCode.UPSERT_DELETE_FLAG_CONFLICT


class _DeleteFlagInExclude(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("_deleted",), delete_flag="_deleted")

    def execute(self, params: NoParams) -> Any:
        return None


class TestDeleteFlagInExclude:
    def test_upsert_delete_flag_in_exclude_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="conflicts with exclude") as exc_info:
            validate_upsert_spec(_DeleteFlagInExclude, _spec(_DeleteFlagInExclude))

        assert exc_info.value.code is ETLErrorCode.UPSERT_DELETE_FLAG_CONFLICT


class _DeleteFlagInInclude(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(keys=("id",), include=("_deleted",), delete_flag="_deleted")

    def execute(self, params: NoParams) -> Any:
        return None


class TestDeleteFlagInInclude:
    def test_upsert_delete_flag_in_include_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="conflicts with include") as exc_info:
            validate_upsert_spec(_DeleteFlagInInclude, _spec(_DeleteFlagInInclude))

        assert exc_info.value.code is ETLErrorCode.UPSERT_DELETE_FLAG_CONFLICT


class _DeleteFlagInPartition(ETLStep[NoParams]):
    target = IntoTable("t.out").upsert(
        keys=("id",), partition_cols=("_deleted",), delete_flag="_deleted"
    )

    def execute(self, params: NoParams) -> Any:
        return None


class TestDeleteFlagInPartition:
    def test_upsert_delete_flag_in_partition_cols_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="conflicts with partition_cols") as exc_info:
            validate_upsert_spec(_DeleteFlagInPartition, _spec(_DeleteFlagInPartition))

        assert exc_info.value.code is ETLErrorCode.UPSERT_DELETE_FLAG_CONFLICT
