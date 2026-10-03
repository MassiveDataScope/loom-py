from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.exc import DBAPIError

from loom.core.backend.scoped_ddl import APP_FIRST, GUARD_FIRST, OPEN_HATCH, PROTECT
from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.migrations.operations import (
    GUARD_OPTION,
    EnsureRangePartitionsOp,
    OpenHatchOp,
    ProtectScopedTableOp,
    run_guard_operation,
)


class _Bind:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def execute(self, clause: Any, parameters: Any = None) -> None:
        self.calls.append((str(clause), parameters))


class _Context:
    def __init__(self, opts: dict[str, Any]) -> None:
        self.opts = opts


class _Operations:
    def __init__(self, opts: dict[str, Any]) -> None:
        self.bind = _Bind()
        self.context = _Context(opts)

    def get_bind(self) -> _Bind:
        return self.bind

    def get_context(self) -> _Context:
        return self.context


def _run(operation: Any, opts: dict[str, Any]) -> _Bind:
    operations = _Operations(opts)
    run_guard_operation(operations, operation)
    return operations.bind


GUARDED = {"version_table_schema": "notes", GUARD_OPTION: "loom_guard_notes"}


def test_open_hatch_resolves_the_guard_first_then_restores_the_application_path() -> None:
    bind = _run(OpenHatchOp(), GUARDED)
    path = {"schema": "notes", "guard": "loom_guard_notes"}
    assert bind.calls == [(GUARD_FIRST, path), (OPEN_HATCH, None), (APP_FIRST, path)]


def test_protect_resolves_the_guard_first() -> None:
    bind = _run(ProtectScopedTableOp("notes", [{"col": "owner_id"}], ["SELECT"]), GUARDED)
    assert [call[0] for call in bind.calls] == [GUARD_FIRST, PROTECT, APP_FIRST]


def test_a_guard_operation_without_the_guard_option_is_refused() -> None:
    operation = OpenHatchOp()
    with pytest.raises(ConfigError, match="run_migrations"):
        _run(operation, {"version_table_schema": "notes"})


class _EmptyCatalogOperations(_Operations):
    def __init__(self, opts: dict[str, Any]) -> None:
        super().__init__(opts)
        self.bind = _EmptyCatalogBind()


class _EmptyCatalogBind(_Bind):
    def execute(self, clause: Any, parameters: Any = None) -> list[Any]:
        super().execute(clause, parameters)
        return []


def test_a_partition_operation_checks_the_guard_revision_before_calling_the_guard() -> None:
    operations = _EmptyCatalogOperations(GUARDED)
    operation = EnsureRangePartitionsOp("note_events", "2026-01-01", "2026-02-01")

    with pytest.raises(ConfigError, match="not one this release"):
        run_guard_operation(operations, operation)

    assert GUARD_FIRST not in [call[0] for call in operations.bind.calls]


def test_a_python_error_inside_the_call_restores_the_application_path() -> None:
    operations = _Operations(GUARDED)

    operation = ProtectScopedTableOp("Bad Name", [], ["SELECT"])
    with pytest.raises(ValueError, match="not a usable SQL identifier"):
        run_guard_operation(operations, operation)

    path = {"schema": "notes", "guard": "loom_guard_notes"}
    assert operations.bind.calls == [(GUARD_FIRST, path), (APP_FIRST, path)]


class _FailingBind(_Bind):
    def execute(self, clause: Any, parameters: Any = None) -> None:
        super().execute(clause, parameters)
        if str(clause) == PROTECT:
            raise DBAPIError(PROTECT, parameters, Exception("refused"))


def test_a_database_error_leaves_the_path_to_the_rollback_of_the_aborted_transaction() -> None:
    operations = _Operations(GUARDED)
    operations.bind = _FailingBind()

    operation = ProtectScopedTableOp("notes", [], ["SELECT"])
    with pytest.raises(DBAPIError):
        run_guard_operation(operations, operation)

    assert [call[0] for call in operations.bind.calls] == [GUARD_FIRST, PROTECT]
