from __future__ import annotations

from typing import Any

import pytest

from loom.core.backend.scoped_ddl import APP_FIRST, GUARD_FIRST, OPEN_HATCH, PROTECT
from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.migrations.operations import (
    GUARD_OPTION,
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
