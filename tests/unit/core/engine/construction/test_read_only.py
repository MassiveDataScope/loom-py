"""A writer refused under a read-only execution is refused before it is built."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.errors import UndeclaredStoreWrite
from loom.core.use_case.use_case import UseCase
from tests.unit.core.engine.construction import acme


async def test_a_writer_under_a_read_only_execution_is_refused_before_it_is_built() -> None:
    executor = acme.executor()
    factory = acme.factory()

    class _ReadOnlyOuter(UseCase[Any, None]):
        async def execute(self) -> None:
            await executor.run(acme.AcmeProbe, factory=factory)

    with pytest.raises(UndeclaredStoreWrite, match="the enclosing execution is read-only"):
        await executor.execute(_ReadOnlyOuter(), read_only=True, identity=acme.ALICE)

    assert acme.JOURNAL.entries == ["EXEC_START", "EXEC_ERROR"]
