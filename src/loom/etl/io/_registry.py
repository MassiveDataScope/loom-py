"""Dispatch registry that routes a target spec to the writer registered for its kind."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from loom.etl.declarative.target import TargetSpec
from loom.etl.ports.contracts import TargetWriter

if TYPE_CHECKING:
    from loom.etl.lineage._records import WriteContext


class WriterRegistry:
    """Dispatch a write to the writer registered for the spec's kind, or to the base writer.

    Args:
        base: Writer every spec whose kind is not in *extra* is written through.
        extra: Writers by ``spec.kind``.
    """

    def __init__(
        self,
        base: TargetWriter,
        *,
        extra: Mapping[str, TargetWriter] | None = None,
    ) -> None:
        self._base = base
        self._extra: Mapping[str, TargetWriter] = extra or {}

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        params: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: WriteContext | None = None,
    ) -> None:
        """Write *frame* into *spec* through its kind's writer, or through the base writer.

        Args:
            frame: Frame returned by the step's ``execute()``.
            spec: Compiled target specification.
            params: Concrete params for current run.
            streaming: Hint for lazy backends to use streaming materialization.
            write_ctx: Execution context for audit-column injection.
        """
        kind = getattr(spec, "kind", None)
        handler = self._extra.get(kind) if kind is not None else None
        writer = handler if handler is not None else self._base
        writer.write(frame, spec, params, streaming=streaming, write_ctx=write_ctx)
