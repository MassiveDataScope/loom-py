"""ETL runtime contracts for catalog discovery and I/O operations.

These protocols define the integration surface between the ETL framework and
backend implementations (Spark, Polars, stubs).

Dependency direction
--------------------
``contracts.py`` depends only on ETL domain types (``TableRef``, ``SourceSpec``,
``TargetSpec``). It does not import compiler or runner modules.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import SourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.schema._schema import ColumnSchema

if TYPE_CHECKING:
    from loom.etl.lineage._records import WriteContext


@runtime_checkable
class TableDiscovery(Protocol):
    """Protocol for catalog-backed table existence and schema discovery.

    Used by :class:`~loom.etl.compiler.ETLCompiler` when a catalog is injected
    at construction time to validate that source and target tables are valid
    before execution begins.
    """

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` when the table exists in the catalog.

        Args:
            ref: Logical table reference to check.

        Returns:
            ``True`` when the table is registered.
        """
        ...

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return table column names in schema order.

        Args:
            ref: Logical table reference.

        Returns:
            Tuple of column names, or ``()`` when unknown.
        """
        ...

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return full table schema.

        Args:
            ref: Logical table reference.

        Returns:
            Ordered column schema tuple, or ``None`` when table is missing.
        """
        ...

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """Persist table schema after a successful write.

        Args:
            ref: Logical table reference.
            schema: Authoritative table schema.
        """
        ...


@runtime_checkable
class SourceReader(Protocol):
    """Protocol for reading one ETL source specification into a frame."""

    def read(self, spec: SourceSpec, params_instance: Any, /) -> Any:
        """Read the source and return a frame.

        Args:
            spec: Compiled source specification.
            params_instance: Concrete params for current run.

        Returns:
            Backend frame type accepted by the step ``execute()`` method.
        """
        ...


@runtime_checkable
class StreamingSourceReader(Protocol):
    """Optional capability protocol for memory-bounded streaming reads.

    Implementations must return a backend frame (typically a
    :class:`polars.LazyFrame`) whose terminal ``collect(engine="streaming")``
    consumes the source incrementally — without materializing the full result
    set in memory. Typical implementations spool a server-side cursor (e.g.
    ClickHouse ``query_arrow_stream``) into a temporary IPC file and return a
    ``pl.scan_ipc(...)`` over it, capping peak RAM at one batch.

    This capability is opt-in: readers expose it only when they can guarantee
    bounded memory. ``ETLExecutor`` checks for this protocol via
    ``isinstance(reader, StreamingSourceReader)`` and raises a clear
    ``TypeError`` when a step requests ``streaming=True`` against a reader
    that lacks the capability, to prevent silent OOM regressions.

    Example:
        >>> reader: StreamingSourceReader = ClickHouseSourceReader(url=...)
        >>> lazy = reader.read_streaming(spec, params)
        >>> df = lazy.collect(engine="streaming")
    """

    def read_streaming(self, spec: SourceSpec, params_instance: Any, /) -> Any:
        """Read the source using a memory-bounded streaming strategy.

        Args:
            spec: Compiled source specification.
            params_instance: Concrete params for current run.

        Returns:
            Backend frame type (typically ``pl.LazyFrame``) whose downstream
            ``collect(engine="streaming")`` consumes the source incrementally.

        Raises:
            TypeError: When the underlying client cannot stream this spec.
        """
        ...


@runtime_checkable
class SQLExecutor(Protocol):
    """Optional capability protocol for SQL execution over source frames."""

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> Any:
        """Execute a SQL query against backend frames.

        Used by ``StepSQL`` execution path in :class:`~loom.etl.executor.ETLExecutor`.

        Args:
            frames: Alias -> frame mapping from resolved step sources.
            query: SQL query text.

        Returns:
            Backend frame type with query results.
        """
        ...


@runtime_checkable
class TargetWriter(Protocol):
    """Protocol for writing one frame into one ETL target specification."""

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: WriteContext | None = None,
    ) -> None:
        """Write frame to target.

        Args:
            frame: Frame returned by step ``execute()``.
            spec: Compiled target specification.
            params_instance: Concrete params for current run.
            streaming: Hint for lazy backends to use streaming materialization.
            write_ctx: Execution context for audit-column injection.
                       ``None`` disables audit columns regardless of config.
        """
        ...


@runtime_checkable
class ClientCommandExecutor(Protocol):
    """Optional capability protocol for engine-client command execution.

    Implemented by engine-specific executors (e.g. ClickHouseClientExecutor)
    and injected into :class:`~loom.etl.executor.ETLExecutor` as a separate
    dependency.  Used exclusively to serve :class:`~loom.etl.ClientStep`
    steps, which produce no DataFrame output and instead receive the raw
    engine client via a ``client`` keyword argument.

    The executor calls :meth:`command` with a callable that encapsulates
    the step invocation.  The implementor is responsible for obtaining the
    engine client and calling ``fn(client)``.

    Example::

        class ClickHouseClientExecutor:
            def command(self, fn: Callable[[Any], None]) -> None:
                fn(self._get_clickhouse_client())
    """

    def command(self, fn: Callable[[Any], None]) -> None:
        """Obtain the engine client and invoke *fn* with it.

        Args:
            fn: Callable that accepts the raw engine client as its sole
                positional argument and executes the step logic against it.
                Produced by the executor as a closure over the step instance
                and the current params.

        Raises:
            RuntimeError: When the underlying client cannot be obtained.
        """
        ...


class AgentIssueKind(StrEnum):
    """Why a declared agent cannot serve a step."""

    NOT_FOUND = "not_found"
    COMPILATION_FAILED = "compilation_failed"
    OUTPUT_MISMATCH = "output_mismatch"
    UNPRICED_BUDGET = "unpriced_budget"


@dataclass(frozen=True)
class AgentIssue:
    """One reason a declared agent cannot serve a step.

    Attributes:
        kind: What is wrong.
        message: Human-readable detail, naming the agent and never a secret.
    """

    kind: AgentIssueKind
    message: str


@runtime_checkable
class AgentBatchRunner(Protocol):
    """Optional capability protocol for running a declared agent over a frame.

    Implemented by :mod:`loom.ai.etl` and injected into
    :class:`~loom.etl.executor.ETLExecutor` and
    :class:`~loom.etl.compiler.ETLCompiler` as a separate dependency, so the
    ETL pillar depends on this contract and never on the AI pillar. Used
    exclusively to serve :class:`~loom.etl.WithAgent` declarations.
    """

    def validate(
        self, name: str, output_type: type[Any], *, max_usd: Decimal | None
    ) -> tuple[AgentIssue, ...]:
        """Check, without spending anything, that *name* can serve a step.

        Args:
            name: Agent name declared by the step.
            output_type: Type the step expects every answer to decode into.
            max_usd: Budget of one execution of the step, when declared.

        Returns:
            Every issue found; empty when the agent can serve the step.
        """
        ...

    def version(self, name: str) -> str:
        """Return the version of agent *name*, stable while its behaviour is.

        Args:
            name: Agent name declared by the step.

        Returns:
            The value written in the ``agent_version`` column.
        """
        ...

    def map(
        self,
        name: str,
        frame: Any,
        *,
        keys: Sequence[str],
        prompt: object,
        output_type: type[Any],
        max_usd: Decimal | None,
    ) -> Any:
        """Run agent *name* once per row of *frame*.

        Args:
            name: Agent name declared by the step.
            frame: Backend frame holding the rows to answer.
            keys: Columns identifying a row, copied to the output.
            prompt: Backend expression, or column name, giving each row's prompt.
            output_type: Type every answer is decoded into.
            max_usd: Budget of this call; ``None`` spends without a ceiling.

        Returns:
            One output row per input row, failures included.
        """
        ...


__all__ = [
    "AgentBatchRunner",
    "AgentIssue",
    "AgentIssueKind",
    "TableDiscovery",
    "SourceReader",
    "StreamingSourceReader",
    "SQLExecutor",
    "TargetWriter",
    "ClientCommandExecutor",
]
