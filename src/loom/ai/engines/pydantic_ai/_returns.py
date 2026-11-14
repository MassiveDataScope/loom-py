"""Tool return values, and the one metadata key a tool may not write.

One of the four invariants of a capability call holds here (US5):

* **Result bounds are applied before the model's context.** ``max_rows`` and
  ``max_result_bytes`` are checked on the way back; a tripped bound returns a
  refusal that names the bound and carries no row data. It returns
  rather than raises, so the model can narrow its own query.

The reserved ``loom`` metadata key is what
:mod:`~loom.ai.engines.pydantic_ai._events` builds an event summary from, so
it is stripped from every return loom did not author.
"""

from __future__ import annotations

from collections.abc import Mapping

import msgspec
from pydantic_ai import ToolReturn

from loom.ai.compiler import CompiledSqlCapability
from loom.core.engine.compilable import Compilable
from loom.core.sql.abc import SqlQueryResult


def ok_return(value: object) -> ToolReturn:
    """Return ``value`` with the structured facts the summary is built from."""
    return ToolReturn(return_value=value, metadata={"loom": {"shape": "ok"}})


def _rows_return(payload: str, rows: int) -> ToolReturn:
    """Return an encoded result set, described by its row count."""
    return ToolReturn(return_value=payload, metadata={"loom": {"shape": "rows", "n": rows}})


def foreign_return(value: object) -> ToolReturn:
    """Present the result of a toolset loom did not author.

    A foreign toolset may already speak the engine's own return type. Its value
    and its own metadata are kept, but the reserved ``loom`` key is stripped:
    that key is what :mod:`~loom.ai.engines.pydantic_ai._events` builds the
    event summary from, so leaving it writable would let an MCP server or a
    third-party toolset dictate the summary of its own call. Anything else is
    described with the
    neutral ``ok`` shape.
    """
    if isinstance(value, ToolReturn):
        return _without_loom_metadata(value)
    return ok_return(value)


def _without_loom_metadata(value: ToolReturn) -> ToolReturn:
    """Strip the reserved ``loom`` metadata key from a foreign tool return."""
    metadata = value.metadata
    if not isinstance(metadata, Mapping) or "loom" not in metadata:
        return value
    return ToolReturn(
        return_value=value.return_value,
        content=value.content,
        metadata={key: item for key, item in metadata.items() if key != "loom"},
        tools=value.tools,
    )


def refusal(reason: str) -> ToolReturn:
    """Refuse by value: the model sees the bound, never a row.

    The ``refused`` shape is what keeps a refusal from reading as a normal call
    in the event stream: it is the event an operator most needs to see.
    """
    return ToolReturn(return_value=f"refused: {reason}", metadata={"loom": {"shape": "refused"}})


def summary_of(use_case: type[Compilable]) -> str:
    doc = use_case.__doc__
    return doc.strip().splitlines()[0] if doc and doc.strip() else ""


def bounded_return(capability: CompiledSqlCapability, result: SqlQueryResult) -> ToolReturn:
    """Apply both result bounds before a single row can enter the model's context.

    The row bound counts ``rows`` — the data actually handed over — rather than
    the sibling ``row_count`` an executor could compute differently.
    """
    rows = len(result.rows)
    if rows > capability.max_rows:
        return refusal(
            f"the result has {rows} rows, above the max_rows bound of "
            f"{capability.max_rows}; narrow the query"
        )
    payload = msgspec.json.encode(
        {"columns": [column.name for column in result.columns], "rows": result.rows}
    )
    if len(payload) > capability.max_result_bytes:
        return refusal(
            f"the result is {len(payload)} bytes, above the max_result_bytes bound of "
            f"{capability.max_result_bytes}; select fewer columns or fewer rows"
        )
    return _rows_return(payload.decode(), rows)
