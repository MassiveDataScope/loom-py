"""Tool return values, their result bounds, and the one metadata key a tool may not write."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Final

import msgspec
from pydantic_ai import ToolReturn

from loom.ai.compiler import CompiledSqlCapability
from loom.core.sql.abc import SqlQueryResult

RESERVED_METADATA_KEY: Final[str] = "loom"
"""Reserved metadata key under which a loom toolset publishes the facts of a return."""


class ReturnShape(StrEnum):
    """The shape of a return a loom toolset publishes under :data:`RESERVED_METADATA_KEY`."""

    OK = "ok"
    ROWS = "rows"
    REFUSED = "refused"


def ok_return(value: object) -> ToolReturn:
    """Return ``value`` with the structured facts the summary is built from."""
    return ToolReturn(
        return_value=value, metadata={RESERVED_METADATA_KEY: {"shape": ReturnShape.OK}}
    )


def _rows_return(payload: str, rows: int) -> ToolReturn:
    """Return an encoded result set, described by its row count."""
    return ToolReturn(
        return_value=payload,
        metadata={RESERVED_METADATA_KEY: {"shape": ReturnShape.ROWS, "n": rows}},
    )


def foreign_return(value: object) -> ToolReturn:
    """Present the result of a toolset loom did not author.

    Args:
        value: What the foreign tool returned.

    Returns:
        A ``ToolReturn`` kept with the reserved ``loom`` metadata key
        stripped, or *value* in the ``ok`` shape.
    """
    if isinstance(value, ToolReturn):
        return _without_loom_metadata(value)
    return ok_return(value)


def _without_loom_metadata(value: ToolReturn) -> ToolReturn:
    """Strip the reserved ``loom`` metadata key from a foreign tool return."""
    metadata = value.metadata
    if not isinstance(metadata, Mapping) or RESERVED_METADATA_KEY not in metadata:
        return value
    return ToolReturn(
        return_value=value.return_value,
        content=value.content,
        metadata={key: item for key, item in metadata.items() if key != RESERVED_METADATA_KEY},
        tools=value.tools,
    )


def refusal(reason: str) -> ToolReturn:
    """Refuse by value, in the ``refused`` shape: the model sees *reason*, never a row."""
    return ToolReturn(
        return_value=f"{ReturnShape.REFUSED}: {reason}",
        metadata={RESERVED_METADATA_KEY: {"shape": ReturnShape.REFUSED}},
    )


def bounded_return(capability: CompiledSqlCapability, result: SqlQueryResult) -> ToolReturn:
    """Encode *result* for the model, or refuse it above ``max_rows`` or ``max_result_bytes``."""
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
