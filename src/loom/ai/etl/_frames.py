"""Polars side of a batch: prompts out of the input frame, answers into the output frame."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

import polars as pl

from loom.ai.etl._batch import RowOutcome
from loom.ai.etl._columns import Column

STATUS_OK: Final = "ok"
STATUS_ERROR: Final = "error"

_PROMPT: Final = "__loom_agent_prompt__"

_METADATA_SCHEMA: Final = pl.Schema(
    {
        "agent_version": pl.String,
        "agent_status": pl.String,
        "agent_error": pl.String,
        "agent_input_tokens": pl.Int64,
        "agent_output_tokens": pl.Int64,
        "agent_cache_read_tokens": pl.Int64,
        "agent_cache_write_tokens": pl.Int64,
        "agent_cost_usd": pl.Float64,
    }
)


def prompt_rows(
    frame: object, keys: Sequence[str], prompt: object
) -> tuple[pl.DataFrame, list[str | None]]:
    """Collect the key columns and the prompt of every row of *frame*.

    Args:
        frame: Polars ``DataFrame`` or ``LazyFrame`` holding the rows.
        keys: Columns identifying a row.
        prompt: Polars expression, or column name, giving each row's prompt.

    Returns:
        The key columns, and one prompt per row, ``None`` where it is null.

    Raises:
        TypeError: When *frame* is not a Polars frame or *prompt* is neither
            an expression nor a column name.
    """
    rows = _lazy(frame).select(*keys, _expression(prompt).cast(pl.String).alias(_PROMPT)).collect()
    return rows.select(keys), rows[_PROMPT].to_list()


def answers_frame(
    keys: pl.DataFrame,
    outcomes: Sequence[RowOutcome],
    columns: Mapping[str, Column],
    version: str,
) -> pl.DataFrame:
    """Join the keys, the answer fields and the run metadata, one row per outcome.

    Every answer column takes the type its field declares, so an empty batch
    or a batch of errors only has the same schema as any other.

    Args:
        keys: Key columns of the input rows, in order.
        outcomes: One outcome per input row, in the same order.
        columns: Columns of the declared output's fields.
        version: Agent version written on every row.

    Returns:
        The output frame.
    """
    answers = pl.DataFrame(
        {
            name: [column.cell(_field(o.output, name)) for o in outcomes]
            for name, column in columns.items()
        },
        schema=pl.Schema({name: column.dtype for name, column in columns.items()}),
    )
    metadata = pl.DataFrame(
        [_metadata(o, version) for o in outcomes], schema=_METADATA_SCHEMA, orient="row"
    )
    return pl.concat([keys, answers, metadata], how="horizontal")


def _lazy(frame: object) -> pl.LazyFrame:
    if isinstance(frame, pl.LazyFrame):
        return frame
    if isinstance(frame, pl.DataFrame):
        return frame.lazy()
    raise TypeError(f"an agent maps a Polars frame, got {type(frame).__name__}")


def _expression(prompt: object) -> pl.Expr:
    if isinstance(prompt, str):
        return pl.col(prompt)
    if isinstance(prompt, pl.Expr):
        return prompt
    raise TypeError(f"an agent prompt is a Polars expression or a column name, got {prompt!r}")


def _field(output: Mapping[str, Any] | None, field: str) -> Any:
    return None if output is None else output.get(field)


def _metadata(outcome: RowOutcome, version: str) -> tuple[object, ...]:
    usage = outcome.usage
    return (
        version,
        STATUS_OK if outcome.error is None else STATUS_ERROR,
        outcome.error,
        0 if usage is None else usage.input_tokens,
        0 if usage is None else usage.output_tokens,
        0 if usage is None else usage.cache_read_tokens,
        0 if usage is None else usage.cache_write_tokens,
        None if usage is None or usage.cost is None else float(usage.cost),
    )
