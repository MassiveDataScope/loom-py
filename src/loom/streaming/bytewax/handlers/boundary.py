"""Bytewax handler family for boundary nodes."""

from __future__ import annotations

from typing import Any

from loom.streaming.bytewax.handlers._shared import _BuildContextProtocol

Stream = Any


def _apply_into_topic(stream: Stream, _raw: object, idx: int, ctx: _BuildContextProtocol) -> None:
    ctx.wire_branch_terminal(f"into_topic_{idx}", stream, ctx.current_path)
