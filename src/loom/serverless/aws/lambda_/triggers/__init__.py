"""What invokes a Lambda function, one module per ``source``.

``http`` and ``sqs`` serve their requests and batches. ``eventbridge``,
``s3``, ``sns`` and ``schedule`` decode an event, parsed, into the payloads
of the jobs it runs: never the job, nor the identity, which the function's
configuration declares. The source decides the decoder; nothing is sniffed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from loom.core.errors import MalformedJobMessage
from loom.core.model import BoundaryValidationError, loom_type_of

_JSON = loom_type_of(Any)


@dataclass(frozen=True)
class Decoded:
    """The payload of one job an event runs, the event's id and the trace it continues."""

    payload: dict[str, Any]
    event_id: str | None = None
    trace_id: str | None = None


Decoder = Callable[[Any], tuple[Decoded, ...]]
"""Reads a parsed event into the jobs it runs, in order."""


def an_object(value: Any, what: str) -> dict[str, Any]:
    """*value*, which must be a JSON object.

    Raises:
        MalformedJobMessage: When it is not one.
    """
    if not isinstance(value, dict):
        raise MalformedJobMessage(f"{what} is not a JSON object")
    return value


def a_list(value: Any, what: str) -> list[Any]:
    """*value*, which must be a JSON array.

    Raises:
        MalformedJobMessage: When it is not one.
    """
    if not isinstance(value, list):
        raise MalformedJobMessage(f"{what} is not a JSON array")
    return value


def parsed(text: Any, what: str) -> Any:
    """The JSON document *text* holds.

    Raises:
        MalformedJobMessage: When *text* is not a string holding JSON.
    """
    if not isinstance(text, str):
        raise MalformedJobMessage(f"{what} is not a string")
    try:
        return _JSON.decode_json(text)
    except BoundaryValidationError:
        # From None: the decode error quotes the document.
        raise MalformedJobMessage(f"{what} is not JSON") from None


def an_id(value: Any) -> str | None:
    """*value* when it is a non-empty string, the id of an upstream event."""
    return value if isinstance(value, str) and value else None


def upstream_trace_id(document: Any) -> str | None:
    """The loom trace the upstream *document* continues; ``None`` starts a new one.

    The one place a source's trace would be read. No AWS source carries a
    loom trace id, so every event job starts its own trace.
    """
    _ = document
    return None
