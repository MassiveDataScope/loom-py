"""MsgspecJSONResponse — zero-copy JSON response for FastAPI.

Uses ``msgspec.json.encode`` to serialise response content directly to bytes,
bypassing the stdlib ``json`` module and Pydantic's serialisation pipeline.
No double-serialisation occurs: the bytes are written to the response body
exactly once.

Supports any type that ``msgspec.json.encode`` accepts: plain Python types
(``dict``, ``list``, ``str``, ``int``, etc.), ``msgspec.Struct`` instances,
and ``dataclasses``. A type msgspec declines — a strict ``pydantic.BaseModel``
result, a ``list`` of them, or an envelope holding one — falls through to
``enc_hook``, which renders it through the loom type instead.

Usage::

    from loom.rest.fastapi.response import MsgspecJSONResponse

    @app.get("/items/{item_id}")
    async def get_item(item_id: int) -> MsgspecJSONResponse:
        item = await repo.get(item_id)
        return MsgspecJSONResponse(content=item)
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

import msgspec
from starlette.responses import Response

from loom.core.model import UnsupportedBoundaryType, loom_type_of
from loom.core.use_case.outcome import Created
from loom.rest.constants import LOCATION_HEADER


class MsgspecJSONResponse(Response):
    """FastAPI ``Response`` subclass that encodes content with ``msgspec.json``.

    Drop-in replacement for ``fastapi.responses.JSONResponse`` with two
    advantages:

    - Native ``msgspec.Struct`` serialisation — no ``dict`` conversion needed.
    - Single encoding pass — content is written directly to bytes with no
      intermediate JSON string or Pydantic round-trip.

    Args:
        content: Any object supported by ``msgspec.json.encode``.
        status_code: HTTP status code.  Defaults to ``200``.
        headers: Additional response headers.
        media_type: Defaults to ``"application/json"``.
        background: Optional Starlette background task.

    Example::

        return MsgspecJSONResponse(content=my_struct, status_code=201)
    """

    media_type = "application/json"

    def render(self, content: object) -> bytes:
        """Encode ``content`` to JSON bytes using ``msgspec.json.encode``.

        Args:
            content: Object to serialise.

        Returns:
            UTF-8 encoded JSON bytes.

        Raises:
            TypeError: ``content``, or a value nested inside it, is a type
                neither msgspec nor pydantic can render.
            pydantic_core.PydanticSerializationError: A nested
                ``pydantic.BaseModel`` fails its own serialisation; pydantic
                raises it directly and it propagates unchanged.
        """
        return msgspec.json.encode(content, enc_hook=_to_builtins)


def _to_builtins(obj: Any) -> Any:
    """Render a type ``msgspec.json.encode`` cannot handle on its own.

    Runs only for a type msgspec declines — a strict ``pydantic.BaseModel``
    nested inside the response, most often — so a Struct, a builtin or
    ``None`` keeps paying for a single encode pass.

    Raises:
        TypeError: ``obj``'s type is not a boundary type either library
            knows how to render.
    """
    try:
        return loom_type_of(type(obj)).to_builtins(obj)
    except UnsupportedBoundaryType as exc:
        raise TypeError(str(exc)) from exc


def created_response(created: Created[Any]) -> MsgspecJSONResponse:
    """Answer a :class:`~loom.core.use_case.Created` result: ``201``, its value, its location.

    ``Location`` is sent only when the use case gave one (RFC 9110 §15.3.2).
    """
    headers = None if created.location is None else {LOCATION_HEADER: created.location}
    return MsgspecJSONResponse(
        content=created.value, status_code=HTTPStatus.CREATED, headers=headers
    )
