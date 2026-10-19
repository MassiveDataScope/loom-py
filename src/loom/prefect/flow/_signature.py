"""Build a Prefect flow's parameter signature from a ``msgspec.Struct``.

Prefect 3 displays a typed parameter form in the UI for each flow run.
For loom ETLs the parameter set is described by the pipeline's
``ParamsT`` ``msgspec.Struct``, so the factory synthesises an
``inspect.Signature`` that mirrors its fields.

This module also handles the datetime coercion needed when Prefect (or a
CLI/UI user) submits a parameter as an ISO string that has to be compared
against UTC-aware Polars columns inside the runner.
"""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from types import UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

import msgspec


def signature_from_params_type(
    params_type: type[msgspec.Struct],
) -> list[inspect.Parameter]:
    """Return one ``inspect.Parameter`` per field of *params_type*.

    All fields are emitted as keyword-only without defaults so Prefect's
    form forces the operator to fill them (or accept the defaults that
    the YAML pre-binds at deploy time).

    Args:
        params_type: ``msgspec.Struct`` describing the ETL's parameters.

    Returns:
        Ordered list of parameters matching the struct's field order.
    """
    hints = get_type_hints(params_type)
    return [
        inspect.Parameter(
            name=field.name,
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=inspect.Parameter.empty,
            annotation=hints.get(field.name, Any),
        )
        for field in msgspec.structs.fields(params_type)
    ]


def synthesise_flow_signature(
    params_type: type[msgspec.Struct],
    *,
    extra_parameters: Sequence[inspect.Parameter] = (),
) -> inspect.Signature:
    """Build the flow signature: user params + ``env`` + factory-specific extras.

    Every loom flow exposes ``env``, an
    :class:`~loom.core.environment.Environment` value: ``None`` by default,
    so the run resolves it like any loom process (``LOOM_ENV``, else
    production), and set by the deployment to the environment it was
    deployed for; each factory appends its own extras
    (``correlation_id``/``processes`` for ``etl_flow``, ``start_from`` for
    ``backfill_flow``, none for ``maintenance_flow``).

    Args:
        params_type: ``msgspec.Struct`` describing the ETL's parameters.
        extra_parameters: Keyword-only parameters appended after ``env``.

    Returns:
        The synthesised ``inspect.Signature``.
    """
    env = inspect.Parameter(
        "env", inspect.Parameter.KEYWORD_ONLY, default=None, annotation=str | None
    )
    return inspect.Signature(
        parameters=[*signature_from_params_type(params_type), env, *extra_parameters],
        return_annotation=None,
    )


def is_datetime_annotation(annotation: Any) -> bool:
    """Return ``True`` iff *annotation* is ``datetime`` or a union including it.

    Strictly rejects ``date`` (which is a supertype of ``datetime``) so we
    only touch fields explicitly typed as ``datetime``.
    """
    if annotation is datetime:
        return True
    if get_origin(annotation) in (Union, UnionType):
        return any(arg is datetime for arg in get_args(annotation))
    return False


def coerce_to_utc(value: Any) -> Any:
    """Promote a ``datetime``, a ``date`` or an ISO string to a tz-aware ``datetime``.

    A naive value is read as UTC and a ``date`` as its midnight there; an
    offset-bearing value keeps its offset, and therefore its instant.
    Returns *value* unchanged when it is already a tz-aware ``datetime``,
    when it is a string that does not parse as an ISO datetime, and when it
    is any other value.
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed
    return value


def normalize_datetime_fields(
    resolved: dict[str, Any],
    params_type: type[msgspec.Struct],
) -> dict[str, Any]:
    """Coerce the fields *params_type* declares as ``datetime`` into tz-aware values.

    Args:
        resolved: Parameter mapping with placeholders already resolved.
        params_type: ``msgspec.Struct`` describing the expected field types.

    Returns:
        A new dict whose datetime-typed entries went through
        :func:`coerce_to_utc`; every other value passes through unchanged.
    """
    hints = get_type_hints(params_type)
    datetime_fields = {
        field.name
        for field in msgspec.structs.fields(params_type)
        if is_datetime_annotation(hints.get(field.name))
    }
    if not datetime_fields:
        return resolved
    return {
        key: coerce_to_utc(value) if key in datetime_fields else value
        for key, value in resolved.items()
    }


__all__ = [
    "coerce_to_utc",
    "is_datetime_annotation",
    "normalize_datetime_fields",
    "signature_from_params_type",
    "synthesise_flow_signature",
]
