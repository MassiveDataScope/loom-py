"""StepSQL — SQL-first ETL step.

Declares the transformation as SQL text instead of implementing a Python
``execute()`` body. SQL is rendered by the step, then executed by the runtime
reader backend (Spark/Polars) via ``SQLExecutor.execute_sql``.

Internal — not part of the public API surface, exposed via ``loom.etl``.
"""

from __future__ import annotations

import typing
from typing import Any, ClassVar, Generic, TypeVar, cast

from loom.etl.pipeline._sql import resolve_sql
from loom.etl.pipeline._step import ETLStep

ParamsT = TypeVar("ParamsT")
FrameT = TypeVar("FrameT")


class StepSQL(ETLStep[ParamsT], Generic[ParamsT, FrameT]):
    """SQL-first ETL step — transformation declared as SQL, not Python.

    Declare ``sql`` as a class variable (static SQL) or a ``@staticmethod``
    that receives params and returns a SQL string (dynamic SQL).

    Sources still support ``.where()`` for pre-filtering before the frame
    is registered as a view.  Target write modes work identically to
    :class:`~loom.etl.ETLStep`.

    .. warning::
        Avoid interpolating raw string params directly.  Use
        ``FromTable.where()`` for source-level filtering instead.
    """

    sql: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        params_type, frame_type = _extract_stepsql_types(cls)
        if params_type is not None:
            cls._params_type = params_type
        if frame_type is not None:
            _generate_execute(cls, frame_type)

    def execute(self, params: Any, **frames: Any) -> Any:
        _ = params
        _ = frames
        raise NotImplementedError(
            f"{type(self).__qualname__} is a SQL step. "
            "It is executed by ETLExecutor via SQLExecutor.execute_sql()."
        )

    def render_sql(self, params: Any) -> str:
        """Render SQL for current params instance."""
        return _resolve_query(type(self), params)


def _extract_stepsql_types(cls: type) -> tuple[type | None, type | None]:
    for base in getattr(cls, "__orig_bases__", ()):
        origin = getattr(base, "__origin__", None)
        if origin is StepSQL:
            args = typing.get_args(base)
            params_t = cast(type, args[0]) if len(args) > 0 else None
            frame_t = cast(type, args[1]) if len(args) > 1 else None
            return params_t, frame_t
    return None, None


def _generate_execute(cls: type, return_type: type) -> None:
    """Inject execute() return annotation inferred from StepSQL generic."""

    def execute(self: Any, params: Any, **frames: Any) -> Any:
        _ = params
        _ = frames
        raise NotImplementedError(
            f"{type(self).__qualname__} is a SQL step. "
            "It is executed by ETLExecutor via SQLExecutor.execute_sql()."
        )

    execute.__annotations__["return"] = return_type
    cls.execute = execute  # type: ignore[attr-defined]


def _resolve_query(cls: type, params: Any) -> str:
    raw = cls.sql  # type: ignore[attr-defined]
    if callable(raw):
        return raw(params)  # type: ignore[no-any-return]
    return resolve_sql(raw, params)
