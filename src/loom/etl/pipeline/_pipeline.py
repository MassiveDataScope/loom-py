"""ETL pipeline base type.

An :class:`ETLPipeline` is the top-level entry point for compilation and
execution.  It declares an ordered set of :class:`~loom.etl.ETLProcess`
types with the same nested-list convention for parallelism.

Example::

    class DailyOrdersPipeline(ETLPipeline[DailyOrdersParams]):
        processes = [
            BuildStagingProcess,
            [EnrichProductsProcess, EnrichCustomersProcess],  # parallel
            AggregateProcess,
        ]
"""

from __future__ import annotations

from typing import Any, ClassVar, Generic, TypeVar

from loom.etl.pipeline._generics import _extract_generic_arg

ParamsT = TypeVar("ParamsT")

_ProcessItem = Any


class ETLPipeline(Generic[ParamsT]):
    """Top-level ETL orchestration unit.

    Declares an ordered list of :class:`~loom.etl.ETLProcess` types.
    A nested list within :attr:`processes` declares a parallel group.

    The pipeline is the entry point for :class:`~loom.etl.compiler.ETLCompiler`
    and :class:`~loom.etl.compiler.ETLExecutor`.  All processes must share the
    same ``ParamsT``.

    Attributes:
        processes: Ordered list of process types.  Nested lists are parallel groups.

    Example::

        class DailyOrdersPipeline(ETLPipeline[DailyOrdersParams]):
            processes = [
                BuildStagingProcess,
                [EnrichProductsProcess, EnrichCustomersProcess],
                AggregateProcess,
            ]
    """

    processes: ClassVar[list[_ProcessItem]] = []

    _params_type: ClassVar[type[Any] | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._params_type = _extract_generic_arg(cls, ETLPipeline)
        if not isinstance(cls.__dict__.get("processes", []), list):
            raise TypeError(
                f"{cls.__qualname__}: 'processes' must be a list, "
                f"got {type(cls.__dict__['processes']).__name__}"
            )
