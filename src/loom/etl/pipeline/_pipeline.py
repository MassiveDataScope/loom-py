"""ETL process and pipeline base types.

An :class:`ETLProcess` is an ordered set of :class:`~loom.etl.ETLStep`
types; a nested list marks a parallel group whose steps run concurrently.
An :class:`ETLPipeline` is the top-level entry point for compilation and
execution.  It declares an ordered set of :class:`ETLProcess` types with the
same nested-list convention for parallelism.

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

from loom.core.errors import InvalidType
from loom.etl.pipeline._generics import _extract_generic_arg

ParamsT = TypeVar("ParamsT")

_StepItem = Any
_ProcessItem = Any


class ETLProcess(Generic[ParamsT]):
    """Base class for ordered sets of ETL steps.

    Declare :attr:`steps` as a list of step types.  A nested list within
    :attr:`steps` declares a parallel group — the executor dispatches all
    steps in the group concurrently and waits for all to complete before
    advancing.

    All steps must share the same ``ParamsT`` as the process — enforced by
    the :class:`~loom.etl.compiler.ETLCompiler`.

    Attributes:
        steps: Ordered list of step types.  Nested lists are parallel groups.

    Example::

        class DailyPipeline(ETLProcess[DailyOrdersParams]):
            steps = [
                IngestStep,
                [EnrichA, EnrichB],   # parallel
                AggregateStep,
            ]
    """

    steps: ClassVar[list[_StepItem]] = []

    _params_type: ClassVar[type[Any] | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._params_type = _extract_generic_arg(cls, ETLProcess)
        if not isinstance(cls.__dict__.get("steps", []), list):
            raise InvalidType(
                f"{cls.__qualname__}: 'steps' must be a list, "
                f"got {type(cls.__dict__['steps']).__name__}"
            )


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
            raise InvalidType(
                f"{cls.__qualname__}: 'processes' must be a list, "
                f"got {type(cls.__dict__['processes']).__name__}"
            )
