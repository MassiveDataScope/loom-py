"""ETLScenario — backend-agnostic, immutable seed dataset, and StepRunnerProto."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, runtime_checkable

from loom.etl.schema._schema import LoomType
from loom.etl.testing._result import StepResult


@runtime_checkable
class StepRunnerProto(Protocol):
    """Protocol satisfied by both :class:`PolarsStepRunner` and :class:`SparkStepRunner`.

    Any object implementing :meth:`seed`, :meth:`seed_temp` and :meth:`run`
    is compatible with :class:`ETLScenario`.
    """

    def seed(
        self,
        ref: str,
        data: list[tuple[Any, ...]],
        columns: list[str],
        *,
        schema: Mapping[str, LoomType] | None = None,
    ) -> Any:
        """Register raw data under the logical table reference *ref*."""
        ...

    def seed_temp(
        self,
        name: Any,
        data: Any = None,
        columns: list[str] | None = None,
        *,
        schema: Mapping[str, LoomType] | None = None,
    ) -> Any:
        """Register data as the temp *name*, as an ``IntoTemp`` step would."""
        ...

    def run(self, step_cls: type[Any], params: Any) -> StepResult:
        """Compile and execute *step_cls* against the seeded state."""
        ...


_RunnerT = TypeVar("_RunnerT", bound=StepRunnerProto)


class _ScenarioOp(Protocol):
    def apply(self, runner: StepRunnerProto) -> None: ...

    def describe(self) -> tuple[str, str]: ...


@dataclass(frozen=True, eq=False)
class _TableSeed:
    ref: str
    data: tuple[tuple[Any, ...], ...]
    columns: tuple[str, ...]
    schema: Mapping[str, LoomType] | None

    def apply(self, runner: StepRunnerProto) -> None:
        # Positional call without schema= keeps runners written for the old protocol working.
        if self.schema is None:
            runner.seed(self.ref, list(self.data), list(self.columns))
        else:
            runner.seed(self.ref, list(self.data), list(self.columns), schema=self.schema)

    def describe(self) -> tuple[str, str]:
        return ("table", self.ref)


@dataclass(frozen=True, eq=False)
class _TempSeed:
    name: str
    data: tuple[tuple[Any, ...], ...]
    columns: tuple[str, ...]
    schema: Mapping[str, LoomType] | None

    def apply(self, runner: StepRunnerProto) -> None:
        runner.seed_temp(self.name, list(self.data), list(self.columns), schema=self.schema)

    def describe(self) -> tuple[str, str]:
        return ("temp", self.name)


class _AfterStep:
    """Run an ``IntoTemp`` step once, then seed its memoized output on later applications."""

    def __init__(self, step_cls: type[Any], params: Any) -> None:
        self._step_cls = step_cls
        self._params = params
        self._result: StepResult | None = None

    def apply(self, runner: StepRunnerProto) -> None:
        if self._result is not None:
            runner.seed_temp(self._result)
            return
        result = runner.run(self._step_cls, self._params)
        if result.temp_name is None:
            raise TypeError(
                f"ETLScenario.after() needs a step whose target is IntoTemp; "
                f"{self._step_cls.__qualname__} writes a table."
            )
        self._result = result

    def describe(self) -> tuple[str, str]:
        return ("after", self._step_cls.__qualname__)


class ETLScenario:
    """Named, reusable and immutable seed dataset for step runner tests.

    Stores input data as plain Python tuples — no backend dependency at
    definition time.  Every :meth:`with_table`, :meth:`with_temp`,
    :meth:`after` and ``+`` returns a new scenario; the original is never
    changed, so scenarios are safe to define at module level and share.

    Data is passed to the runner's :meth:`~StepRunnerProto.seed` and
    :meth:`~StepRunnerProto.seed_temp` when :meth:`apply` is called (or when
    the scenario is given to ``PolarsStepRunner(*scenarios)``), in the order
    the entries were added.

    Cache scenarios and frames, not runners: a runner holds the state its
    steps write, so tests that mutate it (upserts, replaces, fan-in temps)
    need a function-scoped runner.

    Example::

        RAW_ORDERS = ETLScenario().with_table(
            "raw.orders", [(1, 10.0)], ["id", "amount"],
            schema={"id": LoomDtype.INT64, "amount": LoomDtype.FLOAT64},
        )
        NORMALIZED = RAW_ORDERS.after(NormalizeOrders, P)
        FULL = NORMALIZED + CUSTOMERS

        def test_enrich():
            PolarsStepRunner(FULL).run(EnrichOrders, P).assert_count(1)
    """

    def __init__(self, _ops: tuple[_ScenarioOp, ...] = ()) -> None:
        self._ops = _ops

    def with_table(
        self,
        ref: str,
        data: Sequence[tuple[Any, ...]],
        columns: Sequence[str],
        *,
        schema: Mapping[str, LoomType] | None = None,
    ) -> ETLScenario:
        """Return a new scenario that also seeds the table *ref*.

        Args:
            ref:     Logical table reference, e.g. ``"raw.orders"``.
            data:    Row data as a sequence of tuples.
            columns: Column names aligned with the tuple positions.
            schema:  Loom types of some or all columns.  Declare it for empty
                     or all-null columns, which otherwise have no dtype.

        Returns:
            A new :class:`ETLScenario`.
        """
        schema_copy = dict(schema) if schema is not None else None
        return self._with(_TableSeed(ref, tuple(data), tuple(columns), schema_copy))

    def with_temp(
        self,
        name: str,
        data: Sequence[tuple[Any, ...]],
        columns: Sequence[str],
        *,
        schema: Mapping[str, LoomType] | None = None,
    ) -> ETLScenario:
        """Return a new scenario that also seeds the temp *name*.

        Args:
            name:    ``temp_name`` the consuming ``FromTemp`` declares (not its alias).
            data:    Row data as a sequence of tuples.
            columns: Column names aligned with the tuple positions.
            schema:  Loom types of some or all columns.

        Returns:
            A new :class:`ETLScenario`.
        """
        schema_copy = dict(schema) if schema is not None else None
        return self._with(_TempSeed(name, tuple(data), tuple(columns), schema_copy))

    def after(self, step_cls: type[Any], params: Any) -> ETLScenario:
        """Return a new scenario that also holds the real output of an ``IntoTemp`` step.

        The step runs, against the state seeded so far, the first time the
        scenario is applied; its output is memoized and later applications
        only seed it with ``seed_temp``.  Scenarios derived from the returned
        one share the memo.

        Args:
            step_cls: Upstream step whose target is ``IntoTemp``.
            params:   Params instance for that step.

        Returns:
            A new :class:`ETLScenario`.

        Raises:
            TypeError: On first application, when the step does not write a temp.
        """
        return self._with(_AfterStep(step_cls, params))

    def apply(self, runner: _RunnerT) -> _RunnerT:
        """Seed every entry into *runner*, in order, and return it.

        Args:
            runner: Any :class:`StepRunnerProto`-compatible runner.

        Returns:
            The same *runner* for fluent chaining.
        """
        for op in self._ops:
            op.apply(runner)
        return runner

    def __add__(self, other: object) -> ETLScenario:
        """Return a scenario that applies *self* then *other*."""
        if not isinstance(other, ETLScenario):
            return NotImplemented
        return ETLScenario(self._ops + other._ops)

    def __repr__(self) -> str:
        described = [op.describe() for op in self._ops]
        tables = [name for kind, name in described if kind == "table"]
        temps = [name for kind, name in described if kind != "table"]
        return f"ETLScenario(tables={tables}, temps={temps})"

    def _with(self, op: _ScenarioOp) -> ETLScenario:
        return ETLScenario(self._ops + (op,))
