"""ETLScenario — backend-agnostic, immutable seed dataset, and StepRunnerProto."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeVar, runtime_checkable

from loom.etl.testing._result import StepResult
from loom.etl.testing._runners import PolarsStepRunner, SeedSchema, TempData


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
        schema: SeedSchema | None = None,
    ) -> object:
        """Register raw data under the logical table reference *ref*."""
        ...

    def seed_temp(
        self,
        name: str | StepResult,
        data: TempData | None = None,
        columns: Sequence[str] | None = None,
        *,
        schema: SeedSchema | None = None,
    ) -> object:
        """Register data as the temp *name*, or seed the result of an ``IntoTemp`` step."""
        ...

    def run(self, step_cls: type[Any], params: Any) -> StepResult:
        """Compile and execute *step_cls* against the seeded state."""
        ...


_RunnerT = TypeVar("_RunnerT", bound=StepRunnerProto)


class _ScenarioOp(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def name(self) -> str: ...

    def apply(self, runner: StepRunnerProto) -> None: ...


@dataclass(frozen=True, eq=False)
class _Seed:
    """Rows seeded as a table or as a temp."""

    kind: Literal["table", "temp"]
    name: str
    data: tuple[tuple[Any, ...], ...]
    columns: tuple[str, ...]
    schema: SeedSchema | None

    def apply(self, runner: StepRunnerProto) -> None:
        data, columns = list(self.data), list(self.columns)
        if self.kind == "temp":
            runner.seed_temp(self.name, data, columns, schema=self.schema)
        elif self.schema is None:
            # Positional call without schema= keeps runners written for the old protocol working.
            runner.seed(self.name, data, columns)
        else:
            runner.seed(self.name, data, columns, schema=self.schema)


def _seed(
    kind: Literal["table", "temp"],
    name: str,
    data: Sequence[tuple[Any, ...]],
    columns: Sequence[str],
    schema: SeedSchema | None,
) -> _Seed:
    return _Seed(kind, name, tuple(data), tuple(columns), dict(schema) if schema else None)


class _AfterStep:
    """Seed the output of an ``IntoTemp`` step run on the operations that precede it.

    The step runs once, on a fresh :class:`PolarsStepRunner` built only from
    *prefix*, the immutable operations of the scenario ``after()`` was called
    on.  The output depends only on that prefix, so it is memoized on this
    operation and every application seeds the same temp.
    """

    kind = "after"

    def __init__(self, prefix: tuple[_ScenarioOp, ...], step_cls: type[Any], params: Any) -> None:
        self._prefix = prefix
        self._step_cls = step_cls
        self._params = params
        self._result: StepResult | None = None

    @property
    def name(self) -> str:
        return self._step_cls.__qualname__

    def apply(self, runner: StepRunnerProto) -> None:
        runner.seed_temp(self._upstream_result())

    def _upstream_result(self) -> StepResult:
        if self._result is None:
            upstream = ETLScenario(self._prefix).apply(PolarsStepRunner())
            result = upstream.run(self._step_cls, self._params)
            if result.temp_name is None:
                raise TypeError(
                    f"ETLScenario.after() needs a step whose target is IntoTemp; "
                    f"{self.name} writes a table."
                )
            self._result = result
        return self._result


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
        schema: SeedSchema | None = None,
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
        return self._with(_seed("table", ref, data, columns, schema))

    def with_temp(
        self,
        name: str,
        data: Sequence[tuple[Any, ...]],
        columns: Sequence[str],
        *,
        schema: SeedSchema | None = None,
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
        return self._with(_seed("temp", name, data, columns, schema))

    def after(self, step_cls: type[Any], params: Any) -> ETLScenario:
        """Return a new scenario that also holds the real output of an ``IntoTemp`` step.

        The step runs once, the first time the scenario is applied, on a fresh
        in-memory :class:`PolarsStepRunner` seeded only with the entries of
        this scenario (the ones before ``after()``).  It does not see the
        runner the scenario is applied to: neither its config
        (:meth:`PolarsStepRunner.with_config`), nor seeds added outside the
        scenario, nor its ``delta_root``.  Every application seeds the output
        with ``seed_temp``; entries added after ``after()``, or scenarios
        composed with ``+``, do not change it.

        The memo lives in the returned scenario, and in the scenarios derived
        from it.  Define the scenario at module level, or in a module- or
        session-scoped fixture, so the upstream step runs only once; a
        scenario built inside each test runs it in each test.

        Args:
            step_cls: Upstream step whose target is ``IntoTemp``.
            params:   Params instance for that step.

        Returns:
            A new :class:`ETLScenario`.

        Raises:
            TypeError: On first application, when the step does not write a temp.
        """
        return self._with(_AfterStep(self._ops, step_cls, params))

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
        tables = [op.name for op in self._ops if op.kind == "table"]
        temps = [op.name for op in self._ops if op.kind == "temp"]
        after = [op.name for op in self._ops if op.kind == "after"]
        return f"ETLScenario(tables={tables}, temps={temps}, after={after})"

    def _with(self, op: _ScenarioOp) -> ETLScenario:
        return ETLScenario(self._ops + (op,))
