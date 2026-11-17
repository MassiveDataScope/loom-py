from __future__ import annotations

import pytest

from loom.core.job.job import Job
from loom.core.job.policy import Backoff
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn

# ---------------------------------------------------------------------------
# Concrete subclasses used across tests
# ---------------------------------------------------------------------------


class AsyncJob(Job[int]):
    """Minimal async job for testing."""

    async def execute(self, value: int) -> int:  # type: ignore[override]
        return value * 2


class SyncJob(Job[str]):
    """Minimal sync job for testing."""

    def execute(self, text: str) -> str:  # type: ignore[override]
        return text.upper()


class CustomQueueJob(Job[None]):
    __queue__ = "heavy"
    __retry__ = Backoff(first_s=10, factor=3, max_s=600)
    __countdown__ = 10
    __timeout__ = 600
    __priority__ = 3

    async def execute(self) -> None:  # type: ignore[override]
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestJob:
    def test_job_is_abstract(self) -> None:
        with pytest.raises(TypeError):
            Job()  # type: ignore[abstract]

    def test_async_job_is_instantiable(self) -> None:
        job = AsyncJob()
        assert isinstance(job, Job)

    def test_sync_job_is_instantiable(self) -> None:
        job = SyncJob()
        assert isinstance(job, Job)

    def test_default_classvars_on_async_job(self) -> None:
        assert AsyncJob.__queue__ == "default"
        assert AsyncJob.__retry__ is None
        assert AsyncJob.__countdown__ == 0
        assert AsyncJob.__timeout__ is None
        assert AsyncJob.__priority__ == 0

    def test_custom_classvars_on_subclass(self) -> None:
        assert CustomQueueJob.__queue__ == "heavy"
        assert CustomQueueJob.__retry__ == Backoff(first_s=10, factor=3, max_s=600)
        assert CustomQueueJob.__countdown__ == 10
        assert CustomQueueJob.__timeout__ == 600
        assert CustomQueueJob.__priority__ == 3

    def test_custom_classvars_do_not_affect_sibling_jobs(self) -> None:
        assert AsyncJob.__queue__ == "default"
        assert SyncJob.__queue__ == "default"

    def test_execution_plan_defaults_to_none(self) -> None:
        assert AsyncJob.__execution_plan__ is None
        assert SyncJob.__execution_plan__ is None

    def test_computes_defaults_to_empty_tuple(self) -> None:
        assert AsyncJob.computes == ()

    def test_rules_defaults_to_empty_tuple(self) -> None:
        assert AsyncJob.rules == ()

    def test_computes_and_rules_overridable_per_subclass(self) -> None:
        # Use minimal callables that satisfy the ComputeFn and RuleFn contracts.
        def _identity(cmd: object, fields: frozenset[str]) -> object:
            return cmd

        class _AlwaysOk:
            def __call__(self, cmd: object, fields: frozenset[str]) -> None:
                return None

            def from_command(self, *args: object) -> _AlwaysOk:
                return self

            def from_params(self, *names: str) -> _AlwaysOk:
                return self

            def when_present(self, predicate: object) -> _AlwaysOk:
                return self

        dummy_compute: ComputeFn[object] = _identity
        dummy_rule: RuleFn = _AlwaysOk()

        class RichJob(Job[None]):
            computes = (dummy_compute,)
            rules = (dummy_rule,)

            async def execute(self) -> None:  # type: ignore[override]
                pass

        assert RichJob.computes == (dummy_compute,)
        assert RichJob.rules == (dummy_rule,)
        assert AsyncJob.computes == ()
        assert AsyncJob.rules == ()
