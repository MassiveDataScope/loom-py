"""``per_execution``: built once per execution, fresh outside one, never cached when closed."""

from __future__ import annotations

import contextvars
import threading
from concurrent.futures import ThreadPoolExecutor, wait

import pytest

from loom.core.execution import ExecutionContext, ExecutionScope
from loom.core.execution.context import per_execution
from loom.core.identity import ANONYMOUS
from tests.unit.core.execution.context.doubles import AcmeBuiltSession

THREADS = 4
TIMEOUT_S = 5.0


class TestPerExecutionCache:
    def test_outside_an_execution_each_call_builds(self) -> None:
        session = per_execution(AcmeBuiltSession, AcmeBuiltSession)

        assert session() is not session()

    def test_inside_an_execution_the_first_value_is_kept(self) -> None:
        session = per_execution(AcmeBuiltSession, AcmeBuiltSession)

        with ExecutionScope(ANONYMOUS):
            first, second = session(), session()
        with ExecutionScope(ANONYMOUS):
            third = session()

        assert first is second
        assert third is not first

    def test_two_keys_keep_two_values(self) -> None:
        first = per_execution(AcmeBuiltSession, AcmeBuiltSession)
        other = per_execution(object(), AcmeBuiltSession)

        with ExecutionScope(ANONYMOUS):
            assert first() is not other()

    def test_a_closed_context_caches_nothing(self) -> None:
        session = per_execution(AcmeBuiltSession, AcmeBuiltSession)

        def closed() -> tuple[object, object]:
            token = ExecutionContext.entered(ANONYMOUS)
            token.close()
            with pytest.raises(RuntimeError):
                token.close()
            return session(), session()

        first, second = contextvars.copy_context().run(closed)

        assert first is not second

    def test_threads_sharing_one_execution_build_one_value_once(self) -> None:
        start = threading.Barrier(THREADS, timeout=TIMEOUT_S)
        built: list[AcmeBuiltSession] = []

        def build() -> AcmeBuiltSession:
            threading.Event().wait(0.01)  # slow, so threads that start together all miss
            built.append(AcmeBuiltSession())
            return built[-1]

        session = per_execution(AcmeBuiltSession, build)

        def resolve() -> object:
            start.wait()
            return session()

        with ExecutionScope(ANONYMOUS):
            contexts = [contextvars.copy_context() for _ in range(THREADS)]
            with ThreadPoolExecutor(THREADS) as pool:
                futures = [pool.submit(context.run, resolve) for context in contexts]
                done, pending = wait(futures, timeout=TIMEOUT_S)
            kept = session()

        assert not pending
        assert {id(future.result()) for future in done} == {id(kept)}
        assert len(built) == 1

    def test_a_value_may_need_another_on_the_same_thread(self) -> None:
        inner = per_execution(object(), AcmeBuiltSession)
        outer = per_execution(AcmeBuiltSession, lambda: (inner(), inner()))

        with ExecutionScope(ANONYMOUS):
            first, second = outer()  # type: ignore[misc]

        assert first is second

    def test_a_failed_build_leaves_the_key_to_another_thread(self) -> None:
        attempts: list[int] = []

        def build() -> AcmeBuiltSession:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("acme backend down")
            return AcmeBuiltSession()

        session = per_execution(AcmeBuiltSession, build)
        with ExecutionScope(ANONYMOUS):
            with pytest.raises(RuntimeError):
                session()
            context, retried = contextvars.copy_context(), list[object]()
            # A daemon thread: the test stays bounded if it waits for a leaked claim.
            worker = threading.Thread(
                target=lambda: retried.append(context.run(session)), daemon=True
            )
            worker.start()
            worker.join(TIMEOUT_S)
            assert retried, "the other thread waited for a failed build's claim"
            assert session() is retried[0]

        assert len(attempts) == 2
