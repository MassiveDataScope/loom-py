"""End-to-end recipe for ``CallerBoundSql``: compiled use case, real executor.

The collaborator can only bind the roles to an identity the *transport*
verified. What makes that identity reach ``execute`` is the ``Caller()``
marker: a parameter declared without it is an ordinary primitive parameter,
which the compiler binds from the ``params`` the calling code supplies — and on
the agent path those params are tool arguments the model writes.

These tests run both shapes through the real compiler and executor and pin the
difference, because it is invisible in the signature.
"""

from __future__ import annotations

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.stores.sql import CallerBoundSql, SqlQueryResult, SqlQueryService
from loom.core.stores.sql.abc import RolesNotBoundError
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config, make_sql_config

_VERIFIED = Identity(subject="alice", roles=("role_viz_sales",), mechanism="jwt")
_FORGED = Identity(subject="root", roles=("role_viz_reader",), mechanism="jwt")


class ListSales(UseCase[object, SqlQueryResult]):
    """The documented recipe: the identity is declared with ``Caller()``."""

    def __init__(self, sql: CallerBoundSql) -> None:
        self._sql = sql

    async def execute(self, identity: Identity = Caller()) -> SqlQueryResult:
        return await self._sql.execute(
            "SELECT * FROM sales", connection="analytics", identity=identity
        )


class ListSalesUnmarked(UseCase[object, SqlQueryResult]):
    """The same code without the marker — the identity becomes a caller input."""

    def __init__(self, sql: CallerBoundSql) -> None:
        self._sql = sql

    async def execute(self, identity: Identity) -> SqlQueryResult:
        return await self._sql.execute(
            "SELECT * FROM sales", connection="analytics", identity=identity
        )


@pytest.fixture
def fake_executor() -> FakeSqlExecutor:
    """Fresh call-capturing fake executor per test."""
    return FakeSqlExecutor()


def _caller_bound(executor: FakeSqlExecutor) -> CallerBoundSql:
    config = make_sql_config(analytics=make_connection_config())
    service = SqlQueryService(executors={"analytics": executor}, config=config)
    return CallerBoundSql(service, config)


async def _run(use_case: UseCase[object, SqlQueryResult], **kwargs: object) -> SqlQueryResult:
    compiler = UseCaseCompiler()
    compiler.compile(type(use_case))
    executor = RuntimeExecutor(compiler)
    result: SqlQueryResult = await executor.execute(use_case, **kwargs)  # type: ignore[arg-type]
    return result


class TestCallerBoundSqlIntegration:
    async def test_the_marked_identity_parameter_is_not_a_caller_supplied_param(self) -> None:
        """``Caller()`` takes the parameter out of the params the caller writes."""
        plan = UseCaseCompiler().compile(ListSales)
        assert plan.caller_binding is not None
        assert [binding.name for binding in plan.param_bindings] == []

    async def test_the_query_runs_with_the_verified_identity_not_the_supplied_one(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """Params carrying an identity cannot displace the one the transport verified."""
        use_case = ListSales(_caller_bound(fake_executor))
        await _run(use_case, identity=_VERIFIED, params={"identity": _FORGED})
        assert fake_executor.calls[0].options.roles == ("role_viz_sales",)

    async def test_an_unmarked_identity_parameter_runs_with_the_supplied_identity(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """Why the marker is mandatory: without it, the params decide who the caller is.

        This is the shape the docstring must never show. It is pinned here so the
        test above cannot pass for the wrong reason.
        """
        use_case = ListSalesUnmarked(_caller_bound(fake_executor))
        await _run(use_case, identity=_VERIFIED, params={"identity": _FORGED})
        assert fake_executor.calls[0].options.roles == ("role_viz_reader",)

    async def test_the_verified_caller_is_still_refused_when_it_holds_no_allowlisted_role(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """The refusal follows the verified identity, whatever the params claim."""
        use_case = ListSales(_caller_bound(fake_executor))
        intruder = Identity(subject="mallory", roles=("role_intruder",), mechanism="jwt")
        with pytest.raises(RolesNotBoundError):
            await _run(use_case, identity=intruder, params={"identity": _VERIFIED})
        assert fake_executor.calls == []
