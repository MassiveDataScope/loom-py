"""The plan's steps build the arguments of ``execute()``, then ``execute()`` is called.

Every scenario runs through ``RuntimeExecutor.execute``: what is asserted is
what a use case receives (``loom.core.engine.preparation``).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import ParameterBindingError, RuntimeExecutor
from loom.core.engine.plan import ParamBinding
from loom.core.errors import MissingCaller, NotFound, RuleViolation, RuleViolations
from loom.core.identity import ANONYMOUS
from loom.core.use_case.markers import Input
from loom.core.use_case.use_case import UseCase

from .conftest import MakeExecutor
from .doubles import (
    ALICE,
    AcmeCreateUserCommand,
    AcmeDocsSearchUseCase,
    AcmeFullPipelineUseCase,
    AcmeInputOnlyUseCase,
    AcmeMcpOnlyUseCase,
    AcmeMultiExistsUseCase,
    AcmeMultiLoadUseCase,
    AcmeNoMarkersUseCase,
    AcmeNoMcpUseCase,
    AcmeOrder,
    AcmeParamsAndInputUseCase,
    AcmeRenamedInputUseCase,
    AcmeTwoServersUseCase,
    AcmeUser,
    AcmeWithExistsRaiseUseCase,
    AcmeWithExistsUseCase,
    AcmeWithLoadByFieldUseCase,
    AcmeWithLoadFromCommandUseCase,
    AcmeWithLoadUseCase,
    McpCalls,
    fake_repo,
    mcp_resolver,
)


class TestParameters:
    """Primitive parameters are bound by name and coerced to their declared type."""

    async def test_executes_and_returns_result(self, executor: RuntimeExecutor) -> None:
        result = await executor.execute(
            AcmeNoMarkersUseCase(),
            params={"value": 42},
        )
        assert result == "value=42"

    async def test_missing_param_raises(self, executor: RuntimeExecutor) -> None:
        use_case = AcmeNoMarkersUseCase()
        with pytest.raises(ValueError, match="missing required parameter"):
            await executor.execute(use_case, params={})

    async def test_invalid_param_type_raises_binding_error(self, executor: RuntimeExecutor) -> None:
        use_case = AcmeNoMarkersUseCase()
        with pytest.raises(ParameterBindingError, match="invalid value for parameter 'value'"):
            await executor.execute(use_case, params={"value": "not-an-int"})


class TestCommand:
    """The ``Input()`` command is built from the payload."""

    async def test_command_built_from_payload(self, executor: RuntimeExecutor) -> None:
        result = await executor.execute(
            AcmeInputOnlyUseCase(),
            payload={"email": "alice@corp.com", "name": "Alice"},
        )
        assert result == "alice@corp.com"

    async def test_missing_payload_raises(self, executor: RuntimeExecutor) -> None:
        use_case = AcmeInputOnlyUseCase()
        with pytest.raises(ValueError, match="payload is required"):
            await executor.execute(use_case)

    async def test_params_and_input_together(self, executor: RuntimeExecutor) -> None:
        result = await executor.execute(
            AcmeParamsAndInputUseCase(),
            params={"tenant_id": "t-1"},
            payload={"email": "a@b.com", "name": "A"},
        )
        assert result.id == 99

    @pytest.mark.parametrize(
        ("payload", "expected_price"),
        [
            ({"price_cents": 123, "stock": 2}, 123),
            ({"priceCents": 321, "stock": 2}, 321),
        ],
    )
    async def test_input_command_accepts_snake_and_camel_case_keys(
        self,
        executor: RuntimeExecutor,
        payload: dict[str, int],
        expected_price: int,
    ) -> None:
        price, _ = await executor.execute(AcmeRenamedInputUseCase(), payload=payload)
        assert price == expected_price


class TestLoads:
    """``LoadById()`` / ``Load()`` fetch an entity before ``execute()``."""

    async def test_entity_loaded_via_repo(self, executor: RuntimeExecutor) -> None:
        user = AcmeUser(id=1, email="loaded@corp.com")
        repo = fake_repo(user)

        result = await executor.execute(
            AcmeWithLoadUseCase(),
            params={"user_id": 1},
            dependencies={AcmeUser: repo},
        )

        repo.get_by_id.assert_awaited_once_with(1, profile="default")
        assert result == "loaded@corp.com"

    async def test_entity_loaded_by_field_via_repo(self, executor: RuntimeExecutor) -> None:
        user = AcmeUser(id=1, email="loaded@corp.com")
        repo = fake_repo(user)

        result = await executor.execute(
            AcmeWithLoadByFieldUseCase(),
            params={"email": "loaded@corp.com"},
            dependencies={AcmeUser: repo},
        )

        repo.get_by.assert_awaited_once_with("email", "loaded@corp.com", profile="default")
        assert result == "loaded@corp.com"

    async def test_entity_loaded_by_command_field_via_repo(self, executor: RuntimeExecutor) -> None:
        user = AcmeUser(id=1, email="loaded@corp.com")
        repo = fake_repo(user)

        result = await executor.execute(
            AcmeWithLoadFromCommandUseCase(),
            payload={"email": "loaded@corp.com", "name": "Alice"},
            dependencies={AcmeUser: repo},
        )

        repo.get_by.assert_awaited_once_with("email", "loaded@corp.com", profile="default")
        assert result == "loaded@corp.com"

    async def test_load_override_skips_repo(self, executor: RuntimeExecutor) -> None:
        user = AcmeUser(id=7, email="override@corp.com")
        repo = fake_repo(None)

        result = await executor.execute(
            AcmeWithLoadUseCase(),
            params={"user_id": 7},
            dependencies={AcmeUser: repo},
            load_overrides={AcmeUser: user},
        )

        repo.get_by_id.assert_not_awaited()
        assert result == "override@corp.com"

    async def test_missing_entity_raises_not_found(self, executor: RuntimeExecutor) -> None:
        repo = fake_repo(None)

        use_case = AcmeWithLoadUseCase()
        with pytest.raises(NotFound) as exc_info:
            await executor.execute(
                use_case,
                params={"user_id": 99},
                dependencies={AcmeUser: repo},
            )

        assert exc_info.value.entity == "AcmeUser"
        assert exc_info.value.id == 99

    async def test_no_dependencies_raises(self, executor: RuntimeExecutor) -> None:
        use_case = AcmeWithLoadUseCase()
        with pytest.raises(RuntimeError, match="No dependencies provided"):
            await executor.execute(
                use_case,
                params={"user_id": 1},
            )

    async def test_missing_repo_type_raises(self, executor: RuntimeExecutor) -> None:
        use_case = AcmeWithLoadUseCase()
        with pytest.raises(RuntimeError, match="No repository registered"):
            await executor.execute(
                use_case,
                params={"user_id": 1},
                dependencies={},
            )

    async def test_repo_resolver_error_is_not_silenced(self, make_executor: MakeExecutor) -> None:
        def _resolver(_: type[Any]) -> Any:
            raise RuntimeError("resolver exploded")

        executor = make_executor(repo_resolver=_resolver)
        use_case = AcmeWithLoadUseCase()
        with pytest.raises(RuntimeError, match="resolver exploded"):
            await executor.execute(use_case, params={"user_id": 1})

    async def test_a_resolver_finding_no_repository_names_the_entity_type(
        self, make_executor: MakeExecutor
    ) -> None:
        executor = make_executor(repo_resolver=lambda entity_type: None)
        use_case = AcmeWithLoadUseCase()

        with pytest.raises(
            RuntimeError, match="No repository registered for entity type 'AcmeUser'"
        ):
            await executor.execute(use_case, params={"user_id": 1})


class TestExists:
    """``Exists()`` binds whether an entity is present."""

    async def test_exists_true_when_entity_is_present(self, executor: RuntimeExecutor) -> None:
        repo = fake_repo(AcmeUser(id=1, email="ok@corp.com"))

        result = await executor.execute(
            AcmeWithExistsUseCase(),
            params={"email": "ok@corp.com"},
            dependencies={AcmeUser: repo},
        )

        repo.exists_by.assert_awaited_once_with("email", "ok@corp.com")
        assert result is True

    async def test_exists_false_when_entity_is_missing(self, executor: RuntimeExecutor) -> None:
        repo = fake_repo(None)

        result = await executor.execute(
            AcmeWithExistsUseCase(),
            params={"email": "missing@corp.com"},
            dependencies={AcmeUser: repo},
        )

        assert result is False

    async def test_exists_raise_on_missing(self, executor: RuntimeExecutor) -> None:
        repo = fake_repo(None)

        use_case = AcmeWithExistsRaiseUseCase()
        with pytest.raises(NotFound):
            await executor.execute(
                use_case,
                params={"email": "missing@corp.com"},
                dependencies={AcmeUser: repo},
            )


class TestSeveralFetches:
    """Several loads and checks in one plan each bind their own result."""

    async def test_two_independent_loads_both_resolved(self, executor: RuntimeExecutor) -> None:
        """Both LoadById steps resolve their entities regardless of order."""
        user = AcmeUser(id=1, email="a@b.com")
        order = AcmeOrder(id=99)
        user_repo = fake_repo(user)
        order_repo = fake_repo(order)

        result = await executor.execute(
            AcmeMultiLoadUseCase(),
            params={"user_id": 1, "order_id": 99},
            dependencies={AcmeUser: user_repo, AcmeOrder: order_repo},
        )

        assert result == ("a@b.com", 99)
        user_repo.get_by_id.assert_awaited_once_with(1, profile="default")
        order_repo.get_by_id.assert_awaited_once_with(99, profile="default")

    async def test_two_independent_loads_first_missing_raises(
        self, executor: RuntimeExecutor
    ) -> None:
        """NotFound from one load propagates even when other load would succeed."""
        order = AcmeOrder(id=5)
        user_repo = fake_repo(None)  # user not found
        order_repo = fake_repo(order)

        use_case = AcmeMultiLoadUseCase()
        with pytest.raises(NotFound):
            await executor.execute(
                use_case,
                params={"user_id": 1, "order_id": 5},
                dependencies={AcmeUser: user_repo, AcmeOrder: order_repo},
            )

    async def test_two_independent_exists_both_resolved(self, executor: RuntimeExecutor) -> None:
        """Both Exists steps resolve and results are bound correctly."""
        user = AcmeUser(id=1, email="x@y.com")
        order = AcmeOrder(id=7)
        user_repo = fake_repo(user)
        order_repo = fake_repo(order)

        result = await executor.execute(
            AcmeMultiExistsUseCase(),
            params={"email": "x@y.com", "order_id": 7},
            dependencies={AcmeUser: user_repo, AcmeOrder: order_repo},
        )

        assert result == (True, True)

    async def test_loads_run_one_after_another_in_declared_order(
        self, executor: RuntimeExecutor
    ) -> None:
        """Each load is awaited before the next starts, even when its repository suspends."""
        call_order: list[str] = []

        async def slow_user(obj_id: Any, *, profile: str = "default") -> AcmeUser:
            call_order.append("user starts")
            await asyncio.sleep(0)
            call_order.append("user ends")
            return AcmeUser(id=obj_id, email="u@u.com")

        async def slow_order(obj_id: Any, *, profile: str = "default") -> AcmeOrder:
            call_order.append("order starts")
            await asyncio.sleep(0)
            call_order.append("order ends")
            return AcmeOrder(id=obj_id)

        user_repo = AsyncMock()
        user_repo.get_by_id = slow_user
        order_repo = AsyncMock()
        order_repo.get_by_id = slow_order
        result = await executor.execute(
            AcmeMultiLoadUseCase(),
            params={"user_id": 1, "order_id": 2},
            dependencies={AcmeUser: user_repo, AcmeOrder: order_repo},
        )

        assert result == ("u@u.com", 2)
        assert call_order == ["user starts", "user ends", "order starts", "order ends"]


class TestArguments:
    """Every step's result reaches ``execute()``."""

    async def test_kwargs_injected_correctly(self, executor: RuntimeExecutor) -> None:
        user = AcmeUser(id=5, email="user@corp.com")
        uc = AcmeFullPipelineUseCase()

        result = await executor.execute(
            uc,
            params={"user_id": 5},
            payload={"email": "new@corp.com", "name": "Alice"},
            dependencies={AcmeUser: fake_repo(user)},
        )

        assert result.id == 5
        assert uc.received_user is user
        assert uc.received_cmd is not None
        assert uc.received_cmd.email == "new@corp.com"


class TestComputesAndRules:
    """Computes reshape the command, then rules check it, before ``execute()``."""

    async def test_compute_applied_before_execute(self, executor: RuntimeExecutor) -> None:
        received: list[str] = []

        class TrackedUseCase(UseCase[Any, None]):
            computes = [lambda cmd, fs: type(cmd)(email="computed@corp.com", name=cmd.name)]

            async def execute(self, cmd: AcmeCreateUserCommand = Input()) -> None:
                received.append(cmd.email)

        await executor.execute(
            TrackedUseCase(),
            payload={"email": "original@corp.com", "name": "Alice"},
        )
        assert received == ["computed@corp.com"]

    async def test_rule_violation_raises_before_execute(self, executor: RuntimeExecutor) -> None:
        called: list[bool] = []

        def bad_rule(cmd: Any, fs: Any) -> None:
            raise RuleViolation("email", "invalid")

        class RuleUseCase(UseCase[Any, None]):
            rules = [bad_rule]

            async def execute(self, cmd: AcmeCreateUserCommand = Input()) -> None:
                called.append(True)

        use_case = RuleUseCase()
        with pytest.raises(RuleViolations) as exc_info:
            await executor.execute(
                use_case,
                payload={"email": "bad@corp.com", "name": "X"},
            )

        assert len(exc_info.value.violations) == 1
        assert exc_info.value.violations[0].field == "email"
        assert called == []

    async def test_rule_violations_accumulated(self, executor: RuntimeExecutor) -> None:
        def rule_a(cmd: Any, fs: Any) -> None:
            raise RuleViolation("email", "bad email")

        def rule_b(cmd: Any, fs: Any) -> None:
            raise RuleViolation("name", "bad name")

        class MultiRuleUseCase(UseCase[Any, None]):
            rules = [rule_a, rule_b]

            async def execute(self, cmd: AcmeCreateUserCommand = Input()) -> None:
                pass

        use_case = MultiRuleUseCase()
        with pytest.raises(RuleViolations) as exc_info:
            await executor.execute(
                use_case,
                payload={"email": "x@x.com", "name": "X"},
            )

        assert len(exc_info.value.violations) == 2

    async def test_compute_sees_updated_command_in_rules(self, executor: RuntimeExecutor) -> None:
        """Rules run after computes — they see the enriched command."""
        seen_in_rule: list[str] = []

        def capture_rule(cmd: Any, fs: Any) -> None:
            seen_in_rule.append(cmd.email)

        class EnrichAndCheckUseCase(UseCase[Any, None]):
            computes = [lambda cmd, fs: type(cmd)(email="enriched@corp.com", name=cmd.name)]
            rules = [capture_rule]

            async def execute(self, cmd: AcmeCreateUserCommand = Input()) -> None:
                pass

        await executor.execute(
            EnrichAndCheckUseCase(),
            payload={"email": "original@corp.com", "name": "X"},
        )

        assert seen_in_rule == ["enriched@corp.com"]


class TestMcpMarker:
    """``Mcp()`` resolves through the MCP ``ExecutionResolver`` the AI pillar binds.

    The caller can never pass the handle as a parameter.
    """

    async def test_the_resolved_handle_is_bound_to_the_verified_caller(
        self, mcp_executor: RuntimeExecutor, mcp_calls: McpCalls
    ) -> None:
        subject, server = await mcp_executor.execute(AcmeDocsSearchUseCase(), identity=ALICE)

        assert subject == "acme-alice"
        assert server == "docs-server"
        assert mcp_calls == [("docs-server", ("search",), ALICE)]

    async def test_several_mcp_markers_each_resolve_their_own_server(
        self, mcp_executor: RuntimeExecutor, mcp_calls: McpCalls
    ) -> None:
        result = await mcp_executor.execute(AcmeTwoServersUseCase(), identity=ALICE)

        assert result == "docs-server:billing-server"
        assert mcp_calls == [
            ("docs-server", ("search",), ALICE),
            ("billing-server", ("lookup",), ALICE),
        ]

    async def test_an_anonymous_identity_still_resolves_a_handle(
        self, mcp_executor: RuntimeExecutor, mcp_calls: McpCalls
    ) -> None:
        """Binding succeeds even for ANONYMOUS: the handle itself refuses it, not the executor."""
        subject, _ = await mcp_executor.execute(AcmeDocsSearchUseCase(), identity=ANONYMOUS)

        assert subject == ""
        assert mcp_calls == [("docs-server", ("search",), ANONYMOUS)]

    async def test_a_missing_identity_is_refused_instead_of_defaulted(
        self, mcp_executor: RuntimeExecutor
    ) -> None:
        use_case = AcmeDocsSearchUseCase()

        with pytest.raises(
            MissingCaller, match=r"AcmeDocsSearchUseCase.execute declares 'caller = Caller\(\)'"
        ):
            await mcp_executor.execute(use_case)

    async def test_a_missing_identity_is_refused_by_the_mcp_step_itself(
        self, mcp_executor: RuntimeExecutor, mcp_calls: McpCalls
    ) -> None:
        """Pins the ``Mcp()`` step's own guard, unshadowed by the ``Caller()`` step's.

        ``AcmeDocsSearchUseCase`` also declares ``Caller()``, so its own missing-identity
        test above is refused before the ``Mcp()`` step runs at all.
        ``AcmeMcpOnlyUseCase`` declares no ``Caller()``, so this is the one that reaches it.
        """
        use_case = AcmeMcpOnlyUseCase()

        with pytest.raises(
            MissingCaller, match=r"AcmeMcpOnlyUseCase.execute declares 'docs = Mcp\(\)'"
        ) as refused:
            await mcp_executor.execute(use_case)

        assert (refused.value.parameter, refused.value.marker) == ("docs", "Mcp()")
        assert refused.value.use_case.endswith("AcmeMcpOnlyUseCase")
        assert mcp_calls == []

    async def test_no_resolver_bound_fails_with_a_named_error(
        self, executor: RuntimeExecutor
    ) -> None:
        use_case = AcmeDocsSearchUseCase()

        with pytest.raises(RuntimeError, match="bind_mcp_resolver"):
            await executor.execute(use_case, identity=ALICE)

    async def test_binding_a_resolver_twice_is_refused(self, mcp_executor: RuntimeExecutor) -> None:
        second_resolver = mcp_resolver([])

        with pytest.raises(RuntimeError):
            mcp_executor.bind_mcp_resolver(second_resolver)

    async def test_a_use_case_with_no_mcp_marker_never_calls_the_resolver(
        self, mcp_executor: RuntimeExecutor, mcp_calls: McpCalls
    ) -> None:
        result = await mcp_executor.execute(AcmeNoMcpUseCase(), identity=ALICE)

        assert result == "no servers here"
        assert mcp_calls == []

    async def test_a_use_case_with_no_mcp_marker_needs_no_identity_either(
        self, mcp_executor: RuntimeExecutor
    ) -> None:
        """A plan with no ``Mcp()`` compiles no ``Mcp()`` step, so it demands no identity."""
        result = await mcp_executor.execute(AcmeNoMcpUseCase())

        assert result == "no servers here"


class TestStepTable:
    """Each plan is compiled once into its steps; a plan replaced on the class is recompiled."""

    async def test_a_plan_replaced_on_the_class_is_the_one_that_runs(
        self, executor: RuntimeExecutor
    ) -> None:
        class _Replaced(UseCase[Any, str]):
            async def execute(self, value: int) -> str:
                return f"value={value}"

        plan = UseCaseCompiler().compile(_Replaced)
        assert await executor.execute(_Replaced(), params={"value": 1}) == "value=1"
        extra = (*plan.param_bindings, ParamBinding("other", int))
        _Replaced.__execution_plan__ = replace(plan, param_bindings=extra)  # type: ignore[attr-defined]

        use_case = _Replaced()

        with pytest.raises(ValueError, match="missing required parameter 'other'"):
            await executor.execute(use_case, params={"value": 1})
