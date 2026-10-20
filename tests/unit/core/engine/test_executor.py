from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import ParameterBindingError, RuntimeExecutor
from loom.core.errors import NotFound, RuleViolation, RuleViolations
from loom.core.identity import ANONYMOUS
from loom.core.use_case.markers import Exists, Input, Load, LoadById, OnMissing
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class CreateUserCommand(Command, frozen=True):
    email: str
    name: str


class UpdateUserCommand(Command, frozen=True):
    email: str


class User:
    def __init__(self, id: int, email: str) -> None:
        self.id = id
        self.email = email


class UserResult:
    def __init__(self, id: int) -> None:
        self.id = id


# ---------------------------------------------------------------------------
# Recording logger
# ---------------------------------------------------------------------------


class _RecordingLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def bind(self, **fields: Any) -> _RecordingLogger:
        return self

    def debug(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def info(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def warning(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def error(self, event: str, **fields: Any) -> None:
        self.messages.append(event)

    def exception(self, event: str, **fields: Any) -> None:
        self.messages.append(event)


# ---------------------------------------------------------------------------
# UseCase fixtures
# ---------------------------------------------------------------------------


class _NoMarkersUseCase(UseCase[Any, str]):
    async def execute(self, value: int) -> str:
        return f"value={value}"


class _InputOnlyUseCase(UseCase[Any, str]):
    async def execute(self, cmd: CreateUserCommand = Input()) -> str:
        return cmd.email


class _RenamedInputStruct(Command, frozen=True, rename="camel"):
    price_cents: int
    stock: int


class _RenamedInputUseCase(UseCase[Any, tuple[int, frozenset[str]]]):
    async def execute(self, cmd: _RenamedInputStruct = Input()) -> tuple[int, frozenset[str]]:
        return cmd.price_cents, frozenset({"price_cents", "stock"})


class _ParamsAndInputUseCase(UseCase[Any, UserResult]):
    async def execute(
        self,
        tenant_id: str,
        cmd: CreateUserCommand = Input(),
    ) -> UserResult:
        return UserResult(id=99)


class _WithLoadUseCase(UseCase[Any, str]):
    async def execute(
        self,
        user_id: int,
        user: User = LoadById(User, by="user_id"),
    ) -> str:
        return user.email


class _WithLoadByFieldUseCase(UseCase[Any, str]):
    async def execute(
        self,
        email: str,
        user: User = Load(User, from_param="email", against="email"),
    ) -> str:
        return user.email


class _WithLoadFromCommandUseCase(UseCase[Any, str]):
    async def execute(
        self,
        cmd: CreateUserCommand = Input(),
        user: User = Load(User, from_command="email", against="email"),
    ) -> str:
        return user.email


class _WithExistsUseCase(UseCase[Any, bool]):
    async def execute(
        self,
        email: str,
        user_exists: bool = Exists(User, from_param="email", against="email"),
    ) -> bool:
        return user_exists


class _WithExistsRaiseUseCase(UseCase[Any, bool]):
    async def execute(
        self,
        email: str,
        user_exists: bool = Exists(
            User,
            from_param="email",
            against="email",
            on_missing=OnMissing.RAISE,
        ),
    ) -> bool:
        return user_exists


class _ComputeAndRuleUseCase(UseCase[Any, str]):
    computes = [
        lambda cmd, fs: type(cmd)(  # uppercase email
            email=cmd.email.upper(), name=cmd.name
        )
    ]
    rules = [
        lambda cmd, fs: (
            (_ for _ in ()).throw(RuleViolation("email", "bad"))
            if cmd.email == "BANNED@CORP.COM"
            else None
        )
    ]

    async def execute(self, cmd: CreateUserCommand = Input()) -> str:
        return cmd.email


class _FullPipelineUseCase(UseCase[Any, UserResult]):
    def __init__(self) -> None:
        self.received_user: User | None = None
        self.received_cmd: CreateUserCommand | None = None

    async def execute(
        self,
        user_id: int,
        cmd: CreateUserCommand = Input(),
        user: User = LoadById(User, by="user_id"),
    ) -> UserResult:
        self.received_user = user
        self.received_cmd = cmd
        return UserResult(id=user_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(
    *,
    debug: bool = False,
    logger: _RecordingLogger | None = None,
) -> RuntimeExecutor:
    compiler = UseCaseCompiler()
    return RuntimeExecutor(
        compiler,
        debug_execution=debug,
        logger=logger or _RecordingLogger(),
    )


def _fake_repo(entity: Any | None) -> Any:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=entity)
    repo.get_by = AsyncMock(return_value=entity)
    repo.exists_by = AsyncMock(return_value=entity is not None)
    return repo


# ---------------------------------------------------------------------------
# Tests: no markers
# ---------------------------------------------------------------------------


class TestExecuteNoMarkers:
    async def test_executes_and_returns_result(self) -> None:
        ex = _make_executor()
        result = await ex.execute(
            _NoMarkersUseCase(),
            params={"value": 42},
        )
        assert result == "value=42"

    async def test_missing_param_raises(self) -> None:
        ex = _make_executor()
        use_case = _NoMarkersUseCase()
        with pytest.raises(ValueError, match="missing required parameter"):
            await ex.execute(use_case, params={})

    async def test_invalid_param_type_raises_binding_error(self) -> None:
        ex = _make_executor()
        use_case = _NoMarkersUseCase()
        with pytest.raises(ParameterBindingError, match="invalid value for parameter 'value'"):
            await ex.execute(use_case, params={"value": "not-an-int"})


# ---------------------------------------------------------------------------
# Tests: Input binding
# ---------------------------------------------------------------------------


class TestExecuteWithInput:
    async def test_command_built_from_payload(self) -> None:
        ex = _make_executor()
        result = await ex.execute(
            _InputOnlyUseCase(),
            payload={"email": "alice@corp.com", "name": "Alice"},
        )
        assert result == "alice@corp.com"

    async def test_missing_payload_raises(self) -> None:
        ex = _make_executor()
        use_case = _InputOnlyUseCase()
        with pytest.raises(ValueError, match="payload is required"):
            await ex.execute(use_case)

    async def test_params_and_input_together(self) -> None:
        ex = _make_executor()
        result = await ex.execute(
            _ParamsAndInputUseCase(),
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
        payload: dict[str, int],
        expected_price: int,
    ) -> None:
        ex = _make_executor()
        price, _ = await ex.execute(_RenamedInputUseCase(), payload=payload)
        assert price == expected_price


# ---------------------------------------------------------------------------
# Tests: LoadById steps
# ---------------------------------------------------------------------------


class TestExecuteWithLoad:
    async def test_entity_loaded_via_repo(self) -> None:
        user = User(id=1, email="loaded@corp.com")
        repo = _fake_repo(user)
        ex = _make_executor()

        result = await ex.execute(
            _WithLoadUseCase(),
            params={"user_id": 1},
            dependencies={User: repo},
        )

        repo.get_by_id.assert_awaited_once_with(1, profile="default")
        assert result == "loaded@corp.com"

    async def test_entity_loaded_by_field_via_repo(self) -> None:
        user = User(id=1, email="loaded@corp.com")
        repo = _fake_repo(user)
        ex = _make_executor()

        result = await ex.execute(
            _WithLoadByFieldUseCase(),
            params={"email": "loaded@corp.com"},
            dependencies={User: repo},
        )

        repo.get_by.assert_awaited_once_with("email", "loaded@corp.com", profile="default")
        assert result == "loaded@corp.com"

    async def test_entity_loaded_by_command_field_via_repo(self) -> None:
        user = User(id=1, email="loaded@corp.com")
        repo = _fake_repo(user)
        ex = _make_executor()

        result = await ex.execute(
            _WithLoadFromCommandUseCase(),
            payload={"email": "loaded@corp.com", "name": "Alice"},
            dependencies={User: repo},
        )

        repo.get_by.assert_awaited_once_with("email", "loaded@corp.com", profile="default")
        assert result == "loaded@corp.com"

    async def test_load_override_skips_repo(self) -> None:
        user = User(id=7, email="override@corp.com")
        repo = _fake_repo(None)
        ex = _make_executor()

        result = await ex.execute(
            _WithLoadUseCase(),
            params={"user_id": 7},
            dependencies={User: repo},
            load_overrides={User: user},
        )

        repo.get_by_id.assert_not_awaited()
        assert result == "override@corp.com"

    async def test_missing_entity_raises_not_found(self) -> None:
        repo = _fake_repo(None)
        ex = _make_executor()

        use_case = _WithLoadUseCase()
        with pytest.raises(NotFound) as exc_info:
            await ex.execute(
                use_case,
                params={"user_id": 99},
                dependencies={User: repo},
            )

        assert exc_info.value.entity == "User"
        assert exc_info.value.id == 99

    async def test_no_dependencies_raises(self) -> None:
        ex = _make_executor()
        use_case = _WithLoadUseCase()
        with pytest.raises(RuntimeError, match="No dependencies provided"):
            await ex.execute(
                use_case,
                params={"user_id": 1},
            )

    async def test_missing_repo_type_raises(self) -> None:
        ex = _make_executor()
        use_case = _WithLoadUseCase()
        with pytest.raises(RuntimeError, match="No repository registered"):
            await ex.execute(
                use_case,
                params={"user_id": 1},
                dependencies={},
            )

    async def test_repo_resolver_error_is_not_silenced(self) -> None:
        compiler = UseCaseCompiler()

        def _resolver(_: type[Any]) -> Any:
            raise RuntimeError("resolver exploded")

        ex = RuntimeExecutor(
            compiler,
            logger=_RecordingLogger(),
            repo_resolver=_resolver,
        )
        use_case = _WithLoadUseCase()
        with pytest.raises(RuntimeError, match="resolver exploded"):
            await ex.execute(use_case, params={"user_id": 1})


class TestExecuteWithExists:
    async def test_exists_true_when_entity_is_present(self) -> None:
        repo = _fake_repo(User(id=1, email="ok@corp.com"))
        ex = _make_executor()

        result = await ex.execute(
            _WithExistsUseCase(),
            params={"email": "ok@corp.com"},
            dependencies={User: repo},
        )

        repo.exists_by.assert_awaited_once_with("email", "ok@corp.com")
        assert result is True

    async def test_exists_false_when_entity_is_missing(self) -> None:
        repo = _fake_repo(None)
        ex = _make_executor()

        result = await ex.execute(
            _WithExistsUseCase(),
            params={"email": "missing@corp.com"},
            dependencies={User: repo},
        )

        assert result is False

    async def test_exists_raise_on_missing(self) -> None:
        repo = _fake_repo(None)
        ex = _make_executor()

        use_case = _WithExistsRaiseUseCase()
        with pytest.raises(NotFound):
            await ex.execute(
                use_case,
                params={"email": "missing@corp.com"},
                dependencies={User: repo},
            )


# ---------------------------------------------------------------------------
# Tests: full pipeline
# ---------------------------------------------------------------------------


class TestExecuteFullPipeline:
    async def test_kwargs_injected_correctly(self) -> None:
        user = User(id=5, email="user@corp.com")
        uc = _FullPipelineUseCase()
        ex = _make_executor()

        result = await ex.execute(
            uc,
            params={"user_id": 5},
            payload={"email": "new@corp.com", "name": "Alice"},
            dependencies={User: _fake_repo(user)},
        )

        assert result.id == 5
        assert uc.received_user is user
        assert uc.received_cmd is not None
        assert uc.received_cmd.email == "new@corp.com"

    async def test_compute_applied_before_execute(self) -> None:
        received: list[str] = []

        class _TrackedUseCase(UseCase[Any, None]):
            computes = [lambda cmd, fs: type(cmd)(email="computed@corp.com", name=cmd.name)]

            async def execute(self, cmd: CreateUserCommand = Input()) -> None:
                received.append(cmd.email)

        ex = _make_executor()
        await ex.execute(
            _TrackedUseCase(),
            payload={"email": "original@corp.com", "name": "Alice"},
        )
        assert received == ["computed@corp.com"]

    async def test_rule_violation_raises_before_execute(self) -> None:
        called: list[bool] = []

        def bad_rule(cmd: Any, fs: Any) -> None:
            raise RuleViolation("email", "invalid")

        class _RuleUseCase(UseCase[Any, None]):
            rules = [bad_rule]

            async def execute(self, cmd: CreateUserCommand = Input()) -> None:
                called.append(True)

        ex = _make_executor()
        use_case = _RuleUseCase()
        with pytest.raises(RuleViolations) as exc_info:
            await ex.execute(
                use_case,
                payload={"email": "bad@corp.com", "name": "X"},
            )

        assert len(exc_info.value.violations) == 1
        assert exc_info.value.violations[0].field == "email"
        assert called == []

    async def test_rule_violations_accumulated(self) -> None:
        def rule_a(cmd: Any, fs: Any) -> None:
            raise RuleViolation("email", "bad email")

        def rule_b(cmd: Any, fs: Any) -> None:
            raise RuleViolation("name", "bad name")

        class _MultiRuleUseCase(UseCase[Any, None]):
            rules = [rule_a, rule_b]

            async def execute(self, cmd: CreateUserCommand = Input()) -> None:
                pass

        ex = _make_executor()
        use_case = _MultiRuleUseCase()
        with pytest.raises(RuleViolations) as exc_info:
            await ex.execute(
                use_case,
                payload={"email": "x@x.com", "name": "X"},
            )

        assert len(exc_info.value.violations) == 2

    async def test_compute_sees_updated_command_in_rules(self) -> None:
        """Rules run after computes — they see the enriched command."""
        seen_in_rule: list[str] = []

        def capture_rule(cmd: Any, fs: Any) -> None:
            seen_in_rule.append(cmd.email)

        class _EnrichAndCheckUseCase(UseCase[Any, None]):
            computes = [lambda cmd, fs: type(cmd)(email="enriched@corp.com", name=cmd.name)]
            rules = [capture_rule]

            async def execute(self, cmd: CreateUserCommand = Input()) -> None:
                pass

        ex = _make_executor()
        await ex.execute(
            _EnrichAndCheckUseCase(),
            payload={"email": "original@corp.com", "name": "X"},
        )

        assert seen_in_rule == ["enriched@corp.com"]


# ---------------------------------------------------------------------------
# Tests: no re-compilation at runtime
# ---------------------------------------------------------------------------


class TestNoRecompilation:
    async def test_compiler_compile_called_once_per_class(self) -> None:
        # Use a locally-defined class so __execution_plan__ is always None
        # at the start of this test, regardless of other tests' execution order.
        class _FreshUseCase(UseCase[Any, str]):
            async def execute(self, value: int) -> str:
                return f"value={value}"

        compiler = UseCaseCompiler()
        ex = RuntimeExecutor(compiler, logger=_RecordingLogger())

        # Execute twice — _build_plan should only be called once
        with patch.object(compiler, "_build_plan", wraps=compiler._build_plan) as mock_build:
            await ex.execute(_FreshUseCase(), params={"value": 1})
            await ex.execute(_FreshUseCase(), params={"value": 2})

        mock_build.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: logging
# ---------------------------------------------------------------------------


class TestExecutorLogging:
    async def test_always_logs_exec(self) -> None:
        log = _RecordingLogger()
        ex = _make_executor(logger=log)
        await ex.execute(_NoMarkersUseCase(), params={"value": 1})
        assert any("[EXEC]" in m for m in log.messages)

    async def test_always_logs_done_with_ms(self) -> None:
        log = _RecordingLogger()
        ex = _make_executor(logger=log)
        await ex.execute(_NoMarkersUseCase(), params={"value": 1})
        assert any("[DONE]" in m and "ms" in m for m in log.messages)

    async def test_debug_false_no_step_logs(self) -> None:
        log = _RecordingLogger()
        ex = _make_executor(debug=False, logger=log)
        await ex.execute(
            _InputOnlyUseCase(),
            payload={"email": "a@b.com", "name": "X"},
        )
        assert not any("[STEP]" in m for m in log.messages)

    async def test_debug_true_emits_step_logs(self) -> None:
        log = _RecordingLogger()
        ex = _make_executor(debug=True, logger=log)
        await ex.execute(
            _InputOnlyUseCase(),
            payload={"email": "a@b.com", "name": "X"},
        )
        assert any("[STEP]" in m for m in log.messages)

    async def test_debug_true_emits_bind_input(self) -> None:
        log = _RecordingLogger()
        ex = _make_executor(debug=True, logger=log)
        await ex.execute(
            _InputOnlyUseCase(),
            payload={"email": "a@b.com", "name": "X"},
        )
        assert any("Bind Input" in m for m in log.messages)

    async def test_debug_true_emits_load_step(self) -> None:
        log = _RecordingLogger()
        ex = _make_executor(debug=True, logger=log)
        user = User(id=1, email="u@u.com")
        await ex.execute(
            _WithLoadUseCase(),
            params={"user_id": 1},
            dependencies={User: _fake_repo(user)},
        )
        assert any("Load User" in m for m in log.messages)

    async def test_rule_failure_always_logged_as_warning(self) -> None:
        def bad_rule(cmd: Any, fs: Any) -> None:
            raise RuleViolation("email", "bad")

        class _RuleUseCase(UseCase[Any, None]):
            rules = [bad_rule]

            async def execute(self, cmd: CreateUserCommand = Input()) -> None:
                pass

        log = _RecordingLogger()
        ex = _make_executor(debug=False, logger=log)

        use_case = _RuleUseCase()
        with pytest.raises(RuleViolations):
            await ex.execute(
                use_case,
                payload={"email": "x@x.com", "name": "X"},
            )

        assert any("[RULE]" in m for m in log.messages)


# ---------------------------------------------------------------------------
# Tests: parallel load / exists execution
# ---------------------------------------------------------------------------


class Order:
    def __init__(self, id: int) -> None:
        self.id = id


class _MultiLoadUseCase(UseCase[Any, tuple[str, int]]):
    """Two independent LoadById steps on different entity types."""

    async def execute(
        self,
        user_id: int,
        order_id: int,
        user: User = LoadById(User, by="user_id"),
        order: Order = LoadById(Order, by="order_id"),
    ) -> tuple[str, int]:
        return user.email, order.id


class _MultiExistsUseCase(UseCase[Any, tuple[bool, bool]]):
    """Two independent Exists steps on different entity types."""

    async def execute(
        self,
        email: str,
        order_id: int,
        user_exists: bool = Exists(User, from_param="email", against="email"),
        order_exists: bool = Exists(Order, from_param="order_id", against="id"),
    ) -> tuple[bool, bool]:
        return user_exists, order_exists


@pytest.mark.asyncio
class TestParallelLoads:
    async def test_two_independent_loads_both_resolved(self) -> None:
        """Both LoadById steps resolve their entities regardless of order."""
        user = User(id=1, email="a@b.com")
        order = Order(id=99)
        user_repo = _fake_repo(user)
        order_repo = _fake_repo(order)
        ex = _make_executor()

        result = await ex.execute(
            _MultiLoadUseCase(),
            params={"user_id": 1, "order_id": 99},
            dependencies={User: user_repo, Order: order_repo},
        )

        assert result == ("a@b.com", 99)
        user_repo.get_by_id.assert_awaited_once_with(1, profile="default")
        order_repo.get_by_id.assert_awaited_once_with(99, profile="default")

    async def test_two_independent_loads_first_missing_raises(self) -> None:
        """NotFound from one load propagates even when other load would succeed."""
        order = Order(id=5)
        user_repo = _fake_repo(None)  # user not found
        order_repo = _fake_repo(order)
        ex = _make_executor()

        use_case = _MultiLoadUseCase()
        with pytest.raises(NotFound):
            await ex.execute(
                use_case,
                params={"user_id": 1, "order_id": 5},
                dependencies={User: user_repo, Order: order_repo},
            )

    async def test_two_independent_exists_both_resolved(self) -> None:
        """Both Exists steps resolve and results are bound correctly."""
        user = User(id=1, email="x@y.com")
        order = Order(id=7)
        user_repo = _fake_repo(user)
        order_repo = _fake_repo(order)
        ex = _make_executor()

        result = await ex.execute(
            _MultiExistsUseCase(),
            params={"email": "x@y.com", "order_id": 7},
            dependencies={User: user_repo, Order: order_repo},
        )

        assert result == (True, True)

    async def test_gather_calls_repos_concurrently(self) -> None:
        """Verify both repos are called — concurrent or sequential, results correct."""
        import asyncio

        call_order: list[str] = []

        async def slow_user(obj_id: Any, *, profile: str = "default") -> User:
            await asyncio.sleep(0)
            call_order.append("user")
            return User(id=obj_id, email="u@u.com")

        async def slow_order(obj_id: Any, *, profile: str = "default") -> Order:
            await asyncio.sleep(0)
            call_order.append("order")
            return Order(id=obj_id)

        user_repo = AsyncMock()
        user_repo.get_by_id = slow_user
        order_repo = AsyncMock()
        order_repo.get_by_id = slow_order

        ex = _make_executor()
        result = await ex.execute(
            _MultiLoadUseCase(),
            params={"user_id": 1, "order_id": 2},
            dependencies={User: user_repo, Order: order_repo},
        )

        assert result == ("u@u.com", 2)
        assert set(call_order) == {"user", "order"}


# ---------------------------------------------------------------------------
# run() — build through the factory, then execute
# ---------------------------------------------------------------------------


class TestRun:
    """A7: ``run`` is the entry point of hosts that build through a factory."""

    async def test_builds_the_use_case_through_the_factory_and_forwards_every_argument(
        self,
    ) -> None:
        instance = _NoMarkersUseCase()
        factory = MagicMock()
        factory.build = MagicMock(return_value=instance)
        executor = _make_executor()
        identity = ANONYMOUS
        user_repo = AsyncMock()
        overrides = {User: User(id=1, email="u@u.com")}

        with patch.object(
            RuntimeExecutor, "execute", new=AsyncMock(return_value="built")
        ) as execute:
            result = await executor.run(
                _NoMarkersUseCase,
                factory=factory,
                params={"user_id": 1},
                payload={"email": "a@b.com"},
                dependencies={User: user_repo},
                load_overrides=overrides,
                read_only=True,
                identity=identity,
            )

        assert result == "built"
        factory.build.assert_called_once_with(_NoMarkersUseCase)
        execute.assert_awaited_once_with(
            instance,
            params={"user_id": 1},
            payload={"email": "a@b.com"},
            dependencies={User: user_repo},
            load_overrides=overrides,
            read_only=True,
            identity=identity,
        )

    async def test_a_build_failure_surfaces_before_any_execution(self) -> None:
        factory = MagicMock()
        factory.build = MagicMock(side_effect=RuntimeError("no dependency"))
        executor = _make_executor()

        with (
            patch.object(RuntimeExecutor, "execute", new=AsyncMock()) as execute,
            pytest.raises(RuntimeError, match="no dependency"),
        ):
            await executor.run(_NoMarkersUseCase, factory=factory)

        execute.assert_not_awaited()
