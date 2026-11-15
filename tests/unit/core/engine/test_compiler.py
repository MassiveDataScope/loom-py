from __future__ import annotations

from typing import Any

import pytest

from loom.core.command import Command
from loom.core.engine.compiler import CompilationError, UseCaseCompiler
from loom.core.engine.plan import ExecutionPlan
from loom.core.use_case.markers import Exists, Input, Load, LoadById, SourceKind
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Test domain fixtures
# ---------------------------------------------------------------------------


class UserCommand(Command, frozen=True):
    email: str


class User:
    pass


class Product:
    pass


# ---------------------------------------------------------------------------
# Recording logger
# ---------------------------------------------------------------------------


class _RecordingLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    @property
    def messages(self) -> list[str]:
        return [event for event, _ in self.calls]

    def bind(self, **fields: Any) -> _RecordingLogger:
        return self

    def debug(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def info(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def exception(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))


# ---------------------------------------------------------------------------
# Test compute / rule helpers
# ---------------------------------------------------------------------------


def _noop_compute(cmd: object, fields_set: frozenset[str]) -> object:
    return cmd


def _noop_rule(cmd: object, fields_set: frozenset[str]) -> None:
    pass


# ---------------------------------------------------------------------------
# UseCase fixtures
# ---------------------------------------------------------------------------


class _NoMarkersUseCase(UseCase[Any, str]):
    async def execute(self, user_id: int) -> str:
        return "ok"


class _NoParamsUseCase(UseCase[Any, str]):
    async def execute(self) -> str:
        return "ok"


class _WithInputUseCase(UseCase[Any, str]):
    async def execute(self, cmd: UserCommand = Input()) -> str:
        return "ok"


class _InputWithoutFromPayloadUseCase(UseCase[Any, str]):
    async def execute(self, cmd: int = Input()) -> str:
        return f"{cmd}"


class _WithLoadUseCase(UseCase[Any, str]):
    async def execute(
        self,
        user_id: int,
        user: User = LoadById(User, by="user_id"),
    ) -> str:
        return "ok"


class _WithLoadByFieldUseCase(UseCase[Any, str]):
    async def execute(
        self,
        email: str,
        user: User = Load(User, from_param="email", against="email"),
    ) -> str:
        return "ok"


class _WithLoadFromCommandUseCase(UseCase[Any, str]):
    async def execute(
        self,
        cmd: UserCommand = Input(),
        user: User = Load(User, from_command="email", against="email"),
    ) -> str:
        return "ok"


class _WithExistsUseCase(UseCase[Any, bool]):
    async def execute(
        self,
        email: str,
        user_exists: bool = Exists(User, from_param="email", against="email"),
    ) -> bool:
        return user_exists


class _WithAllUseCase(UseCase[Any, str]):
    computes = [_noop_compute]
    rules = [_noop_rule]

    async def execute(
        self,
        user_id: int,
        cmd: UserCommand = Input(),
        user: User = LoadById(User, by="user_id"),
    ) -> str:
        return "ok"


class _MultipleLoadsUseCase(UseCase[Any, str]):
    async def execute(
        self,
        user_id: int,
        product_id: int,
        user: User = LoadById(User, by="user_id"),
        product: Product = LoadById(Product, by="product_id"),
    ) -> str:
        return "ok"


# ---------------------------------------------------------------------------
# Compilation: no markers
# ---------------------------------------------------------------------------


class TestCompileNoMarkers:
    def test_produces_execution_plan(self) -> None:
        plan = UseCaseCompiler().compile(_NoMarkersUseCase)
        assert isinstance(plan, ExecutionPlan)
        assert plan.use_case_type is _NoMarkersUseCase

    def test_extracts_primitive_param(self) -> None:
        plan = UseCaseCompiler().compile(_NoMarkersUseCase)
        assert len(plan.param_bindings) == 1
        assert plan.param_bindings[0].name == "user_id"
        assert plan.param_bindings[0].annotation is int

    def test_no_input_binding(self) -> None:
        plan = UseCaseCompiler().compile(_NoMarkersUseCase)
        assert plan.input_binding is None

    def test_no_load_steps(self) -> None:
        plan = UseCaseCompiler().compile(_NoMarkersUseCase)
        assert plan.load_steps == ()
        assert plan.exists_steps == ()

    def test_empty_pipeline_steps(self) -> None:
        plan = UseCaseCompiler().compile(_NoMarkersUseCase)
        assert plan.compute_steps == ()
        assert plan.rule_steps == ()

    def test_no_params_usecase(self) -> None:
        plan = UseCaseCompiler().compile(_NoParamsUseCase)
        assert plan.param_bindings == ()
        assert plan.input_binding is None
        assert plan.load_steps == ()


# ---------------------------------------------------------------------------
# Compilation: Input marker
# ---------------------------------------------------------------------------


class TestCompileWithInput:
    def test_detects_input_binding(self) -> None:
        plan = UseCaseCompiler().compile(_WithInputUseCase)
        assert plan.input_binding is not None
        assert plan.input_binding.name == "cmd"
        assert plan.input_binding.command_type is UserCommand

    def test_input_not_in_param_bindings(self) -> None:
        plan = UseCaseCompiler().compile(_WithInputUseCase)
        names = [pb.name for pb in plan.param_bindings]
        assert "cmd" not in names

    def test_input_type_must_implement_from_payload(self) -> None:
        compiler = UseCaseCompiler()
        with pytest.raises(CompilationError, match="must implement from_payload"):
            compiler.compile(_InputWithoutFromPayloadUseCase)


# ---------------------------------------------------------------------------
# Compilation: LoadById marker
# ---------------------------------------------------------------------------


class TestCompileWithLoad:
    def test_detects_load_step(self) -> None:
        plan = UseCaseCompiler().compile(_WithLoadUseCase)
        assert len(plan.load_steps) == 1
        ls = plan.load_steps[0]
        assert ls.name == "user"
        assert ls.entity_type is User
        assert ls.source_kind is SourceKind.PARAM
        assert ls.source_name == "user_id"

    def test_load_not_in_param_bindings(self) -> None:
        plan = UseCaseCompiler().compile(_WithLoadUseCase)
        names = [pb.name for pb in plan.param_bindings]
        assert "user" not in names

    def test_multiple_loads(self) -> None:
        plan = UseCaseCompiler().compile(_MultipleLoadsUseCase)
        assert len(plan.load_steps) == 2
        names = {ls.name for ls in plan.load_steps}
        assert names == {"user", "product"}

    def test_multiple_loads_by_refs(self) -> None:
        plan = UseCaseCompiler().compile(_MultipleLoadsUseCase)
        by_map = {ls.name: ls.source_name for ls in plan.load_steps}
        assert by_map["user"] == "user_id"
        assert by_map["product"] == "product_id"

    def test_detects_load_by_field(self) -> None:
        plan = UseCaseCompiler().compile(_WithLoadByFieldUseCase)
        assert len(plan.load_steps) == 1
        ls = plan.load_steps[0]
        assert ls.source_kind is SourceKind.PARAM
        assert ls.source_name == "email"
        assert ls.against == "email"

    def test_detects_load_from_command(self) -> None:
        plan = UseCaseCompiler().compile(_WithLoadFromCommandUseCase)
        assert len(plan.load_steps) == 1
        ls = plan.load_steps[0]
        assert ls.source_kind is SourceKind.COMMAND
        assert ls.source_name == "email"
        assert ls.against == "email"


class TestCompileWithExists:
    def test_detects_exists_step(self) -> None:
        plan = UseCaseCompiler().compile(_WithExistsUseCase)
        assert len(plan.exists_steps) == 1
        es = plan.exists_steps[0]
        assert es.source_kind is SourceKind.PARAM
        assert es.source_name == "email"
        assert es.against == "email"


# ---------------------------------------------------------------------------
# Compilation: full plan (all markers + pipeline)
# ---------------------------------------------------------------------------


class TestCompileFullPlan:
    def test_all_sections_populated(self) -> None:
        plan = UseCaseCompiler().compile(_WithAllUseCase)
        assert len(plan.param_bindings) == 1
        assert plan.input_binding is not None
        assert len(plan.load_steps) == 1
        assert len(plan.exists_steps) == 0
        assert len(plan.compute_steps) == 1
        assert len(plan.rule_steps) == 1

    def test_compute_fn_reference_preserved(self) -> None:
        plan = UseCaseCompiler().compile(_WithAllUseCase)
        assert plan.compute_steps[0].fn is _noop_compute

    def test_rule_fn_reference_preserved(self) -> None:
        plan = UseCaseCompiler().compile(_WithAllUseCase)
        assert plan.rule_steps[0].fn is _noop_rule


# ---------------------------------------------------------------------------
# Cache behaviour
# ---------------------------------------------------------------------------


class TestCompilerCache:
    def test_same_plan_instance_on_second_call(self) -> None:
        compiler = UseCaseCompiler()
        plan1 = compiler.compile(_NoMarkersUseCase)
        plan2 = compiler.compile(_NoMarkersUseCase)
        assert plan1 is plan2

    def test_get_plan_returns_none_before_compile(self) -> None:
        compiler = UseCaseCompiler()
        assert compiler.get_plan(_NoMarkersUseCase) is None

    def test_get_plan_returns_plan_after_compile(self) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(_NoMarkersUseCase)
        assert compiler.get_plan(_NoMarkersUseCase) is not None

    def test_different_classes_get_independent_plans(self) -> None:
        compiler = UseCaseCompiler()
        plan1 = compiler.compile(_NoMarkersUseCase)
        plan2 = compiler.compile(_WithInputUseCase)
        assert plan1 is not plan2


# ---------------------------------------------------------------------------
# Validation failures (fail-fast)
# ---------------------------------------------------------------------------


class TestCompilationErrors:
    def test_two_input_markers_raises(self) -> None:
        class _TwoInputs(UseCase[Any, str]):
            async def execute(
                self,
                a: UserCommand = Input(),
                b: UserCommand = Input(),
            ) -> str:
                return "ok"

        compiler = UseCaseCompiler()
        with pytest.raises(CompilationError, match="only one Input"):
            compiler.compile(_TwoInputs)

    def test_load_by_unknown_param_raises(self) -> None:
        class _BadLoad(UseCase[Any, str]):
            async def execute(
                self,
                user: User = LoadById(User, by="nonexistent"),
            ) -> str:
                return "ok"

        compiler = UseCaseCompiler()
        with pytest.raises(CompilationError, match="nonexistent"):
            compiler.compile(_BadLoad)

    def test_abstract_execute_raises(self) -> None:
        compiler = UseCaseCompiler()
        with pytest.raises(CompilationError, match="must override execute"):
            compiler.compile(UseCase)  # type: ignore[type-abstract]

    def test_load_by_references_input_param_raises(self) -> None:
        """Input binding params are not primitive params — LoadById cannot ref them."""

        class _BadRef(UseCase[Any, str]):
            async def execute(
                self,
                cmd: UserCommand = Input(),
                user: User = LoadById(User, by="cmd"),
            ) -> str:
                return "ok"

        compiler = UseCaseCompiler()
        with pytest.raises(CompilationError, match="'cmd' not found"):
            compiler.compile(_BadRef)

    @pytest.mark.parametrize(
        ("marker", "wording"),
        [
            pytest.param(LoadById(User, by="missing"), r"LoadById\(\) User", id="load-by-id"),
            pytest.param(
                Load(User, from_param="missing", against="email"), r": Load\(\) User", id="load"
            ),
            pytest.param(
                Exists(User, from_param="missing", against="email"), r"Exists\(\) User", id="exists"
            ),
        ],
    )
    def test_a_lookup_error_names_the_marker_as_written(self, marker: Any, wording: str) -> None:
        class _BadLookup(UseCase[Any, str]):
            async def execute(self, user: Any = marker) -> str:
                return "ok"

        with pytest.raises(CompilationError, match=wording):
            UseCaseCompiler().compile(_BadLookup)


# ---------------------------------------------------------------------------
# Boot logging
# ---------------------------------------------------------------------------


class TestCompilerLogging:
    """Compiling a use case logs one ``[BOOT] Compiled UseCase`` line with what it found."""

    def test_logs_one_line_naming_the_use_case(self) -> None:
        log = _RecordingLogger()
        UseCaseCompiler(logger=log).compile(_NoMarkersUseCase)
        assert log.messages == ["[BOOT] Compiled UseCase"]
        assert log.calls[0][1]["usecase"] == "_NoMarkersUseCase"

    @pytest.mark.parametrize(
        ("use_case", "detected"),
        [
            pytest.param(_WithInputUseCase, "Input: UserCommand", id="input"),
            pytest.param(_WithLoadUseCase, "LoadById: User by user_id", id="load-by-id"),
        ],
    )
    def test_lists_each_detected_marker(self, use_case: type[Any], detected: str) -> None:
        log = _RecordingLogger()
        UseCaseCompiler(logger=log).compile(use_case)
        [(_, fields)] = log.calls
        assert detected in fields["detected"]

    def test_counts_the_computes_rules_and_steps(self) -> None:
        log = _RecordingLogger()
        UseCaseCompiler(logger=log).compile(_WithAllUseCase)
        [(_, fields)] = log.calls
        assert (fields["computes"], fields["rules"], fields["steps"]) == (1, 1, 3)

    def test_no_logs_on_cache_hit(self) -> None:
        log = _RecordingLogger()
        compiler = UseCaseCompiler(logger=log)
        compiler.compile(_NoMarkersUseCase)
        count_before = len(log.messages)
        compiler.compile(_NoMarkersUseCase)  # cache hit — no re-inspection
        assert len(log.messages) == count_before
