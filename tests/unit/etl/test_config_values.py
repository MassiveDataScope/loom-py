"""Tests for FromConfig: declaration, compile-time validation and injection."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import msgspec
import polars as pl
import pytest

from loom.core.config import ConfigContext
from loom.etl import (
    ClientStep,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    FromConfig,
    FromTable,
    IntoTable,
    Sources,
    StepSQL,
)
from loom.etl.compiler import ETLCompilationError, ETLCompiler, ETLErrorCode
from loom.etl.executor import ETLExecutor
from loom.etl.ports import ConfigValueError
from loom.etl.runner import ETLRunner
from loom.etl.testing import PolarsStepRunner, StubSourceReader, StubTargetWriter
from tests.unit._resolver_stubs import MappingResolver
from tests.unit.etl.doubles import RunDateIgnoredParams


class RespondioSettings(msgspec.Struct, kw_only=True):
    api_token: str
    page_size: int = 100


_PARAMS = RunDateIgnoredParams(run_date=date(2026, 9, 25))
_CONFIG = {"respondio": {"api_token": "tok-1", "page_size": 50}}


class FetchStep(ETLStep[RunDateIgnoredParams]):
    api_token = FromConfig("respondio.api_token")
    respondio = FromConfig("respondio", RespondioSettings)
    target = IntoTable("raw.messages").replace()

    def execute(  # type: ignore[override]
        self, params: RunDateIgnoredParams, *, api_token: str, respondio: RespondioSettings
    ) -> Any:
        return {"token": api_token, "page_size": respondio.page_size}


class InlineSourceStep(ETLStep[RunDateIgnoredParams]):
    orders = FromTable("raw.orders")
    api_token = FromConfig("respondio.api_token")
    target = IntoTable("staging.orders").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any, api_token: str) -> Any:  # type: ignore[override]
        return (orders, api_token)


class GroupedSourceStep(ETLStep[RunDateIgnoredParams]):
    sources = Sources(orders=FromTable("raw.orders"))
    api_token = FromConfig("respondio.api_token")
    target = IntoTable("staging.grouped").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any, api_token: str) -> Any:  # type: ignore[override]
        return (orders, api_token)


class TokenClientStep(ClientStep[RunDateIgnoredParams]):
    api_token = FromConfig("respondio.api_token")

    def execute(self, params: RunDateIgnoredParams, *, client: Any, api_token: str) -> None:  # type: ignore[override]
        client.append(api_token)


class _ListClientExecutor:
    def __init__(self) -> None:
        self.client: list[str] = []

    def command(self, fn: Callable[[Any], None]) -> None:
        fn(self.client)


def _executor(
    config: dict[str, Any] | None = _CONFIG,
    reader: StubSourceReader | None = None,
    client_executor: _ListClientExecutor | None = None,
) -> tuple[ETLExecutor, StubTargetWriter]:
    writer = StubTargetWriter()
    executor = ETLExecutor(
        reader or StubSourceReader({}),
        writer,
        client_executor=client_executor,
        config_context=ConfigContext.from_dict(config) if config is not None else None,
    )
    return executor, writer


def _compile_error(step: type[Any], config: dict[str, Any] | None = None) -> ETLCompilationError:
    context = ConfigContext.from_dict(config) if config is not None else None
    with pytest.raises(ETLCompilationError) as exc_info:
        ETLCompiler(config_context=context).compile_step(step)
    return exc_info.value


class TestDeclaration:
    def test_defaults_to_str(self) -> None:
        value = FromConfig("respondio.api_token")

        assert value.key == "respondio.api_token"
        assert value.value_type is str

    def test_repr_names_key_and_type(self) -> None:
        assert repr(FromConfig("respondio", RespondioSettings)) == (
            "FromConfig('respondio', RespondioSettings)"
        )

    @pytest.mark.parametrize("key", ["", "respondio..api_token", ".respondio", "respondio."])
    def test_rejects_malformed_key(self, key: str) -> None:
        with pytest.raises(ValueError, match="FromConfig key"):
            FromConfig(key)

    def test_is_not_a_frame_source(self) -> None:
        assert FetchStep._source_form.value == "none"
        assert set(FetchStep._config_values) == {"api_token", "respondio"}

    def test_coexists_with_grouped_sources(self) -> None:
        assert GroupedSourceStep._source_form.value == "grouped"
        assert set(GroupedSourceStep._config_values) == {"api_token"}


class TestCompileStructure:
    def test_plan_carries_bindings_without_values(self) -> None:
        plan = ETLCompiler().compile_step(FetchStep)

        assert [(b.alias, b.key, b.value_type) for b in plan.config_bindings] == [
            ("api_token", "respondio.api_token", str),
            ("respondio", "respondio", RespondioSettings),
        ]

    def test_missing_execute_param_is_rejected(self) -> None:
        class _Step(ETLStep[RunDateIgnoredParams]):
            api_token = FromConfig("respondio.api_token")
            target = IntoTable("raw.messages").replace()

            def execute(self, params: RunDateIgnoredParams) -> Any:  # type: ignore[override]
                return None

        error = _compile_error(_Step)

        assert error.code is ETLErrorCode.MISSING_CONFIG_PARAMS
        assert "api_token" in str(error)

    def test_extra_execute_param_is_rejected(self) -> None:
        class _Step(ETLStep[RunDateIgnoredParams]):
            target = IntoTable("raw.messages").replace()

            def execute(self, params: RunDateIgnoredParams, *, api_token: str) -> Any:  # type: ignore[override]
                return None

        assert _compile_error(_Step).code is ETLErrorCode.EXTRA_SOURCE_PARAMS

    def test_alias_shared_with_a_grouped_source_is_rejected(self) -> None:
        class _Step(ETLStep[RunDateIgnoredParams]):
            sources = Sources(orders=FromTable("raw.orders"))
            orders = FromConfig("respondio.api_token")
            target = IntoTable("staging.orders").replace()

            def execute(self, params: RunDateIgnoredParams, *, orders: Any) -> Any:  # type: ignore[override]
                return orders

        assert _compile_error(_Step).code is ETLErrorCode.CONFIG_ALIAS_CONFLICT

    def test_client_step_needs_the_param(self) -> None:
        class _Client(ClientStep[RunDateIgnoredParams]):
            api_token = FromConfig("respondio.api_token")

            def execute(self, params: RunDateIgnoredParams, *, client: Any) -> None:  # type: ignore[override]
                return None

        assert _compile_error(_Client).code is ETLErrorCode.MISSING_CONFIG_PARAMS

    def test_client_alias_is_reserved_on_client_steps(self) -> None:
        class _Client(ClientStep[RunDateIgnoredParams]):
            client = FromConfig("respondio.api_token")

            def execute(self, params: RunDateIgnoredParams, *, client: Any) -> None:  # type: ignore[override]
                return None

        assert _compile_error(_Client).code is ETLErrorCode.CONFIG_ALIAS_CONFLICT

    def test_sql_step_cannot_receive_config_values(self) -> None:
        class _Sql(StepSQL[RunDateIgnoredParams, Any]):
            orders = FromTable("raw.orders")
            api_token = FromConfig("respondio.api_token")
            target = IntoTable("staging.sql").replace()
            sql = "SELECT * FROM orders"

        assert _compile_error(_Sql).code is ETLErrorCode.UNSUPPORTED_CONFIG_VALUE

    def test_compiler_without_config_skips_resolution(self) -> None:
        plan = ETLCompiler().compile_step(FetchStep)

        assert plan.step_type is FetchStep


class TestCompileAgainstConfig:
    def test_valid_config_compiles(self) -> None:
        plan = ETLCompiler(config_context=ConfigContext.from_dict(_CONFIG)).compile_step(FetchStep)

        assert len(plan.config_bindings) == 2

    def test_missing_key_is_reported(self) -> None:
        error = _compile_error(FetchStep, {"respondio": {"page_size": 1}})

        assert error.code is ETLErrorCode.UNRESOLVED_CONFIG_VALUE
        assert error.field == "api_token"
        assert "'respondio.api_token'" in str(error)
        assert "is not set" in str(error)

    def test_type_mismatch_is_reported(self) -> None:
        error = _compile_error(FetchStep, {"respondio": {"api_token": "t", "page_size": "many"}})

        assert error.code is ETLErrorCode.UNRESOLVED_CONFIG_VALUE
        assert "does not validate as 'RespondioSettings'" in str(error)

    def test_unresolvable_interpolation_is_reported(self) -> None:
        config = {"respondio": {"api_token": "${oc.env:LOOM_TEST_UNSET_TOKEN_VAR}"}}

        error = _compile_error(InlineSourceStep, config)

        assert "cannot be resolved" in str(error)

    def test_pipeline_compile_checks_every_step(self) -> None:
        class _Proc(ETLProcess[RunDateIgnoredParams]):
            steps = [InlineSourceStep, FetchStep]

        class _Pipeline(ETLPipeline[RunDateIgnoredParams]):
            processes = [_Proc]

        compiler = ETLCompiler(config_context=ConfigContext.from_dict({"respondio": {}}))

        with pytest.raises(ETLCompilationError, match="respondio.api_token"):
            compiler.compile(_Pipeline)


class TestExecutorInjection:
    def test_injects_scalar_and_struct(self) -> None:
        executor, writer = _executor()

        executor.run_step(ETLCompiler().compile_step(FetchStep), _PARAMS)

        assert writer.written[0][0] == {"token": "tok-1", "page_size": 50}

    def test_injects_next_to_inline_and_grouped_sources(self) -> None:
        for step in (InlineSourceStep, GroupedSourceStep):
            executor, writer = _executor(reader=StubSourceReader({"orders": "frame"}))

            executor.run_step(ETLCompiler().compile_step(step), _PARAMS)

            assert writer.written[0][0] == ("frame", "tok-1")

    def test_injects_into_client_steps(self) -> None:
        client_executor = _ListClientExecutor()
        executor, _ = _executor(client_executor=client_executor)

        executor.run_step(ETLCompiler().compile_step(TokenClientStep), _PARAMS)

        assert client_executor.client == ["tok-1"]

    def test_resolves_at_run_time_not_at_compile_time(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = {"respondio": {"api_token": "${oc.env:LOOM_TEST_ROTATING_TOKEN}"}}
        executor, writer = _executor(config=config, reader=StubSourceReader({"orders": 1}))
        plan = ETLCompiler().compile_step(InlineSourceStep)

        monkeypatch.setenv("LOOM_TEST_ROTATING_TOKEN", "first")
        executor.run_step(plan, _PARAMS)
        monkeypatch.setenv("LOOM_TEST_ROTATING_TOKEN", "second")
        executor.run_step(plan, _PARAMS)

        assert [frame[1] for frame, _ in writer.written] == ["first", "second"]

    def test_without_config_context_fails_clearly(self) -> None:
        executor, _ = _executor(config=None)

        with pytest.raises(RuntimeError, match=r"FromConfig.*config_context"):
            executor.run_step(ETLCompiler().compile_step(FetchStep), _PARAMS)

    def test_resolution_failure_raises_config_value_error(self) -> None:
        executor, _ = _executor(config={"other": 1})

        with pytest.raises(ConfigValueError, match="'respondio.api_token' is not set") as exc:
            executor.run_step(ETLCompiler().compile_step(FetchStep), _PARAMS)

        assert exc.value.key == "respondio.api_token"

    def test_steps_without_config_values_need_no_context(self) -> None:
        class _Plain(ETLStep[RunDateIgnoredParams]):
            target = IntoTable("raw.plain").replace()

            def execute(self, params: RunDateIgnoredParams) -> Any:  # type: ignore[override]
                return "ok"

        executor, writer = _executor(config=None)

        executor.run_step(ETLCompiler().compile_step(_Plain), _PARAMS)

        assert writer.written[0][0] == "ok"


class _TokenLengthStep(ETLStep[RunDateIgnoredParams]):
    api_token = FromConfig("respondio.api_token")
    target = IntoTable("raw.token_length").replace()

    def execute(self, params: RunDateIgnoredParams, *, api_token: str) -> pl.LazyFrame:  # type: ignore[override]
        return pl.DataFrame({"token_length": [len(api_token)]}).lazy()


class _TokenProc(ETLProcess[RunDateIgnoredParams]):
    steps = [_TokenLengthStep]


class _TokenPipeline(ETLPipeline[RunDateIgnoredParams]):
    processes = [_TokenProc]


def _write_yaml(tmp_path: Path, token: str) -> str:
    path = tmp_path / "loom.yaml"
    path.write_text(
        "storage:\n"
        "  missing_table_policy: create\n"
        "  defaults:\n"
        "    table_path:\n"
        f"      uri: {tmp_path}\n"
        "respondio:\n"
        f"  api_token: {token}\n",
        encoding="utf-8",
    )
    return str(path)


@pytest.mark.usefixtures("clear_builtin_resolvers")
class TestRunnerFromYaml:
    def test_value_comes_from_the_runner_yaml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("LOOM_TEST_RESPONDIO_TOKEN", "12345")
        runner = ETLRunner.from_yaml(_write_yaml(tmp_path, "${oc.env:LOOM_TEST_RESPONDIO_TOKEN}"))

        runner.run(_TokenPipeline, _PARAMS)

        written = pl.scan_delta(str(tmp_path / "raw" / "token_length")).collect()
        assert written["token_length"].to_list() == [5]

    def test_value_may_come_from_a_resolver(self, tmp_path: Path) -> None:
        vault = MappingResolver("vault", {"/respondio/token": "abc"})
        runner = ETLRunner.from_yaml(
            _write_yaml(tmp_path, "${vault:/respondio/token}"), resolvers=[vault]
        )

        runner.run(_TokenPipeline, _PARAMS)

        written = pl.scan_delta(str(tmp_path / "raw" / "token_length")).collect()
        assert written["token_length"].to_list() == [3]

    def test_missing_key_fails_before_any_step_runs(self, tmp_path: Path) -> None:
        path = tmp_path / "loom.yaml"
        path.write_text(f"storage:\n  defaults:\n    table_path:\n      uri: {tmp_path}\n")
        runner = ETLRunner.from_yaml(str(path))

        with pytest.raises(ETLCompilationError, match="'respondio.api_token' is not set"):
            runner.run(_TokenPipeline, _PARAMS)

        assert not (tmp_path / "raw").exists()


class TestPolarsStepRunner:
    def test_with_config_injects_fake_values(self) -> None:
        class _Step(ETLStep[RunDateIgnoredParams]):
            orders = FromTable("raw.orders")
            api_token = FromConfig("respondio.api_token")
            target = IntoTable("staging.orders").replace()

            def execute(  # type: ignore[override]
                self, params: RunDateIgnoredParams, *, orders: pl.LazyFrame, api_token: str
            ) -> pl.LazyFrame:
                return orders.with_columns(pl.lit(api_token).alias("token"))

        runner = PolarsStepRunner().with_config({"respondio": {"api_token": "fake"}})
        runner.seed("raw.orders", [(1,)], ["id"])

        result = runner.run(_Step, _PARAMS)

        assert result.to_polars()["token"].to_list() == ["fake"]

    def test_with_config_is_validated_at_compile(self) -> None:
        runner = PolarsStepRunner().with_config({"respondio": {}})

        with pytest.raises(ETLCompilationError, match="respondio.api_token"):
            runner.run(_TokenLengthStep, _PARAMS)
