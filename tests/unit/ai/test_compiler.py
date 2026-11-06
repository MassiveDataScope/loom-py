"""Multi-error compilation tests for ``AgentCompiler`` (US1, T048).

Pinned contract decisions, until the implementation says otherwise:

* ``issue.component`` is the artifact path handed via ``source_path`` — the
  ``AgentCompilationIssue`` docstring documents exactly that shape.
* ``AgentPlan.output`` is a ``CompiledOutput`` exposing a **built**
  :class:`~loom.core.model.LoomType` under ``output.loom_type``
  (data-model.md Tier 3).
* A compiled capability exposes its ``kind`` and carries the resolved handle
  (the ``SqlConnectionConfig``, the registered use-case types) among its
  field values — strings die at compile.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.ai.compiler import (
    AgentCompilationError,
    AgentCompiler,
    AgentPlan,
    CompiledSkillsCapability,
)
from loom.ai.config import AiConfig
from loom.ai.declarative import (
    AgentSpecV1,
    DecodedSpec,
    JsonSchemaOutput,
    PolicySpec,
    SqlCapability,
    TypeRefOutput,
    UsecaseCapability,
    load_specs,
)
from loom.ai.errors import AgentErrorCode
from loom.core.errors import BoundaryValidationError
from loom.core.sql.config import SqlConfig
from loom.core.use_case.registry import UseCaseRegistry
from tests.unit.ai.phases.conftest import ALL_KINDS, admits_every_native_tool

from .conftest import CORPUS_PATTERN

CORPUS_DIR = Path(__file__).parent / "fixtures" / "corpus_v1"


BROKEN_SOURCE = "agents/broken.agent.yaml"

ANSWER_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string"}},
}


def _spec(**overrides: object) -> AgentSpecV1:
    base: dict[str, object] = {
        "spec_version": 1,
        "name": "subject-agent",
        "description": "Answers questions for the compiler tests.",
        "instructions": "Answer using only the prompt. Say so when unsure.",
        "output": JsonSchemaOutput(schema=ANSWER_SCHEMA),
    }
    base.update(overrides)
    return AgentSpecV1(**base)  # type: ignore[arg-type]


def _triple_fault_spec() -> AgentSpecV1:
    """One artifact, three independent faults in three different phases."""
    return _spec(
        model_role="unbound-role",
        policies=PolicySpec(retries=99),
        capabilities=(UsecaseCapability(keys=("nowhere.unknown_key",)),),
    )


@pytest.fixture
def compiler(
    compiler_env_config: AiConfig,
    compiler_env_registry: UseCaseRegistry,
    compiler_env_sql: SqlConfig,
    fake_myapp_path: object,
) -> AgentCompiler:
    return AgentCompiler(
        config=compiler_env_config,
        registry=compiler_env_registry,
        supported_kinds=ALL_KINDS,
        sql=compiler_env_sql,
        native_tools=admits_every_native_tool,
    )


@pytest.fixture
def triple_fault_error(compiler: AgentCompiler) -> AgentCompilationError:
    spec = _triple_fault_spec()
    with pytest.raises(AgentCompilationError) as excinfo:
        compiler.compile(spec, source_path=BROKEN_SOURCE)
    return excinfo.value


class TestMultiErrorAccumulation:
    """A spec with three faults reports all three in one raise (FR-011)."""

    def test_compile_raises_exactly_three_issues_when_spec_has_three_faults(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert len(triple_fault_error.issues) == 3

    def test_compile_reports_three_distinct_codes_when_spec_has_three_faults(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert {issue.code for issue in triple_fault_error.issues} == {
            AgentErrorCode.MODEL_ROLE_UNBOUND,
            AgentErrorCode.POLICY_OUT_OF_RANGE,
            AgentErrorCode.USECASE_KEY_UNKNOWN,
        }

    def test_compile_reports_the_faulted_field_on_each_issue_when_spec_has_three_faults(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert {(issue.code, issue.field) for issue in triple_fault_error.issues} == {
            (AgentErrorCode.MODEL_ROLE_UNBOUND, "model_role"),
            (AgentErrorCode.POLICY_OUT_OF_RANGE, "policies.retries"),
            (AgentErrorCode.USECASE_KEY_UNKNOWN, "capabilities.keys"),
        }

    def test_compile_points_every_issue_at_the_artifact_when_source_path_is_given(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert {issue.component for issue in triple_fault_error.issues} == {BROKEN_SOURCE}


class TestCompileAll:
    """``compile_all`` accumulates across specs before raising once."""

    def test_compile_all_accumulates_issues_of_both_specs_when_two_specs_are_broken(
        self, compiler: AgentCompiler
    ) -> None:
        first = _spec(name="first-agent", model_role="unbound-role")
        second = _spec(
            name="second-agent",
            capabilities=(UsecaseCapability(keys=("nowhere.unknown_key",)),),
        )
        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile_all([first, second])
        assert {issue.code for issue in excinfo.value.issues} == {
            AgentErrorCode.MODEL_ROLE_UNBOUND,
            AgentErrorCode.USECASE_KEY_UNKNOWN,
        }

    def test_compile_all_reports_duplicate_name_when_two_specs_share_a_name(
        self, compiler: AgentCompiler
    ) -> None:
        specs = [_spec(name="dup-agent"), _spec(name="dup-agent")]

        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile_all(specs)
        assert AgentErrorCode.AGENT_NAME_DUPLICATE in {issue.code for issue in excinfo.value.issues}

    def test_compile_all_names_the_artifacts_when_two_files_share_a_name(
        self, compiler: AgentCompiler
    ) -> None:
        """The duplicate issue promises provenance, so it must carry paths.

        Reporting the name once per occurrence ("dup-agent, dup-agent") tells
        an operator nothing they did not already read in ``component``.
        """
        first = DecodedSpec(spec=_spec(name="dup-agent"), source_path="agents/triage.yaml")
        second = DecodedSpec(spec=_spec(name="dup-agent"), source_path="agents/intake.yaml")
        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile_all([first, second])
        issue = next(
            item
            for item in excinfo.value.issues
            if item.code is AgentErrorCode.AGENT_NAME_DUPLICATE
        )
        assert "agents/triage.yaml" in issue.message
        assert "agents/intake.yaml" in issue.message

    def test_compile_all_marks_the_pathless_spec_when_a_duplicate_has_no_artifact(
        self, compiler: AgentCompiler
    ) -> None:
        """A spec handed over as an object has no path; it is named, not dropped."""
        specs = [
            DecodedSpec(spec=_spec(name="dup-agent"), source_path="agents/triage.yaml"),
            _spec(name="dup-agent"),
        ]

        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile_all(specs)
        issue = next(
            item
            for item in excinfo.value.issues
            if item.code is AgentErrorCode.AGENT_NAME_DUPLICATE
        )
        assert "agents/triage.yaml" in issue.message
        assert "<in-memory spec>" in issue.message

    def test_compile_accepts_each_duplicate_spec_when_compiled_alone(
        self, compiler: AgentCompiler
    ) -> None:
        """Duplication is an application-level fault, only visible in compile_all."""
        plan = compiler.compile(_spec(name="dup-agent"))
        assert plan.name == "dup-agent"


class TestCorpus:
    """The published valid corpus compiles clean against a satisfying config."""

    def test_compile_all_returns_one_plan_per_artifact_when_corpus_is_valid(
        self, compiler: AgentCompiler
    ) -> None:
        decoded = load_specs([CORPUS_PATTERN], root=CORPUS_DIR)
        assert len(decoded) == 10
        plans = compiler.compile_all(decoded)
        assert len(plans) == 10

    def test_compile_all_resolves_local_skill_libraries_when_specs_keep_their_path(
        self, compiler: AgentCompiler
    ) -> None:
        """A ``./`` library resolves beside the artifact, so the path must survive."""
        decoded = load_specs([CORPUS_PATTERN], root=CORPUS_DIR)
        plans = {plan.name: plan for plan in compiler.compile_all(decoded)}
        capability = _capability_of_kind(plans["capability-skills-agent"], "skills")
        assert isinstance(capability, CompiledSkillsCapability)
        assert Path(capability.directory).is_dir()


def _flat_field_values(struct: object) -> list[object]:
    values: list[object] = []
    for name in struct.__struct_fields__:  # type: ignore[attr-defined]
        value = getattr(struct, name)
        if isinstance(value, tuple):
            values.extend(value)
        else:
            values.append(value)
    return values


def _capability_of_kind(plan: AgentPlan, kind: str) -> object:
    for capability in plan.capabilities:
        if getattr(capability, "kind", None) == kind:
            return capability
    pytest.fail(f"plan carries no compiled capability of kind {kind!r}")


class TestPlanShape:
    """The plan is immutable and carries resolved handles, not strings."""

    def test_plan_rejects_mutation_when_a_field_is_assigned(self, compiler: AgentCompiler) -> None:
        plan = compiler.compile(_spec())
        with pytest.raises(AttributeError):
            plan.name = "other"  # type: ignore[misc]

    def test_sql_capability_carries_connection_config_when_plan_is_compiled(
        self, compiler: AgentCompiler, compiler_env_sql: SqlConfig
    ) -> None:
        spec = _spec(
            capabilities=(
                SqlCapability(
                    connection="reporting_readonly",
                    max_rows=500,
                    max_result_bytes=1_048_576,
                ),
            )
        )
        capability = _capability_of_kind(compiler.compile(spec), "sql")
        expected = compiler_env_sql.connections["reporting_readonly"]
        assert any(value == expected for value in _flat_field_values(capability))

    def test_usecase_capability_carries_registered_types_when_plan_is_compiled(
        self, compiler: AgentCompiler, compiler_env_registry: UseCaseRegistry
    ) -> None:
        spec = _spec(capabilities=(UsecaseCapability(keys=("orders.get_order_status",)),))
        capability = _capability_of_kind(compiler.compile(spec), "usecase")
        expected = compiler_env_registry.resolve("orders.get_order_status")
        assert any(value is expected for value in _flat_field_values(capability))


class TestCompiledOutput:
    """``json_schema`` outputs compile to a built, strict :class:`LoomType`."""

    def test_plan_exposes_a_msgspec_loom_type_when_output_is_json_schema(
        self, compiler: AgentCompiler
    ) -> None:
        plan = compiler.compile(_spec())
        assert plan.output.loom_type.library == "msgspec"

    def test_loom_type_decodes_valid_bytes_when_output_is_json_schema(
        self, compiler: AgentCompiler
    ) -> None:
        plan = compiler.compile(_spec())
        decoded = plan.output.loom_type.decode_json(b'{"answer": "ok"}')
        assert plan.output.loom_type.to_builtins(decoded) == {"answer": "ok"}

    def test_loom_type_rejects_unknown_fields_when_output_is_json_schema(
        self, compiler: AgentCompiler
    ) -> None:
        plan = compiler.compile(_spec())
        with pytest.raises(BoundaryValidationError):
            plan.output.loom_type.decode_json(b'{"answer": "ok", "extra": 1}')

    def test_schema_is_byte_equal_to_the_authored_document_when_output_is_json_schema(
        self, compiler: AgentCompiler
    ) -> None:
        """FR-007: the authored schema reaches the model unchanged, on the ``json_schema`` path."""
        plan = compiler.compile(_spec())
        assert dict(plan.output.schema) == ANSWER_SCHEMA

    def test_compiles_when_the_object_schema_declares_no_properties(
        self, compiler: AgentCompiler
    ) -> None:
        spec = _spec(output=JsonSchemaOutput(schema={"type": "object"}))
        plan = compiler.compile(spec)
        assert plan.output.loom_type.decode_json(b'{"a": 1}') == {"a": 1}

    def test_compiles_when_the_schema_is_array_rooted(self, compiler: AgentCompiler) -> None:
        spec = _spec(output=JsonSchemaOutput(schema={"type": "array", "items": {"type": "string"}}))
        plan = compiler.compile(spec)
        assert plan.output.loom_type.decode_json(b'["a", "b"]') == ["a", "b"]


class TestTypeRefOutput:
    """``type_ref`` accepts a strict ``msgspec.Struct`` or ``pydantic.BaseModel``.

    T053 fixed this to ``msgspec.Struct`` only; D7/FR-012 add the
    ``pydantic.BaseModel`` branch (``extra='forbid'`` standing in for
    ``forbid_unknown_fields=True``), for callers who never decode a
    ``type_ref`` output back from the model — e.g. one built entirely by
    application code and only passed through loom for its shape and
    ``AgentHandle[...]`` marker check. The decode/coercion behaviour of the
    compiled ``LoomType`` is pinned once, library-agnostically, in
    ``tests/unit/core/model/test_loom_type.py``.
    """

    def test_compile_returns_plan_when_type_ref_resolves_to_msgspec_struct(
        self, compiler: AgentCompiler
    ) -> None:
        spec = _spec(output=TypeRefOutput(ref="myapp.domain.invoices:InvoiceSummary"))
        plan = compiler.compile(spec)
        assert isinstance(plan, AgentPlan)

    def test_compile_returns_plan_when_type_ref_resolves_to_pydantic_model(
        self, compiler: AgentCompiler
    ) -> None:
        spec = _spec(output=TypeRefOutput(ref="myapp.domain.pydantic_invoices:InvoiceSummaryModel"))
        plan = compiler.compile(spec)
        assert isinstance(plan, AgentPlan)

    def test_pydantic_type_ref_loom_type_matches_the_msgspec_shape(
        self, compiler: AgentCompiler
    ) -> None:
        """The marker check (``ai/_startup.py``) reads ``.type``, the engine
        reads ``.library`` to decide construction-time (D7)."""
        from myapp.domain.pydantic_invoices import InvoiceSummaryModel

        spec = _spec(output=TypeRefOutput(ref="myapp.domain.pydantic_invoices:InvoiceSummaryModel"))
        plan = compiler.compile(spec)

        assert plan.output.loom_type.type is InvoiceSummaryModel
        assert plan.output.loom_type.library == "pydantic"

    @pytest.mark.parametrize(
        "ref",
        [
            "myapp.domain.unsupported:PlainModel",
            "myapp.domain.unsupported:NOT_A_TYPE",
            "myapp.domain.unsupported:LaxModel",
        ],
    )
    def test_compile_reports_unsupported_when_type_ref_resolves_to_non_struct(
        self, compiler: AgentCompiler, ref: str
    ) -> None:
        spec = _spec(output=TypeRefOutput(ref=ref))

        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile(spec)
        assert [issue.code for issue in excinfo.value.issues] == [
            AgentErrorCode.OUTPUT_TYPE_REF_UNSUPPORTED
        ]

    def test_compile_reports_unresolvable_when_type_ref_does_not_import(
        self, compiler: AgentCompiler
    ) -> None:
        spec = _spec(output=TypeRefOutput(ref="no_such_pkg_zz.domain:Missing"))
        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile(spec)
        assert [issue.code for issue in excinfo.value.issues] == [
            AgentErrorCode.OUTPUT_TYPE_REF_UNRESOLVABLE
        ]
