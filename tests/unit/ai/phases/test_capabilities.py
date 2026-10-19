"""Capability phase failures (T050): one case per applicable error code.

Documented decisions taken while writing these tests:

* **Naming, not locating.** ``mcp``, ``a2a`` and ``sql`` name something the
  deployment configuration locates, so the faults this phase can find are
  *unknown name* faults (``MCP_SERVER_UNKNOWN``, ``A2A_AGENT_UNKNOWN``,
  ``SQL_CONNECTION_UNKNOWN``). A malformed URL or an inline credential is a
  Tier-2 fault and is pinned in ``tests/unit/ai/test_config.py``; it is not
  representable in an artifact at all.
* **Skill libraries resolve offline.** Listing a directory is not network
  access, so the library, the discovery and the include/exclude filter all die
  at compile and the plan carries exact names. These tests therefore write real
  ``SKILL.md`` packages on disk rather than mocking a listing.
* ``SQL_CONNECTION_ROLES_UNBOUND`` — semantics anchored on
  ``loom.core.sql.config.roles_need_identity_binding``: a connection whose
  ``allowed_roles`` is non-empty needs a verified caller identity to bind a
  subset of the allowlist. An agent whose endpoint opts into
  ``allow_anonymous`` carries no such identity, so granting it that
  connection must be rejected (FR-043a). The anonymous opt-out also trips
  ``ANONYMOUS_WITH_DATA_CAPABILITY``, hence the containment assertion.
* ``SQL_RESULT_BOUND_MISSING`` — see ``TestSqlResultBoundMissing``.
* Anonymous classification (FR-045a): kinds that read application data or
  call a remote system — ``usecase``, ``sql``, ``python``, ``mcp``, ``a2a`` —
  trip ``ANONYMOUS_WITH_DATA_CAPABILITY``; ``skills`` grants packaged prompt
  material only, reads no data and calls nothing remote, so it does not.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec
import pytest

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.compiler import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledCapability,
    CompiledMcpCapability,
    CompiledPythonCapability,
    CompiledSkillsCapability,
)
from loom.ai.compiler.phases import compile_mcp_capability
from loom.ai.config import AgentEndpointConfig, AiConfig
from loom.ai.declarative import (
    A2ACapability,
    AgentSpecV1,
    CapabilitySpec,
    McpCapability,
    PythonCapability,
    SkillsCapability,
    SqlCapability,
    UsecaseCapability,
)
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case import Query
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase

from ..conftest import SKILLS_ROOT_DIR, STDIO_MCP_SERVER

_SHARED_LIBRARY = "shared"
"""Bare library name the shared fixture root resolves; see ``fixtures/skills_root``."""

_SHARED_SKILLS: tuple[str, ...] = ("group-changes", "tone-of-voice")
"""Every skill of the shared library, in the order the compiler emits them."""


def _valid_sql() -> SqlCapability:
    return SqlCapability(connection="reporting_readonly", max_rows=500, max_result_bytes=1_048_576)


def _capability_of_kind(plan: AgentPlan, kind: str) -> CompiledCapability:
    for capability in plan.capabilities:
        if capability.kind == kind:
            return capability
    pytest.fail(f"plan carries no compiled capability of kind {kind!r}")


def test_reports_kind_unsupported_when_engine_does_not_serve_the_kind(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    spec = spec_factory(capabilities=(McpCapability(server="knowledge"),))
    issue = single_issue_for(spec, supported_kinds=frozenset({"usecase"}))
    assert (issue.code, issue.field) == (
        AgentErrorCode.CAPABILITY_KIND_UNSUPPORTED,
        "capabilities",
    )


def test_reports_usecase_key_unknown_when_key_is_not_registered(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    spec = spec_factory(capabilities=(UsecaseCapability(keys=("nowhere.unknown_key",)),))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.USECASE_KEY_UNKNOWN,
        "capabilities.keys",
    )


class _ListQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    limit: int = 50


class _ListItems(UseCase[Any, int]):
    async def execute(self, query: _ListQuery = Query()) -> int:
        return query.limit


def test_reports_usecase_declares_query_when_the_granted_use_case_takes_query(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    UseCaseCompiler().compile(_ListItems)
    registry = UseCaseRegistry(
        _by_name={"items.list": _ListItems}, _by_type={_ListItems: "items.list"}
    )
    spec = spec_factory(capabilities=(UsecaseCapability(keys=("items.list",)),))

    issue = single_issue_for(spec, registry=registry)

    assert (issue.code, issue.field) == (
        AgentErrorCode.USECASE_DECLARES_QUERY,
        "capabilities.keys",
    )
    assert "items.list" in issue.message
    assert "Query()" in issue.message


class TestSql:
    def test_reports_connection_unknown_when_connection_is_not_configured(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(
                SqlCapability(connection="missing_conn", max_rows=10, max_result_bytes=1024),
            )
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SQL_CONNECTION_UNKNOWN,
            "capabilities.connection",
        )

    def test_reports_not_readonly_when_connection_permits_writes(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(SqlCapability(connection="writable", max_rows=10, max_result_bytes=1024),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SQL_CONNECTION_NOT_READONLY,
            "capabilities.connection",
        )

    def test_reports_config_missing_when_no_data_layer_config_is_supplied(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """FR-046a: absence of configuration must never skip the check silently."""
        issue = single_issue_for(spec_factory(capabilities=(_valid_sql(),)), sql=None)
        assert issue.code is AgentErrorCode.SQL_CONFIG_MISSING

    def test_reports_roles_unbound_when_allowlisted_connection_has_no_identity_to_bind(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        issues_for: Callable[..., tuple[AgentCompilationIssue, ...]],
        ai_config_factory: Callable[..., AiConfig],
    ) -> None:
        """FR-043a: without a verified identity the allowlist becomes a menu."""
        config = ai_config_factory(
            endpoints={
                "subject-agent": AgentEndpointConfig(
                    enabled=True, auth="bearer", allow_anonymous=True
                )
            }
        )
        spec = spec_factory(
            capabilities=(
                SqlCapability(connection="roles_menu", max_rows=10, max_result_bytes=1024),
            )
        )
        codes = {issue.code for issue in issues_for(spec, config=config)}
        assert AgentErrorCode.SQL_CONNECTION_ROLES_UNBOUND in codes


class TestSqlResultBoundMissing:
    """FR-046b, decision record.

    ``SqlCapability`` makes an unbounded query unrepresentable at Tier 1:
    ``max_rows`` and ``max_result_bytes`` are required fields constrained to
    ``>= 1`` at decode, so no ``AgentSpecV1`` can reach the compiler without
    bounds and the compiler phase has nothing left to reject. These tests pin
    that irrepresentability (the guarantee T061 relies on) and that the code
    stays published in the catalogue for external constructors of issues.
    """

    def test_decode_rejects_sql_capability_when_bounds_are_absent(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert(
                {"kind": "sql", "connection": "reporting_readonly"},
                type=CapabilitySpec,
            )

    @pytest.mark.parametrize("bound_field", ["max_rows", "max_result_bytes"])
    def test_decode_rejects_sql_capability_when_a_bound_is_not_positive(
        self, bound_field: str
    ) -> None:
        payload: dict[str, Any] = {
            "kind": "sql",
            "connection": "reporting_readonly",
            "max_rows": 10,
            "max_result_bytes": 1024,
            bound_field: 0,
        }
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert(payload, type=CapabilitySpec)

    def test_error_code_stays_published_in_the_catalogue(self) -> None:
        assert AgentErrorCode.SQL_RESULT_BOUND_MISSING.value == "SQL_RESULT_BOUND_MISSING"


class TestMcp:
    """An artifact names a server; ``ai.mcp_servers`` says where it is."""

    def test_reports_server_unknown_when_server_is_not_configured(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(capabilities=(McpCapability(server="nowhere"),))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.MCP_SERVER_UNKNOWN,
            "capabilities.server",
        )

    def test_carries_the_configured_endpoint_when_server_is_known(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        compiler_env_config: AiConfig,
    ) -> None:
        """The name dies at compile: the plan holds URL, credential ref and deadline."""
        spec = spec_factory(capabilities=(McpCapability(server="knowledge"),))
        capability = _capability_of_kind(plan_for(spec), "mcp")
        server = compiler_env_config.mcp_servers["knowledge"]
        assert isinstance(capability, CompiledMcpCapability)
        assert (
            capability.transport,
            capability.url,
            capability.headers_ref,
            capability.timeout_ms,
        ) == ("http", server.url, server.headers_ref, server.timeout_ms)

    def test_carries_transport_command_args_and_env_when_server_is_stdio(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        compiler_env_config: AiConfig,
    ) -> None:
        """A subprocess server travels whole, its ``env`` as key-sorted pairs (FR-009)."""
        spec = spec_factory(capabilities=(McpCapability(server=STDIO_MCP_SERVER),))
        capability = _capability_of_kind(plan_for(spec), "mcp")
        server = compiler_env_config.mcp_servers[STDIO_MCP_SERVER]
        assert isinstance(capability, CompiledMcpCapability)
        assert server.env is not None
        assert (capability.transport, capability.command, capability.args, capability.env) == (
            "stdio",
            server.command,
            server.args,
            tuple(sorted(server.env.items())),
        )
        assert capability.url is None

    def test_carries_the_filter_verbatim_when_server_is_known(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
    ) -> None:
        """Tool names only exist at start-up, so the globs travel unevaluated."""
        capability_spec = McpCapability(
            server="knowledge",
            include=("search_*", "fetch_document"),
            exclude=("delete_*",),
        )
        plan = plan_for(spec_factory(capabilities=(capability_spec,)))
        capability = _capability_of_kind(plan, "mcp")
        assert isinstance(capability, CompiledMcpCapability)
        assert (capability.include, capability.exclude) == (
            ("search_*", "fetch_document"),
            ("delete_*",),
        )


class TestCompileMcpCapabilityHelper:
    """``compile_mcp_capability`` (T201): the shared body ``_compile_mcp`` calls.

    Exercised directly, with no ``AgentSpecV1`` in sight, because it is meant
    to be reachable from the use-case marker path without the artifact
    vocabulary — this is what a caller outside the agent capability phase
    actually calls.
    """

    def test_resolves_a_configured_server_with_the_callers_own_filter(
        self, compiler_env_config: AiConfig
    ) -> None:
        capability, issues = compile_mcp_capability(
            "knowledge",
            include=("search_*",),
            exclude=(),
            servers=compiler_env_config.mcp_servers,
            component="orders.get_order_status",
        )
        server = compiler_env_config.mcp_servers["knowledge"]
        assert issues == []
        assert isinstance(capability, CompiledMcpCapability)
        assert (capability.url, capability.include, capability.exclude) == (
            server.url,
            ("search_*",),
            (),
        )

    def test_reports_server_unknown_when_the_name_is_not_configured(
        self, compiler_env_config: AiConfig
    ) -> None:
        capability, issues = compile_mcp_capability(
            "nowhere",
            include=(),
            exclude=(),
            servers=compiler_env_config.mcp_servers,
            component="orders.get_order_status",
        )
        assert capability is None
        (issue,) = issues
        assert (issue.code, issue.field) == (
            AgentErrorCode.MCP_SERVER_UNKNOWN,
            "capabilities.server",
        )

    def test_the_agent_capability_phase_calls_it_for_an_identical_result(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        compiler_env_config: AiConfig,
    ) -> None:
        """The extraction changes nothing observable about the existing path."""
        spec = spec_factory(
            capabilities=(McpCapability(server="knowledge", include=("search_*",)),)
        )
        via_phase = _capability_of_kind(plan_for(spec), "mcp")
        via_helper, issues = compile_mcp_capability(
            "knowledge",
            include=("search_*",),
            exclude=(),
            servers=compiler_env_config.mcp_servers,
            component=spec.name,
        )
        assert issues == []
        assert via_phase == via_helper


class TestA2A:
    """An artifact names a remote agent; ``ai.a2a_agents`` says where it is."""

    def test_reports_agent_unknown_when_agent_is_not_configured(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(capabilities=(A2ACapability(agent="nowhere"),))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.A2A_AGENT_UNKNOWN,
            "capabilities.agent",
        )

    def test_carries_the_configured_endpoint_and_filter_when_agent_is_known(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        compiler_env_config: AiConfig,
    ) -> None:
        """The card is fetched at start-up, so the skill filter travels unevaluated."""
        spec = spec_factory(
            capabilities=(A2ACapability(agent="translations", include=("translate_*",)),)
        )
        capability = _capability_of_kind(plan_for(spec), "a2a")
        agent = compiler_env_config.a2a_agents["translations"]
        assert isinstance(capability, CompiledA2ACapability)
        assert (capability.url, capability.headers_ref, capability.include) == (
            agent.url,
            agent.headers_ref,
            ("translate_*",),
        )


class TestSkills:
    """Libraries resolve, discover and filter offline; the plan holds exact names."""

    def test_reports_root_missing_when_config_declares_no_skills_root(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        ai_config_factory: Callable[..., AiConfig],
    ) -> None:
        """A bare name has nothing to resolve against without ``ai.skills_root``."""
        spec = spec_factory(capabilities=(SkillsCapability(library=_SHARED_LIBRARY),))
        issue = single_issue_for(spec, config=ai_config_factory(skills_root=None))
        assert issue.code is AgentErrorCode.SKILLS_ROOT_MISSING

    def test_reports_library_escapes_when_library_climbs_out_of_its_directory(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """``..`` is unrepresentable in a decoded artifact; the phase refuses it anyway."""
        spec = spec_factory(capabilities=(SkillsCapability(library=".."),))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SKILLS_LIBRARY_ESCAPES,
            "capabilities.library",
        )

    def test_reports_library_invalid_when_a_local_library_has_no_artifact_path(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """``./name`` is anchored to the artifact; bare bytes give it nothing to anchor to."""
        spec = spec_factory(capabilities=(SkillsCapability(library="./skills"),))
        issue = single_issue_for(spec, source_path=None)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SKILLS_LIBRARY_INVALID,
            "capabilities.library",
        )

    def test_reports_library_invalid_when_the_directory_does_not_exist(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(capabilities=(SkillsCapability(library="no-such-library"),))
        issue = single_issue_for(spec)
        assert issue.code is AgentErrorCode.SKILLS_LIBRARY_INVALID

    def test_reports_capability_empty_when_the_filter_selects_nothing(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """A grant that ends up granting nothing is a fault, never a silent no-op."""
        spec = spec_factory(
            capabilities=(SkillsCapability(library=_SHARED_LIBRARY, include=("no-such-skill-*",)),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (AgentErrorCode.CAPABILITY_EMPTY, "capabilities")

    def test_reports_name_collision_when_two_libraries_expose_the_same_skill(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        skill_library: Callable[..., Path],
        artifact_path: str,
    ) -> None:
        """One agent cannot hold two skills answering to the same name."""
        skill_library("first", "duplicated", "only-in-first")
        skill_library("second", "duplicated")
        spec = spec_factory(
            capabilities=(
                SkillsCapability(library="./first"),
                SkillsCapability(library="./second"),
            )
        )
        issue = single_issue_for(spec, source_path=artifact_path)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SKILLS_NAME_COLLISION,
            "capabilities.library",
        )

    def test_compiles_two_libraries_when_their_skill_names_are_disjoint(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        skill_library: Callable[..., Path],
        artifact_path: str,
    ) -> None:
        """The collision check is about names, not about holding two libraries."""
        skill_library("first", "alpha")
        skill_library("second", "beta")
        spec = spec_factory(
            capabilities=(
                SkillsCapability(library="./first"),
                SkillsCapability(library="./second"),
            )
        )
        plan = plan_for(spec, source_path=artifact_path)
        assert [
            capability.names
            for capability in plan.capabilities
            if isinstance(capability, CompiledSkillsCapability)
        ] == [("alpha",), ("beta",)]

    def test_resolves_a_shared_library_to_its_directory_and_names(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
    ) -> None:
        """A bare name resolves against ``ai.skills_root``; the plan holds exact names."""
        spec = spec_factory(capabilities=(SkillsCapability(library=_SHARED_LIBRARY),))
        capability = _capability_of_kind(plan_for(spec), "skills")
        assert isinstance(capability, CompiledSkillsCapability)
        assert (Path(capability.directory), capability.names) == (
            (SKILLS_ROOT_DIR / _SHARED_LIBRARY).resolve(),
            _SHARED_SKILLS,
        )

    def test_resolves_a_local_library_beside_the_artifact(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        skill_library: Callable[..., Path],
        artifact_path: str,
    ) -> None:
        """``./name`` travels with the artifact, so it resolves from its directory."""
        directory = skill_library("skills", "release-notes")
        spec = spec_factory(capabilities=(SkillsCapability(library="./skills"),))
        capability = _capability_of_kind(plan_for(spec, source_path=artifact_path), "skills")
        assert isinstance(capability, CompiledSkillsCapability)
        assert Path(capability.directory) == directory.resolve()

    def test_applies_the_filter_at_compile_when_the_library_resolves(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
    ) -> None:
        """Skill names are known offline, so the globs die at compile."""
        spec = spec_factory(
            capabilities=(
                SkillsCapability(
                    library=_SHARED_LIBRARY,
                    include=("*-changes", "tone-*"),
                    exclude=("group-*",),
                ),
            )
        )
        capability = _capability_of_kind(plan_for(spec), "skills")
        assert isinstance(capability, CompiledSkillsCapability)
        assert capability.names == ("tone-of-voice",)

    def test_ignores_a_directory_without_a_manifest_when_the_library_is_listed(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        skill_library: Callable[..., Path],
        artifact_path: str,
    ) -> None:
        """Only an immediate child holding ``SKILL.md`` is a skill."""
        directory = skill_library("skills", "real-skill")
        (directory / "not-a-skill").mkdir()
        spec = spec_factory(capabilities=(SkillsCapability(library="./skills"),))
        capability = _capability_of_kind(plan_for(spec, source_path=artifact_path), "skills")
        assert isinstance(capability, CompiledSkillsCapability)
        assert capability.names == ("real-skill",)


class TestPythonFactory:
    def test_reports_unresolvable_when_factory_does_not_import(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(capabilities=(PythonCapability(factory="no_such_pkg_zz.tools:build"),))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_UNRESOLVABLE,
            "capabilities.factory",
        )

    def test_reports_not_callable_when_factory_resolves_to_a_non_callable(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(PythonCapability(factory="myapp.tools.broken:NOT_CALLABLE"),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_NOT_CALLABLE,
            "capabilities.factory",
        )

    def test_reports_not_callable_when_factory_leaves_no_slot_for_the_context(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(PythonCapability(factory="myapp.tools.params:build_keyword_only"),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_NOT_CALLABLE,
            "capabilities.factory",
        )

    def test_carries_params_when_they_bind_to_the_factory_signature(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
    ) -> None:
        params = {"max_results": 3, "radius_km": 25}
        spec = spec_factory(
            capabilities=(
                PythonCapability(factory="myapp.tools.geo:build_geo_toolset", params=params),
            )
        )
        (compiled,) = plan_for(spec).capabilities
        assert isinstance(compiled, CompiledPythonCapability)
        assert compiled.params == params

    def test_reports_params_rejected_when_a_key_is_unknown_to_the_factory(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(
                PythonCapability(
                    factory="myapp.tools.geo:build_geo_toolset", params={"max_depth": 3}
                ),
            )
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_PARAMS_REJECTED,
            "capabilities.params",
        )
        assert "myapp.tools.geo:build_geo_toolset" in issue.message
        assert "max_depth" in issue.message

    def test_reports_params_rejected_when_a_required_parameter_is_missing(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(PythonCapability(factory="myapp.tools.params:build_strict"),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_PARAMS_REJECTED,
            "capabilities.params",
        )
        assert "max_results" in issue.message

    def test_accepts_any_key_when_the_factory_declares_kwargs(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
    ) -> None:
        spec = spec_factory(
            capabilities=(
                PythonCapability(factory="myapp.tools.params:build_any", params={"anything": 1}),
            )
        )
        (compiled,) = plan_for(spec).capabilities
        assert isinstance(compiled, CompiledPythonCapability)
        assert compiled.params == {"anything": 1}

    def test_accepts_params_when_the_factory_signature_cannot_be_inspected(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
    ) -> None:
        spec = spec_factory(
            capabilities=(
                PythonCapability(factory="myapp.tools.params:UNINSPECTABLE", params={"x": 1}),
            )
        )
        (compiled,) = plan_for(spec).capabilities
        assert isinstance(compiled, CompiledPythonCapability)
        assert compiled.params == {"x": 1}


class TestAnonymousOptOut:
    """FR-045a: anonymous agents may hold no data or remote capability.

    The opt-out is ``AiConfig.endpoints[name].allow_anonymous = True``.
    Data/remote kinds: ``usecase`` and ``sql`` read application data;
    ``python``, ``mcp`` and ``a2a`` run application code or call a remote
    system. ``skills`` only injects packaged prompt material, so it is the
    one kind an anonymous agent may keep.
    """

    @staticmethod
    def _anonymous_config(ai_config_factory: Callable[..., AiConfig]) -> AiConfig:
        return ai_config_factory(
            endpoints={
                "subject-agent": AgentEndpointConfig(
                    enabled=True, auth="bearer", allow_anonymous=True
                )
            }
        )

    @pytest.mark.parametrize(
        "capability",
        [
            UsecaseCapability(keys=("orders.get_order_status",)),
            _valid_sql(),
            PythonCapability(factory="myapp.tools.geo:build_geo_toolset"),
            McpCapability(server="knowledge"),
            A2ACapability(agent="translations"),
        ],
        ids=["usecase", "sql", "python", "mcp", "a2a"],
    )
    def test_reports_anonymous_with_data_capability_when_kind_reads_data_or_calls_remote(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        issues_for: Callable[..., tuple[AgentCompilationIssue, ...]],
        ai_config_factory: Callable[..., AiConfig],
        capability: CapabilitySpec,
    ) -> None:
        spec = spec_factory(capabilities=(capability,))
        config = self._anonymous_config(ai_config_factory)
        codes = {issue.code for issue in issues_for(spec, config=config)}
        assert AgentErrorCode.ANONYMOUS_WITH_DATA_CAPABILITY in codes

    def test_compiles_clean_when_anonymous_agent_holds_only_skills(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., AgentPlan],
        ai_config_factory: Callable[..., AiConfig],
    ) -> None:
        spec = spec_factory(capabilities=(SkillsCapability(library=_SHARED_LIBRARY),))
        plan = plan_for(spec, config=self._anonymous_config(ai_config_factory))
        assert plan.name == "subject-agent"
