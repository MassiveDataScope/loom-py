"""``McpGrantView`` and ``SqlGrantView``: the concrete grant handles (T301–T303).

Covers the filter refusing an excluded tool without touching the session,
typed and untyped calls, the three network-facing failure modes and their
ordering, and the SQL view's role binding and byte-bound truncation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import msgspec
import pytest

from loom.ai.abc import McpToolCallResult, McpToolInfo
from loom.ai.compiler import CompiledMcpCapability, CompiledSqlCapability
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._grants import McpGrantView, SqlGrantView
from loom.core.identity import ANONYMOUS, Identity
from loom.core.sql.abc.contracts import SqlColumn, SqlQueryResult
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.sql.service import SqlQueryService
from tests.integration.ai.conftest import make_mcp_capability, make_sql_capability

_AGENT = "triage"
_CALLER = Identity(subject="ada", roles=("analyst",), mechanism="test")


class FakeSession:
    """A double of ``SharedMcpSession`` recording every call it served."""

    def __init__(
        self,
        *,
        tools: tuple[McpToolInfo, ...],
        results: Mapping[str, McpToolCallResult] | None = None,
    ) -> None:
        self._tools = tools
        self._results = dict(results or {})
        self.calls: list[str] = []

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return self._tools

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        self.calls.append(name)
        return self._results.get(name, McpToolCallResult(ok=True, structured=None))


class _Severity(msgspec.Struct, frozen=True):
    level: int


pydantic = pytest.importorskip("pydantic")


class _SeverityModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    level: int


def _view(
    *,
    capability: CompiledMcpCapability,
    session: FakeSession,
    catalogue: tuple[McpToolInfo, ...],
) -> McpGrantView:
    return McpGrantView(
        span_attributes={"agent": _AGENT},
        capability=capability,
        include=capability.include,
        exclude=capability.exclude,
        session=session,  # type: ignore[arg-type]
        catalogue=catalogue,
        timeout_s=1.0,
        identity=_CALLER,
        observability=None,
    )


class TestTheMcpGrantsFilter:
    """The filter is the artifact's own include/exclude, without touching the network."""

    async def test_an_excluded_tool_is_rejected_without_calling_the_session(self) -> None:
        capability = make_mcp_capability(exclude=("write_*",))
        catalogue = (
            McpToolInfo(name="read_orders", has_output_schema=True),
            McpToolInfo(name="write_orders", has_output_schema=True),
        )
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call_untyped("write_orders", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert session.calls == []

    async def test_the_message_for_a_tool_outside_the_grant_matches_use_case_dsl_md(
        self,
    ) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for ``TOOL_UNKNOWN``.

        Edit one without the other and this test is the gap the next review
        catches.
        """
        capability = make_mcp_capability(server="runbooks", include=("search_incident",))
        catalogue = (
            McpToolInfo(name="search_incident", has_output_schema=True),
            McpToolInfo(name="delete_incident", has_output_schema=True),
        )
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call_untyped("delete_incident", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert str(excinfo.value) == (
            "mcp server 'runbooks' grants no tool named 'delete_incident'; "
            "tools this grant admits: search_incident"
        )

    def test_tools_lists_only_what_the_filter_admits(self) -> None:
        capability = make_mcp_capability(include=("read_*",))
        catalogue = (
            McpToolInfo(name="read_orders", has_output_schema=True),
            McpToolInfo(name="write_orders", has_output_schema=True),
        )
        view = _view(
            capability=capability, session=FakeSession(tools=catalogue), catalogue=catalogue
        )

        assert view.tools() == ("read_orders",)


class TestTypedCall:
    """``call`` decodes the structured content into ``expect``."""

    async def test_decodes_the_structured_content(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            results={"severity": McpToolCallResult(ok=True, structured={"level": 4})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        result = await view.call("severity", {}, expect=_Severity)

        assert result == _Severity(level=4)

    async def test_a_schemaless_tool_is_rejected_before_the_network(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="untyped_tool", has_output_schema=False),)
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("untyped_tool", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNTYPED
        assert session.calls == []

    async def test_the_message_for_a_shapeless_tool_matches_use_case_dsl_md(self) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for ``TOOL_UNTYPED``."""
        capability = make_mcp_capability(server="runbooks")
        catalogue = (McpToolInfo(name="legacy_lookup", has_output_schema=False),)
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("legacy_lookup", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNTYPED
        assert str(excinfo.value) == (
            "tool 'legacy_lookup' of mcp server 'runbooks' publishes no output "
            "schema; call it with call_untyped() instead"
        )

    async def test_no_structured_content_is_rejected_as_unstructured(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue, results={"severity": McpToolCallResult(ok=True, structured=None)}
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED

    async def test_an_error_flag_is_reported_before_attempting_to_decode(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            # A payload that would fail to decode into '_Severity' too, so a
            # wrong precedence would report TOOL_DECODE_FAILED instead.
            results={"severity": McpToolCallResult(ok=False, structured={"level": "not-an-int"})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_CALL_FAILED

    async def test_content_that_does_not_fit_fails_to_decode(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            results={"severity": McpToolCallResult(ok=True, structured={"level": "not-an-int"})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_DECODE_FAILED
        assert "level" in str(excinfo.value)

    async def test_a_pydantic_expect_decodes_the_structured_content(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            results={"severity": McpToolCallResult(ok=True, structured={"level": 4})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        result = await view.call("severity", {}, expect=_SeverityModel)

        assert result == _SeverityModel(level=4)

    async def test_an_invalid_pydantic_result_fails_to_decode_without_echoing_the_input(
        self,
    ) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            results={
                "severity": McpToolCallResult(
                    ok=True, structured={"level": "another-callers-secret"}
                )
            },
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_SeverityModel)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_DECODE_FAILED
        message = str(excinfo.value)
        assert "level" in message
        assert "another-callers-secret" not in message

    async def test_a_mixed_union_expect_fails_before_calling_the_tool(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_Severity | _SeverityModel)  # type: ignore[arg-type]

        assert excinfo.value.code is AgentRunErrorCode.TOOL_DECODE_FAILED
        assert "mixes msgspec and pydantic" in str(excinfo.value)
        assert session.calls == []

    async def test_an_unsupported_expect_fails_with_a_decode_error_not_a_bare_type_error(
        self,
    ) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=object())  # type: ignore[arg-type]

        assert excinfo.value.code is AgentRunErrorCode.TOOL_DECODE_FAILED
        assert session.calls == []


class TestUntypedCall:
    async def test_returns_the_servers_json_without_requiring_a_schema(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="raw_tool", has_output_schema=False),)
        session = FakeSession(
            tools=catalogue,
            results={"raw_tool": McpToolCallResult(ok=True, structured={"anything": True})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        result = await view.call_untyped("raw_tool", {})

        assert result == {"anything": True}

    async def test_no_structured_content_returns_an_empty_map(self) -> None:
        """A legitimate absence of content — not a contradiction — folds to ``{}``."""
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="raw_tool", has_output_schema=False),)
        session = FakeSession(
            tools=catalogue,
            results={"raw_tool": McpToolCallResult(ok=True, structured=None)},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        result = await view.call_untyped("raw_tool", {})

        assert result == {}

    async def test_non_mapping_content_is_rejected_instead_of_discarded(self) -> None:
        """A list or a scalar contradicts the protocol; it does not fold to ``{}`` silently."""
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="raw_tool", has_output_schema=False),)
        session = FakeSession(
            tools=catalogue,
            results={"raw_tool": McpToolCallResult(ok=True, structured=["not", "a", "mapping"])},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call_untyped("raw_tool", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED
        assert "list" in str(excinfo.value)


def _sql_view(
    *, capability: CompiledSqlCapability, identity: Identity, service: SqlQueryService
) -> SqlGrantView:
    return SqlGrantView(
        agent=_AGENT,
        capability=capability,
        sql_query_service=service,
        identity=identity,
        observability=None,
    )


class FakeSqlQueryService(SqlQueryService):
    """A double of ``SqlQueryService`` returning a fixed result and recording roles."""

    def __init__(self, result: SqlQueryResult) -> None:
        super().__init__(executors={}, config=SqlConfig(connections={}))
        self._result = result
        self.roles: tuple[str, ...] | None = None

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        roles: Sequence[str] | None = None,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        del sql, connection, parameters, limit, offset
        self.roles = tuple(roles) if roles is not None else None
        return self._result


def _result(*rows: tuple[Any, ...]) -> SqlQueryResult:
    return SqlQueryResult(
        columns=(SqlColumn(name="id", type="Int64"), SqlColumn(name="name", type="String")),
        rows=rows,
        row_count=len(rows),
        limit=1000,
        offset=0,
        has_more=False,
        elapsed_ms=1.0,
    )


class TestTheSqlGrantsRoles:
    """Roles come from the verified caller, never from an argument."""

    async def test_runs_with_the_callers_roles_under_the_allowlist(self) -> None:
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst", "auditor"),
                readonly=True,
            ),
        )
        service = FakeSqlQueryService(_result())
        view = _sql_view(capability=capability, identity=_CALLER, service=service)

        await view.query("select 1")

        assert service.roles == ("analyst",)

    async def test_an_anonymous_caller_is_rejected_before_querying(self) -> None:
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst",),
                readonly=True,
            ),
        )
        service = FakeSqlQueryService(_result())
        view = _sql_view(capability=capability, identity=ANONYMOUS, service=service)

        with pytest.raises(AgentRunError) as excinfo:
            await view.query("select 1")

        assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert service.roles is None


class TestTheSqlResultBounds:
    """The byte bound truncates rather than rejecting."""

    async def test_trims_trailing_rows_to_stay_within_max_result_bytes(self) -> None:
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst",),
                readonly=True,
            ),
            max_result_bytes=80,
        )
        rows = tuple((i, f"row-{i}") for i in range(20))
        service = FakeSqlQueryService(_result(*rows))
        view = _sql_view(capability=capability, identity=_CALLER, service=service)

        result = await view.query("select * from t")

        assert len(result) < len(rows)
        assert len(msgspec.json.encode(list(result))) <= capability.max_result_bytes

    @pytest.mark.parametrize(
        ("max_result_bytes", "expected_rows"),
        [
            (20, 0),  # one byte short of the first row's own encoded array
            (21, 1),  # exactly the first row's own encoded array
            (40, 1),  # one byte short of both rows plus their separator
            (41, 2),  # exactly both rows plus their separator
        ],
    )
    async def test_the_cut_falls_exactly_at_the_byte_limit(
        self, max_result_bytes: int, expected_rows: int
    ) -> None:
        """Pins the exact boundary: the encoded array overhead and the row
        separator are counted once each, not approximated.
        """
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst",),
                readonly=True,
            ),
            max_result_bytes=max_result_bytes,
        )
        rows = ((1, "a"), (2, "b"))
        service = FakeSqlQueryService(_result(*rows))
        view = _sql_view(capability=capability, identity=_CALLER, service=service)

        result = await view.query("select * from t")

        assert len(result) == expected_rows
        assert len(msgspec.json.encode(list(result))) <= max_result_bytes
