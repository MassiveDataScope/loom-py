"""Unit tests for bind_interfaces and create_fastapi_app."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum, StrEnum
from typing import Annotated, Any

import msgspec
import pydantic
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic.alias_generators import to_camel

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.bootstrap.bootstrap import BootstrapResult, bootstrap_app
from loom.core.command import Command, Internal
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.model import BaseModel, ColumnField, LoomStruct, String
from loom.core.observability.event import EventKind as ObservabilityEventKind
from loom.core.observability.event import LifecycleEvent as ObservabilityLifecycleEvent
from loom.core.observability.event import Scope as ObservabilityScope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.abc import CursorCodec
from loom.core.repository.abc.query import (
    PageParams,
    PageResult,
    PaginationMode,
    QuerySpec,
    build_page_result,
)
from loom.core.repository.sqlalchemy.query_compiler import QuerySpecCompiler
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.field_ref import F
from loom.core.use_case.markers import Exists, Input
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import (
    CompiledRoute,
    InterfaceCompilationError,
    RestInterfaceCompiler,
    RouteSources,
)
from loom.rest.fastapi.app import _register_openapi_components, create_fastapi_app
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import ALL, RestApiDefaults, RestInterface, RestRoute

# ---------------------------------------------------------------------------
# Dummy use cases
# ---------------------------------------------------------------------------


class PingUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        await asyncio.sleep(0)
        return "pong"


class EchoUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"status": "received"}


class CreateItemCmd(Command, frozen=True):
    full_name: str
    tenant_id: Internal[int] = 0


class CreateItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, cmd: CreateItemCmd = Input()) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"full_name": cmd.full_name}


class ItemModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", alias_generator=to_camel, populate_by_name=True
    )

    full_name: str


class ItemRootModel(pydantic.RootModel[ItemModel]):
    pass


class ModelResultUseCase(UseCase[Any, ItemModel]):
    async def execute(self, **kwargs: Any) -> ItemModel:
        await asyncio.sleep(0)
        return ItemModel(full_name="Alice")


class ModelListResultUseCase(UseCase[Any, list[ItemModel]]):
    async def execute(self, **kwargs: Any) -> list[ItemModel]:
        await asyncio.sleep(0)
        return [ItemModel(full_name="Alice"), ItemModel(full_name="Bob")]


class RootModelResultUseCase(UseCase[Any, ItemRootModel]):
    async def execute(self, **kwargs: Any) -> ItemRootModel:
        await asyncio.sleep(0)
        return ItemRootModel(ItemModel(full_name="Alice"))


@dataclass(frozen=True)
class PlainCreateCmd:
    full_name: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> tuple[PlainCreateCmd, frozenset[str]]:
        return cls(full_name=str(payload["full_name"])), frozenset({"full_name"})


class PlainCreateUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, cmd: PlainCreateCmd = Input()) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"full_name": cmd.full_name}


class ItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, item_id: str, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"item_id": item_id}


class IntItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, item_id: int, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"item_id": item_id}


class ProfileAwareUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, profile: str, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"profile": profile}


class QueryAwareUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, query: QuerySpec, **kwargs: Any) -> dict[str, Any]:
        first_filter = query.filters.filters[0] if query.filters else None
        first_sort = query.sort[0] if query.sort else None
        return {
            "pagination": query.pagination.value,
            "page": query.page,
            "limit": query.limit,
            "cursor": query.cursor,
            "filter_field": first_filter.field if first_filter else None,
            "filter_op": first_filter.op.value if first_filter else None,
            "filter_value": first_filter.value if first_filter else None,
            "sort_field": first_sort.field if first_sort else None,
            "sort_direction": first_sort.direction if first_sort else None,
        }


class _FilterRow(BaseModel):
    __tablename__ = "router_runtime_filter_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    rank: int = ColumnField()


class CompiledFilterUseCase(UseCase[_FilterRow, str]):
    allowed_fields: frozenset[str] = frozenset()

    def __init__(self) -> None:
        super().__init__()

    async def execute(self, query: QuerySpec, **kwargs: Any) -> str:
        compile_all(_FilterRow)
        sa_model = get_compiled(_FilterRow)
        assert sa_model is not None
        assert query.filters is not None
        compiler = QuerySpecCompiler(
            sa_model,
            sa_model.id,
            self.allowed_fields,
            backend="sqlalchemy",
            model=_FilterRow,
            cursor_codec=CursorCodec(signing="ephemeral"),
            scope="",
            columns={},
        )
        return str(compiler.compile_filter(query.filters))


class IdOnlyFilterUseCase(CompiledFilterUseCase):
    allowed_fields = frozenset({"id"})


class _Tone(Enum):
    LIGHT = "light"
    DARK = "dark"


class _Phase(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


class _Entry(BaseModel):
    __tablename__ = "router_runtime_entries"

    id: int = ColumnField(primary_key=True)
    code: str = ColumnField()
    rank: Annotated[int, msgspec.Meta(ge=1)] = ColumnField()
    quota: Annotated[int, msgspec.Meta(ge=1)] | None = ColumnField()
    active: bool = ColumnField()
    tone: _Tone = ColumnField(String(5))
    phase: _Phase = ColumnField()
    due: date = ColumnField()
    seen_at: datetime = ColumnField()
    sort_order: int = ColumnField()


FILTERS_SEEN: list[dict[str, Any]] = []
"""The filter values each entry filter double received, in call order."""


class EntryFilterValuesUseCase(UseCase[_Entry, PageResult[_Entry]]):
    """Answers with entries, so a column it filters on must be one they show."""

    def __init__(self) -> None:
        super().__init__()

    async def execute(self, query: QuerySpec, **kwargs: Any) -> PageResult[_Entry]:
        await asyncio.sleep(0)
        assert query.filters is not None
        FILTERS_SEEN.append(self.seen({spec.field: spec.value for spec in query.filters.filters}))
        return build_page_result([], 0, PageParams(page=1, limit=1))

    def seen(self, values: dict[str, Any]) -> dict[str, Any]:
        return values


class EntryFilterTypesUseCase(EntryFilterValuesUseCase):
    def seen(self, values: dict[str, Any]) -> dict[str, Any]:
        return {field: type(value).__name__ for field, value in values.items()}


def _filters_seen(
    query: str, use_case: type[EntryFilterValuesUseCase] = EntryFilterValuesUseCase
) -> dict[str, Any]:
    FILTERS_SEEN.clear()
    response = _entry_client(use_case).get(f"/entries/?{query}")
    assert response.status_code == 200, response.text
    (values,) = FILTERS_SEEN
    return values


SORTS_SEEN: list[list[list[str]]] = []
"""The sort each ``EntrySortUseCase`` call received, in call order."""


class EntrySortUseCase(UseCase[_Entry, PageResult[_Entry]]):
    """Answers with entries, so a column it sorts by must be one they show."""

    def __init__(self) -> None:
        super().__init__()

    async def execute(self, query: QuerySpec, **kwargs: Any) -> PageResult[_Entry]:
        await asyncio.sleep(0)
        SORTS_SEEN.append([[spec.field, spec.direction] for spec in query.sort])
        return build_page_result([], 0, PageParams(page=1, limit=1))


class DocUseCase(UseCase[Any, str]):
    """Doc summary from use case.

    Extended documentation for OpenAPI description.
    """

    async def execute(self, **kwargs: Any) -> str:
        await asyncio.sleep(0)
        return "ok"


class _ExistsUserRecord(LoomStruct):
    email: str


class _ExistsCheckEmailCmd(Command, frozen=True):
    email: str


class _ExistsCheckEmailUseCase(UseCase[_ExistsUserRecord, bool]):
    async def execute(
        self,
        cmd: _ExistsCheckEmailCmd = Input(),
        email_exists: bool = Exists(_ExistsUserRecord, from_command="email", against="email"),
    ) -> bool:
        await asyncio.sleep(0)
        return email_exists


class _AutoItem(LoomStruct):
    id: int
    name: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uc_compiler(*types: type[UseCase[Any, Any]]) -> UseCaseCompiler:
    c = UseCaseCompiler()
    for t in types:
        c.compile(t)
    return c


def _factory(uc_compiler: UseCaseCompiler, *types: type[UseCase[Any, Any]]) -> UseCaseFactory:
    container = LoomContainer()
    factory = UseCaseFactory(container)
    for t in types:
        factory.register(t)
    return factory


def _resolve_schema(schema_ref: dict[str, Any], openapi_doc: dict[str, Any]) -> dict[str, Any]:
    """Follow a ``$ref`` to its component definition when present."""
    ref = schema_ref.get("$ref", "")
    if ref.startswith("#/components/schemas/"):
        name = ref.removeprefix("#/components/schemas/")
        return openapi_doc["components"]["schemas"][name]  # type: ignore[no-any-return]
    return schema_ref


def _make_app(
    *compiled_routes: CompiledRoute,
    observability_runtime: ObservabilityRuntime | None = None,
    **factory_types: Any,
) -> FastAPI:
    """Build a minimal FastAPI app from pre-compiled routes."""
    # Extract use case types from compiled routes
    types = [cr.route.use_case for cr in compiled_routes]
    uc_comp = _uc_compiler(*types)
    factory = _factory(uc_comp, *types)
    executor = RuntimeExecutor(uc_comp)
    app = FastAPI()
    component_registry = bind_interfaces(
        app,
        list(compiled_routes),
        factory,
        executor,
        observability_runtime=observability_runtime or ObservabilityRuntime.noop(),
    )
    if component_registry:
        _register_openapi_components(app, component_registry)
    return app


def _compile_routes(
    interface: type[RestInterface[Any]],
    *extra_use_cases: type[UseCase[Any, Any]],
) -> list[CompiledRoute]:
    """Compile a RestInterface, pre-compiling all declared use cases."""
    use_case_types = [r.use_case for r in interface.routes] + list(extra_use_cases)
    uc_comp = _uc_compiler(*use_case_types)
    return RestInterfaceCompiler(uc_comp).compile(interface)


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[ObservabilityLifecycleEvent] = []

    def on_event(self, event: ObservabilityLifecycleEvent) -> None:
        self.events.append(event)


# ---------------------------------------------------------------------------
# bind_interfaces — basic routing
# ---------------------------------------------------------------------------


def test_bind_interfaces_registers_route() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/")
    assert response.status_code == 200
    assert response.json() == "pong"


def test_bind_interfaces_content_type_is_json() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/")
    assert "application/json" in response.headers["content-type"]


def test_bind_interfaces_path_param_extracted() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=ItemUseCase, method="GET", path="/{item_id}"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/42")
    assert response.status_code == 200
    assert response.json()["item_id"] == "42"


def test_bind_interfaces_path_param_typed_from_execute_signature() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/typed-items"
        routes = (RestRoute(use_case=IntItemUseCase, method="GET", path="/{item_id}"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/typed-items/42")
    assert response.status_code == 200
    assert response.json()["item_id"] == 42


def test_bind_interfaces_path_param_validation_uses_typed_signature() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/typed-items"
        routes = (RestRoute(use_case=IntItemUseCase, method="GET", path="/{item_id}"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/typed-items/not-an-int")
    assert response.status_code == 422


def test_bind_interfaces_status_code_respected() -> None:
    class IFace(RestInterface[str]):
        prefix = "/things"
        routes = (RestRoute(use_case=PingUseCase, method="POST", path="/", status_code=201),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.post("/things/")
    assert response.status_code == 201


def test_bind_interfaces_multiple_routes() -> None:
    class IFace(RestInterface[Any]):
        prefix = "/v1"
        routes = (
            RestRoute(use_case=PingUseCase, method="GET", path="/ping"),
            RestRoute(use_case=ItemUseCase, method="GET", path="/items/{item_id}"),
        )

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    assert client.get("/v1/ping").json() == "pong"
    assert client.get("/v1/items/7").json()["item_id"] == "7"


# ---------------------------------------------------------------------------
# bind_interfaces — POST with body
# ---------------------------------------------------------------------------


def test_bind_interfaces_request_body_decoded() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/echo"
        routes = (RestRoute(use_case=EchoUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.post("/echo/", json={"name": "loom"})
    assert response.status_code == 200
    assert response.json() == {"status": "received"}


def test_bind_interfaces_empty_body_no_payload() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/")
    assert response.status_code == 200


def test_bind_interfaces_rejects_profile_query_when_not_exposed() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/?profile=detail")
    assert response.status_code == 400
    assert "not allowed" in response.json()["detail"]


def test_bind_interfaces_passes_default_profile_when_use_case_accepts_it() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        profile_default = "summary"
        routes = (RestRoute(use_case=ProfileAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/")
    assert response.status_code == 200
    assert response.json() == {"profile": "summary"}


def test_bind_interfaces_accepts_profile_query_when_exposed() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        expose_profile = True
        allowed_profiles = ("summary", "detail")
        profile_default = "summary"
        routes = (RestRoute(use_case=ProfileAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?profile=detail")
    assert response.status_code == 200
    assert response.json() == {"profile": "detail"}


def test_bind_interfaces_rejects_disallowed_profile_query() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        expose_profile = True
        allowed_profiles = ("summary",)
        routes = (RestRoute(use_case=ProfileAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?profile=detail")
    assert response.status_code == 400
    assert "Invalid profile" in response.json()["detail"]


def test_bind_interfaces_builds_query_spec_from_offset_url_params() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?page=2&limit=10")
    assert response.status_code == 200
    assert response.json() == {
        "pagination": PaginationMode.OFFSET.value,
        "page": 2,
        "limit": 10,
        "cursor": None,
        "filter_field": None,
        "filter_op": None,
        "filter_value": None,
        "sort_field": None,
        "sort_direction": None,
    }


def test_bind_interfaces_builds_query_spec_from_cursor_url_params() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?after=abc123&limit=1")
    assert response.status_code == 200
    assert response.json() == {
        "pagination": PaginationMode.CURSOR.value,
        "page": 1,
        "limit": 1,
        "cursor": "abc123",
        "filter_field": None,
        "filter_op": None,
        "filter_value": None,
        "sort_field": None,
        "sort_direction": None,
    }


def test_bind_interfaces_refuses_a_page_whose_offset_overflows() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    client = TestClient(_make_app(*_compile_routes(IFace)), raise_server_exceptions=False)

    response = client.get(f"/items/?page={2**63}&limit=2")

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "query_validation"
    assert [violation["field"] for violation in detail["violations"]] == ["page"]


def test_bind_interfaces_uses_compiled_default_pagination_mode() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        pagination_mode = PaginationMode.CURSOR
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?limit=1")
    assert response.status_code == 200
    assert response.json()["pagination"] == PaginationMode.CURSOR.value


def test_bind_interfaces_rejects_pagination_override_when_disabled() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        pagination_mode = PaginationMode.OFFSET
        allow_pagination_override = False
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?pagination=cursor")
    assert response.status_code == 400
    assert "cannot override" in response.json()["detail"]


def test_bind_interfaces_answers_400_when_the_repository_refuses_a_filter_field() -> None:
    class IFace(RestInterface[str]):
        prefix = "/rows"
        filterable = ALL
        routes = (RestRoute(use_case=IdOnlyFilterUseCase, method="GET", path="/"),)

    client = TestClient(_make_app(*_compile_routes(IFace)), raise_server_exceptions=False)

    response = client.get("/rows/?rank=7")

    assert response.status_code == 400
    body = response.json()["detail"]
    assert body["code"] == "unsupported_query"
    assert "rank" in body["message"]


def _entry_client(
    use_case: type[UseCase[_Entry, Any]] = EntryFilterValuesUseCase,
) -> TestClient:
    class IFace(RestInterface[_Entry]):
        prefix = "/entries"
        filterable = (
            F(_Entry).code,
            F(_Entry).rank,
            F(_Entry).quota,
            "active",
            "tone",
            "phase",
            "due",
            "seen_at",
            "sort_order",
        )
        routes = (
            RestRoute(
                use_case=use_case,
                method="GET",
                path="/",
                sortable=(F(_Entry).seen_at,) if use_case is EntrySortUseCase else None,
            ),
        )

    return TestClient(_make_app(*_compile_routes(IFace)), raise_server_exceptions=False)


def _refusal(response: Any) -> dict[str, Any]:
    assert response.status_code == 400
    detail: dict[str, Any] = response.json()["detail"]
    assert detail["code"] == "unsupported_query"
    return {key: value for key, value in detail.items() if key != "trace_id"}


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("sort=seenAt", [["seen_at", "ASC"]]),
        ("sort=id&direction=DESC", [["id", "DESC"]]),
    ],
)
def test_bind_interfaces_sorts_by_a_declared_column_or_the_primary_key(
    query: str, expected: list[list[str]]
) -> None:
    SORTS_SEEN.clear()
    response = _entry_client(EntrySortUseCase).get(f"/entries/?{query}")

    assert response.status_code == 200
    assert [expected] == SORTS_SEEN


def test_bind_interfaces_refuses_a_sort_on_any_undeclared_column_alike() -> None:
    client = _entry_client(EntrySortUseCase)

    existing = _refusal(client.get("/entries/?sort=code"))
    missing = _refusal(client.get("/entries/?sort=nope"))

    assert existing["message"].replace("'code'", "'nope'") == missing["message"]


@pytest.mark.parametrize("query", ["id=1", "id__like=1%25", "id__is_null=true"])
def test_bind_interfaces_refuses_a_filter_on_an_undeclared_field_for_every_operator(
    query: str,
) -> None:
    detail = _refusal(_entry_client().get(f"/entries/?{query}"))

    assert detail["message"] == "Filtering on field 'id' is not allowed by this route."


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("code__exists=true", "Operator 'exists' is not supported by this route."),
        ("code__not_exists=true", "Operator 'not_exists' is not supported by this route."),
        ("rank__like=1%25", "Operator 'like' applies to text fields only."),
        ("rank__ilike=1%25", "Operator 'ilike' applies to text fields only."),
    ],
)
def test_bind_interfaces_refuses_an_operator_the_declared_field_cannot_take(
    query: str, message: str
) -> None:
    assert _refusal(_entry_client().get(f"/entries/?{query}"))["message"] == message


@pytest.mark.parametrize("raw", ["0123", "true", "1.5"])
def test_bind_interfaces_keeps_a_string_field_filter_value_as_sent(raw: str) -> None:
    assert _filters_seen(f"code={raw}") == {"code": raw}


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("rank=7", {"rank": 7}),
        ("sortOrder=4", {"sort_order": 4}),
        ("quota__gt=0", {"quota": 0}),
        ("code__like=01%25", {"code": "01%"}),
    ],
)
def test_bind_interfaces_converts_a_filter_value_by_its_field_type(
    query: str, expected: dict[str, Any]
) -> None:
    assert _filters_seen(query) == expected


def test_bind_interfaces_builds_enum_and_temporal_filter_values() -> None:
    seen = _filters_seen(
        "tone=dark&due=2026-01-02&seen_at__gte=2026-01-02T03:04:05Z", EntryFilterTypesUseCase
    )

    assert seen == {"tone": "_Tone", "due": "date", "seen_at": "datetime"}


def test_bind_interfaces_filters_a_str_enum_column_declared_filterable_by_its_value() -> None:
    assert _filters_seen("phase=closed") == {"phase": "closed"}
    assert _filters_seen("phase=closed", EntryFilterTypesUseCase) == {"phase": "_Phase"}


@pytest.mark.parametrize(
    ("query", "field"),
    [("rank=s3cr3t", "rank"), ("active=1", "active"), ("rank__in=1,s3cr3t", "rank")],
)
def test_bind_interfaces_answers_400_when_a_filter_value_is_not_its_field_type(
    query: str, field: str
) -> None:
    response = _entry_client().get(f"/entries/?{query}")

    assert response.status_code == 400
    body = response.json()["detail"]
    assert body["code"] == "query_validation"
    assert [violation["field"] for violation in body["violations"]] == [field]
    assert "s3cr3t" not in response.text


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("rank__in=1,2,3", {"rank": [1, 2, 3]}),
        ("code__in=01,true,2.5", {"code": ["01", "true", "2.5"]}),
    ],
)
def test_bind_interfaces_converts_each_in_filter_item_by_its_field_type(
    query: str, expected: dict[str, Any]
) -> None:
    assert _filters_seen(query) == expected


# ---------------------------------------------------------------------------
# bind_interfaces — tags propagated to OpenAPI
# ---------------------------------------------------------------------------


def test_bind_interfaces_tags_on_routes() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        tags = ("Pings",)
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    # Verify OpenAPI schema has the tag on the route
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/ping/"]["get"]
    assert "Pings" in operation.get("tags", [])


def test_bind_interfaces_uses_use_case_docstring_for_route_docs() -> None:
    class IFace(RestInterface[str]):
        prefix = "/docs"
        routes = (RestRoute(use_case=DocUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/docs/"]["get"]
    assert operation["summary"] == "Doc summary from use case."
    assert "Extended documentation" in operation["description"]


def test_openapi_response_schema_inferred_from_use_case_return_type() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    response_schema = schema["paths"]["/ping/"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert response_schema["type"] == "string"


def test_openapi_request_schema_from_command_excludes_internal_fields() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=CreateItemUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    raw_schema = schema["paths"]["/items/"]["post"]["requestBody"]["content"]["application/json"][
        "schema"
    ]
    request_schema = _resolve_schema(raw_schema, schema)
    properties = request_schema["properties"]
    assert "fullName" in properties
    assert "tenant_id" not in properties


def test_openapi_request_schema_for_plain_command_type() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/plain"
        routes = (RestRoute(use_case=PlainCreateUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    raw_schema = schema["paths"]["/plain/"]["post"]["requestBody"]["content"]["application/json"][
        "schema"
    ]
    request_schema = _resolve_schema(raw_schema, schema)
    assert request_schema["type"] == "object"
    assert "full_name" in request_schema["properties"]


# ---------------------------------------------------------------------------
# bind_interfaces — boundary validation (a Command body that fails to convert)
# ---------------------------------------------------------------------------


def test_bind_interfaces_bad_body_field_maps_to_422_boundary_validation() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=CreateItemUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.post("/items/", json={"fullName": 12})
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["code"] == "boundary_validation"
    assert detail["violations"] == [{"field": "fullName", "message": "Expected `str`, got `int`"}]


def test_bind_interfaces_missing_body_field_reports_that_field() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=CreateItemUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.post("/items/", json={})
    assert response.status_code == 422
    assert response.json()["detail"]["violations"][0]["field"] == "fullName"


# ---------------------------------------------------------------------------
# bind_interfaces — a strict pydantic.BaseModel result renders through the loom type
# ---------------------------------------------------------------------------


def test_bind_interfaces_basemodel_result_renders_as_json_object() -> None:
    class IFace(RestInterface[Any]):
        prefix = "/model"
        routes = (RestRoute(use_case=ModelResultUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/model/")
    assert response.status_code == 200
    assert response.json() == {"fullName": "Alice"}


# ---------------------------------------------------------------------------
# OpenAPI — response schema for a pydantic return type
# ---------------------------------------------------------------------------


def test_openapi_response_schema_for_basemodel_return_type() -> None:
    class IFace(RestInterface[Any]):
        prefix = "/model"
        routes = (RestRoute(use_case=ModelResultUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    raw_schema = schema["paths"]["/model/"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    response_schema = _resolve_schema(raw_schema, schema)
    assert "fullName" in response_schema["properties"]
    assert "title" not in response_schema


def test_openapi_response_schema_for_basemodel_list_return_type() -> None:
    class IFace(RestInterface[Any]):
        prefix = "/models"
        routes = (RestRoute(use_case=ModelListResultUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    response_schema = schema["paths"]["/models/"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert response_schema["type"] == "array"
    items_schema = _resolve_schema(response_schema["items"], schema)
    assert "fullName" in items_schema["properties"]


def test_openapi_response_schema_for_root_model_return_type() -> None:
    class IFace(RestInterface[Any]):
        prefix = "/root"
        routes = (RestRoute(use_case=RootModelResultUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    raw_schema = schema["paths"]["/root/"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    response_schema = _resolve_schema(raw_schema, schema)
    assert "fullName" in response_schema["properties"]


def test_openapi_autocrud_list_has_response_schema_and_query_parameters() -> None:
    class IFace(RestInterface[_AutoItem]):
        prefix = "/auto-items"
        auto = True
        include = ("list",)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/auto-items/"]["get"]

    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema

    parameter_names = {parameter["name"] for parameter in operation["parameters"]}
    expected_params = {"page", "limit", "pagination", "after", "cursor", "sort", "direction"}
    assert expected_params <= parameter_names


def test_openapi_autocrud_summary_uses_generated_docstring() -> None:
    class IFace(RestInterface[_AutoItem]):
        prefix = "/auto-docs"
        auto = True
        include = ("list",)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/auto-docs/"]["get"]
    assert operation["summary"] == "List _AutoItem with filtering, sorting and pagination."


def test_openapi_autocrud_get_path_param_uses_model_id_type() -> None:
    class IFace(RestInterface[_AutoItem]):
        prefix = "/auto-item"
        auto = True
        include = ("get",)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/auto-item/{id}"]["get"]
    id_param = next(param for param in operation["parameters"] if param["name"] == "id")
    assert id_param["schema"]["type"] == "integer"


# ---------------------------------------------------------------------------
# create_fastapi_app — integration
# ---------------------------------------------------------------------------


class _FakeConfig:
    debug: bool = False


def _bootstrap(*use_cases: type[UseCase[Any, Any]]) -> BootstrapResult:
    return bootstrap_app(config=_FakeConfig(), use_cases=list(use_cases))


def test_create_fastapi_app_returns_fastapi_instance() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[IFace]),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    assert isinstance(app, FastAPI)


def test_create_fastapi_app_emits_use_case_span() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[IFace]),
        observability_runtime=runtime,
    )
    client = TestClient(app)
    response = client.get("/ping/")

    assert response.status_code == 200
    assert [event.kind for event in observer.events] == [
        ObservabilityEventKind.START,
        ObservabilityEventKind.END,
    ]
    assert observer.events[0].scope is ObservabilityScope.USE_CASE
    assert observer.events[0].name == "PingUseCase"
    assert observer.events[0].meta["route"] == "/ping/"
    assert observer.events[1].meta["status_code"] == 200


def test_use_case_span_nests_under_the_request_span_of_a_host_sdk() -> None:
    """A host's request span parents the use case span, in the host's own trace.

    The ASGI middleware stands in for what an OTEL HTTP instrumentation does in
    production. Loom never installs a tracer provider globally, so the two
    providers export separately while sharing one context and one trace.
    """

    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    host_exporter = InMemorySpanExporter()
    host_provider = TracerProvider()
    host_provider.add_span_processor(SimpleSpanProcessor(host_exporter))
    loom_exporter = InMemorySpanExporter()
    loom_provider = TracerProvider()
    loom_provider.add_span_processor(SimpleSpanProcessor(loom_exporter))
    host_tracer = host_provider.get_tracer("host.sdk")

    class _HostSpanMiddleware:
        """Pure-ASGI middleware, so its span is current in the endpoint's task."""

        def __init__(self, app: Any) -> None:
            self.app = app

        async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
            if scope["type"] != "http":
                await self.app(scope, receive, send)
                return
            with host_tracer.start_as_current_span("host:request"):
                await self.app(scope, receive, send)

    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[IFace]),
        observability_runtime=ObservabilityRuntime(
            [], tracer=loom_provider.get_tracer("loom.rest")
        ),
    )
    app.add_middleware(_HostSpanMiddleware)

    assert TestClient(app).get("/ping/").status_code == 200

    host_spans = host_exporter.get_finished_spans()
    loom_spans = loom_exporter.get_finished_spans()
    assert [span.name for span in host_spans] == ["host:request"]
    assert [span.name for span in loom_spans] == ["use_case:PingUseCase"]

    request_context = host_spans[0].get_span_context()
    use_case_context = loom_spans[0].get_span_context()
    use_case_parent = loom_spans[0].parent
    assert request_context is not None
    assert use_case_context is not None
    assert use_case_parent is not None
    assert use_case_parent.span_id == request_context.span_id
    assert use_case_context.trace_id == request_context.trace_id


def test_create_fastapi_app_routes_work() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[IFace]),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    client = TestClient(app)
    assert client.get("/ping/").json() == "pong"


def test_create_fastapi_app_accepts_fastapi_kwargs() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[IFace]),
        observability_runtime=ObservabilityRuntime.noop(),
        title="My API",
        version="2.0.0",
    )
    assert app.title == "My API"
    assert app.version == "2.0.0"


class TestDeprecatedInterfacesKeyword:
    """B1: the pre-``RouteSources`` ``interfaces=`` keyword keeps working."""

    def test_interfaces_keyword_mounts_the_same_routes_as_routes(self) -> None:
        class IFace(RestInterface[str]):
            prefix = "/ping"
            routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

        result = _bootstrap(PingUseCase)
        with pytest.deprecated_call():
            app = create_fastapi_app(
                result,
                interfaces=[IFace],
                observability_runtime=ObservabilityRuntime.noop(),
            )
        client = TestClient(app)
        assert client.get("/ping/").json() == "pong"

    def test_interfaces_keyword_warns_naming_routes_as_the_replacement(self) -> None:
        class IFace(RestInterface[str]):
            prefix = "/ping"
            routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

        result = _bootstrap(PingUseCase)
        observability_runtime = ObservabilityRuntime.noop()
        with pytest.warns(DeprecationWarning, match="routes=RouteSources"):
            create_fastapi_app(
                result,
                interfaces=[IFace],
                observability_runtime=observability_runtime,
            )

    def test_interfaces_keyword_warning_points_at_the_caller_not_this_module(self) -> None:
        """L8: stacklevel must land on this test's call site, not app.py's internals."""

        class IFace(RestInterface[str]):
            prefix = "/ping"
            routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

        result = _bootstrap(PingUseCase)
        observability_runtime = ObservabilityRuntime.noop()
        with pytest.warns(DeprecationWarning) as record:
            create_fastapi_app(
                result,
                interfaces=[IFace],
                observability_runtime=observability_runtime,
            )

        assert record[0].filename == __file__

    def test_neither_routes_nor_interfaces_raises_type_error(self) -> None:
        result = _bootstrap(PingUseCase)
        observability_runtime = ObservabilityRuntime.noop()
        with pytest.raises(TypeError, match="requires 'routes'"):
            create_fastapi_app(result, observability_runtime=observability_runtime)

    def test_the_old_positional_sequence_names_its_replacement(self) -> None:
        class IFace(RestInterface[str]):
            prefix = "/ping"
            routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

        result = _bootstrap(PingUseCase)
        observability_runtime = ObservabilityRuntime.noop()
        with pytest.raises(TypeError, match="RouteSources"):
            create_fastapi_app(
                result,
                [IFace],  # type: ignore[arg-type]
                observability_runtime=observability_runtime,
            )

    def test_both_routes_and_interfaces_raises_type_error(self) -> None:
        class IFace(RestInterface[str]):
            prefix = "/ping"
            routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

        result = _bootstrap(PingUseCase)
        route_sources = RouteSources(python=[IFace])
        observability_runtime = ObservabilityRuntime.noop()
        with pytest.raises(TypeError, match="not both"):
            create_fastapi_app(
                result,
                routes=route_sources,
                interfaces=[IFace],
                observability_runtime=observability_runtime,
            )


def test_create_fastapi_app_multiple_interfaces() -> None:
    class PingIFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    class ItemIFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=ItemUseCase, method="GET", path="/{item_id}"),)

    result = _bootstrap(PingUseCase, ItemUseCase)
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[PingIFace, ItemIFace]),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    client = TestClient(app)
    assert client.get("/ping/").json() == "pong"
    assert client.get("/items/99").json()["item_id"] == "99"


def test_create_fastapi_app_uncompiled_use_case_raises() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    # bootstrap with NO use cases — PingUseCase not compiled
    result = _bootstrap()
    observability_runtime = ObservabilityRuntime.noop()
    route_sources = RouteSources(python=[IFace])
    with pytest.raises(InterfaceCompilationError, match="not been compiled"):
        create_fastapi_app(
            result,
            routes=route_sources,
            observability_runtime=observability_runtime,
        )


def test_create_fastapi_app_accepts_defaults() -> None:
    from loom.rest.model import PaginationMode

    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    defaults = RestApiDefaults(pagination_mode=PaginationMode.CURSOR)
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[IFace]),
        observability_runtime=ObservabilityRuntime.noop(),
        defaults=defaults,
    )
    assert isinstance(app, FastAPI)


def test_create_fastapi_app_exists_marker_uses_registered_repository() -> None:
    class _UsersIFace(RestInterface[bool]):
        prefix = "/users"
        routes = (RestRoute(use_case=_ExistsCheckEmailUseCase, method="POST", path="/exists"),)

    class _FakeRepo:
        model = _ExistsUserRecord

        async def exists_by(self, field: str, value: Any) -> bool:
            await asyncio.sleep(0)
            return field == "email" and value == "taken@example.com"

    _repo = _FakeRepo()
    _token = ("repo", _ExistsUserRecord)

    def _register_repo(container: LoomContainer) -> None:
        container.register(_token, lambda: _repo, scope=Scope.APPLICATION)
        container.register_repo(_ExistsUserRecord, _token)

    result = bootstrap_app(
        config=_FakeConfig(),
        use_cases=[_ExistsCheckEmailUseCase],
        modules=[_register_repo],
    )
    app = create_fastapi_app(
        result,
        routes=RouteSources(python=[_UsersIFace]),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    client = TestClient(app)

    response = client.post("/users/exists", json={"email": "taken@example.com"})
    assert response.status_code == 200
    assert response.json() is True
