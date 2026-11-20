"""Integration tests — REST observability: trace_id propagation and span events.

Verifies that:
  - ``TraceIdMiddleware`` echoes the incoming ``x-request-id`` back in the
    response header.
  - A missing header causes the middleware to generate a valid trace-id.
  - Error response bodies carry the same ``trace_id`` that was set by the
    middleware (end-to-end correlation).
  - ``ObservabilityRuntime.span()`` emits START → END/ERROR events to every
    observer wired into the REST layer.

Infrastructure:
    - Full ``create_fastapi_app`` stack with ``TraceIdMiddleware``
    - ``HttpTestHarness`` for repository injection
    - ``InMemoryRepository`` (zero-dependency)
    - In-process ``_RecordingObserver`` for span event assertions
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from loom.core.bootstrap import Kernel, create_kernel
from loom.core.errors import NotFound
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observability import ObservabilityRuntime
from loom.rest.compiler import RouteSources
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.middleware import TraceIdMiddleware
from loom.testing import HttpTestHarness, InMemoryRepository
from tests.integration.fake_repo.product.fake import InMemoryProductRepository
from tests.integration.fake_repo.product.interface import ProductRestInterface
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.repository_contract import ProductRepo
from tests.integration.fake_repo.product.use_cases import (
    CreateProductUseCase,
    DeleteProductUseCase,
    FindProductByNameUseCase,
    GetProductUseCase,
    ListProductsUseCase,
    UpdateProductUseCase,
)


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


def _product_kernel(harness: HttpTestHarness) -> Kernel:
    """A kernel serving every product use case from *harness*'s repositories.

    ``ProductRepo`` is bound to a plain in-memory repository: the harness binds
    its fakes under the model, which an error-forcing proxy would not carry to the
    protocol that ``FindProductByNameUseCase`` injects.
    """
    return create_kernel(
        config=object(),
        use_cases=[
            CreateProductUseCase,
            GetProductUseCase,
            UpdateProductUseCase,
            DeleteProductUseCase,
            ListProductsUseCase,
            FindProductByNameUseCase,
        ],
        modules=[
            harness._build_module(),
            lambda container: container.register_instance(ProductRepo, InMemoryProductRepository()),
        ],
    )


def _build_full_stack_client(
    repo: InMemoryRepository[Product] | None = None,
    error_on: tuple[str, Exception] | None = None,
    observer: _RecordingObserver | None = None,
) -> TestClient:
    """Build a TestClient with TraceIdMiddleware and optional observability."""
    harness = HttpTestHarness()
    harness.inject_repo(Product, repo or InMemoryProductRepository())
    if error_on is not None:
        method, exc = error_on
        harness.force_error(Product, method, exc)

    result = _product_kernel(harness)
    runtime = ObservabilityRuntime([observer]) if observer else ObservabilityRuntime.noop()
    app = create_fastapi_app(
        result,
        RouteSources(python=[ProductRestInterface]),
        observability_runtime=runtime,
        middleware=[TraceIdMiddleware],
    )
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Trace-id header propagation
# ---------------------------------------------------------------------------


class TestTraceIdPropagation:
    def test_client_trace_id_echoed_in_response_header(self) -> None:
        client = _build_full_stack_client()

        resp = client.post(
            "/products/",
            json={"name": "Widget", "price": 10.0},
            headers={"x-request-id": "my-trace-123"},
        )

        assert resp.status_code == 201
        assert resp.headers.get("x-request-id") == "my-trace-123"

    def test_missing_header_generates_trace_id_in_response(self) -> None:
        client = _build_full_stack_client()

        resp = client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert resp.status_code == 201
        trace_id = resp.headers.get("x-request-id")
        assert trace_id is not None
        assert len(trace_id) > 0

    def test_each_request_gets_distinct_trace_id(self) -> None:
        client = _build_full_stack_client()

        resp1 = client.post("/products/", json={"name": "A", "price": 1.0})
        resp2 = client.post("/products/", json={"name": "B", "price": 2.0})

        assert resp1.headers.get("x-request-id") != resp2.headers.get("x-request-id")


# ---------------------------------------------------------------------------
# Trace-id in error response body
# ---------------------------------------------------------------------------


class TestTraceIdInErrorBody:
    def test_error_body_carries_propagated_trace_id(self) -> None:
        client = _build_full_stack_client(error_on=("get_by_id", NotFound("Product", id=1)))

        resp = client.get(
            "/products/1",
            headers={"x-request-id": "error-trace-xyz"},
        )

        assert resp.status_code == 404
        detail = resp.json()["detail"]
        assert detail["trace_id"] == "error-trace-xyz"

    def test_error_body_has_non_null_trace_id_without_header(self) -> None:
        client = _build_full_stack_client(error_on=("get_by_id", NotFound("Product", id=99)))

        resp = client.get("/products/99")

        assert resp.status_code == 404
        detail = resp.json()["detail"]
        assert detail["trace_id"] is not None


# ---------------------------------------------------------------------------
# ObservabilityRuntime span events through REST layer
# ---------------------------------------------------------------------------


class TestSpanEventsFromRESTLayer:
    def test_successful_request_emits_start_then_end(self) -> None:
        observer = _RecordingObserver()
        client = _build_full_stack_client(observer=observer)

        resp = client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert resp.status_code == 201
        kinds = [e.kind for e in observer.events]
        assert EventKind.START in kinds
        assert EventKind.END in kinds
        assert EventKind.ERROR not in kinds

    def test_failed_request_emits_start_then_error(self) -> None:
        observer = _RecordingObserver()
        client = _build_full_stack_client(
            error_on=("get_by_id", NotFound("Product", id=1)),
            observer=observer,
        )

        resp = client.get("/products/1")

        assert resp.status_code == 404
        kinds = [e.kind for e in observer.events]
        assert EventKind.START in kinds
        assert EventKind.ERROR in kinds

    def test_span_scope_is_use_case(self) -> None:
        observer = _RecordingObserver()
        client = _build_full_stack_client(observer=observer)

        client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert all(e.scope == Scope.USE_CASE for e in observer.events)

    def test_end_event_has_positive_duration_ms(self) -> None:
        observer = _RecordingObserver()
        client = _build_full_stack_client(observer=observer)

        client.post("/products/", json={"name": "Widget", "price": 10.0})

        end_events = [e for e in observer.events if e.kind == EventKind.END]
        assert end_events, "no END event emitted"
        assert end_events[0].duration_ms is not None
        assert end_events[0].duration_ms >= 0

    def test_span_name_matches_use_case_class(self) -> None:
        observer = _RecordingObserver()
        client = _build_full_stack_client(observer=observer)

        client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert all(e.name == "CreateProductUseCase" for e in observer.events)

    def test_broken_observer_does_not_break_request(self) -> None:
        class _BrokenObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                raise RuntimeError("observer crash")

        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryProductRepository())
        result = _product_kernel(harness)
        runtime = ObservabilityRuntime([_BrokenObserver()])
        app = create_fastapi_app(
            result,
            RouteSources(python=[ProductRestInterface]),
            observability_runtime=runtime,
            middleware=[TraceIdMiddleware],
        )
        client = TestClient(app, raise_server_exceptions=False)

        resp = client.post("/products/", json={"name": "Widget", "price": 10.0})

        assert resp.status_code == 201
