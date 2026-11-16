"""Unit tests for HttpTestHarness."""

from __future__ import annotations

from typing import Any, Protocol, cast

import pytest

from loom.composition import Application, Project
from loom.composition.recipe import Adapter
from loom.core.command.base import Command
from loom.core.errors import Conflict, NotFound
from loom.core.model import LoomStruct
from loom.core.stores.repository import Readable
from loom.core.use_case.markers import Input
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute
from loom.testing.http_harness import HttpTestHarness
from loom.testing.in_memory import InMemoryRepository

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class Product(LoomStruct):
    id: int
    name: str


class CreateProductCmd(Command, frozen=True):
    name: str


class GetProductUseCase(UseCase[Product, Product | None]):
    async def execute(self, product_id: int) -> Product | None:
        return await self.main_repo.get_by_id(product_id)


class CreateProductUseCase(UseCase[Product, Product]):
    async def execute(self, cmd: CreateProductCmd = Input()) -> Product:
        return cast(Product, await self.main_repo.create(cmd))


class DeleteProductUseCase(UseCase[Product, bool]):
    async def execute(self, product_id: int) -> bool:
        return await self.main_repo.delete(product_id)


class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    routes = (
        RestRoute(use_case=GetProductUseCase, method="GET", path="/{product_id}"),
        RestRoute(use_case=CreateProductUseCase, method="POST", path="/", status_code=201),
    )


# ---------------------------------------------------------------------------
# Second domain for multi-interface tests
# ---------------------------------------------------------------------------


class Tag(LoomStruct):
    id: int
    label: str


class GetTagUseCase(UseCase[Tag, Tag | None]):
    async def execute(self, tag_id: int) -> Tag | None:
        return await self.main_repo.get_by_id(tag_id)


class TagInterface(RestInterface[Tag]):
    prefix = "/tags"
    routes = (RestRoute(use_case=GetTagUseCase, method="GET", path="/{tag_id}"),)


# ---------------------------------------------------------------------------
# GET — happy path via InMemoryRepository
# ---------------------------------------------------------------------------


class TestGetWithInMemoryRepo:
    def test_returns_seeded_entity(self) -> None:
        repo: InMemoryRepository[Product] = InMemoryRepository(Product)
        repo.seed(Product(id=1, name="Widget"))

        harness = HttpTestHarness()
        harness.inject_repo(Product, repo)
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.get("/products/1")
        assert resp.status_code == 200
        assert resp.json()["name"] == "Widget"
        assert resp.json()["id"] == 1

    def test_returns_null_for_missing_entity(self) -> None:
        repo: InMemoryRepository[Product] = InMemoryRepository(Product)

        harness = HttpTestHarness()
        harness.inject_repo(Product, repo)
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.get("/products/99")
        assert resp.status_code == 200
        assert resp.json() is None

    def test_path_param_forwarded_correctly(self) -> None:
        repo: InMemoryRepository[Product] = InMemoryRepository(Product)
        repo.seed(Product(id=42, name="FortyTwo"))

        harness = HttpTestHarness()
        harness.inject_repo(Product, repo)
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.get("/products/42")
        assert resp.json()["id"] == 42


# ---------------------------------------------------------------------------
# POST — create via InMemoryRepository
# ---------------------------------------------------------------------------


class TestCreateWithInMemoryRepo:
    def test_returns_201_with_created_entity(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.post("/products/", json={"name": "NewProduct"})
        assert resp.status_code == 201
        assert resp.json()["name"] == "NewProduct"

    def test_created_entity_has_auto_assigned_id(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.post("/products/", json={"name": "X"})
        assert isinstance(resp.json()["id"], int)

    def test_sequential_creates_get_different_ids(self) -> None:
        repo: InMemoryRepository[Product] = InMemoryRepository(Product)
        harness = HttpTestHarness()
        harness.inject_repo(Product, repo)
        client = harness.build_app(interfaces=[ProductInterface])

        r1 = client.post("/products/", json={"name": "A"})
        r2 = client.post("/products/", json={"name": "B"})
        assert r1.json()["id"] != r2.json()["id"]


# ---------------------------------------------------------------------------
# Custom fake repo
# ---------------------------------------------------------------------------


class TestWithCustomFakeRepo:
    def test_custom_fake_is_used(self) -> None:
        class FakeProductRepo:
            async def get_by_id(self, obj_id: Any, profile: str = "default") -> Product | None:
                return Product(id=int(obj_id), name="from-fake")

            async def create(self, cmd: Any) -> Product:
                return Product(id=99, name=cmd.name)

            async def delete(self, obj_id: Any) -> bool:
                return True

        harness = HttpTestHarness()
        harness.inject_repo(Product, FakeProductRepo())
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.get("/products/5")
        assert resp.json()["name"] == "from-fake"


# ---------------------------------------------------------------------------
# Error injection
# ---------------------------------------------------------------------------


class TestForceError:
    def test_not_found_maps_to_404(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "get_by_id", NotFound("Product", id=1))
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.get("/products/1")
        assert resp.status_code == 404

    def test_conflict_maps_to_409(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "create", Conflict("duplicate product"))
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.post("/products/", json={"name": "X"})
        assert resp.status_code == 409

    def test_unaffected_method_still_works(self) -> None:
        repo: InMemoryRepository[Product] = InMemoryRepository(Product)
        repo.seed(Product(id=1, name="Widget"))

        harness = HttpTestHarness()
        harness.inject_repo(Product, repo)
        harness.force_error(Product, "create", Conflict("dup"))
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.get("/products/1")
        assert resp.status_code == 200


class TestSimulateSystemError:
    def test_system_error_returns_500(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.simulate_system_error(Product, "get_by_id")
        client = harness.build_app(interfaces=[ProductInterface])

        resp = client.get("/products/1")
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# Multiple interfaces
# ---------------------------------------------------------------------------


class TestMultipleInterfaces:
    def test_both_interfaces_work(self) -> None:
        product_repo: InMemoryRepository[Product] = InMemoryRepository(Product)
        product_repo.seed(Product(id=1, name="Widget"))

        tag_repo: InMemoryRepository[Tag] = InMemoryRepository(Tag)
        tag_repo.seed(Tag(id=10, label="python"))

        harness = HttpTestHarness()
        harness.inject_repo(Product, product_repo)
        harness.inject_repo(Tag, tag_repo)
        client = harness.build_app(interfaces=[ProductInterface, TagInterface])

        assert client.get("/products/1").json()["name"] == "Widget"
        assert client.get("/tags/10").json()["label"] == "python"

    def test_models_are_isolated(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.inject_repo(Tag, InMemoryRepository(Tag))
        client = harness.build_app(interfaces=[ProductInterface, TagInterface])

        assert client.get("/products/99").json() is None
        assert client.get("/tags/99").json() is None


# ---------------------------------------------------------------------------
# FastAPI kwargs forwarded
# ---------------------------------------------------------------------------


class TestFastAPIKwargs:
    def test_title_forwarded(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        client = harness.build_app(
            interfaces=[ProductInterface],
            title="My Test API",
        )
        schema = client.get("/openapi.json").json()
        assert schema["info"]["title"] == "My Test API"


# ---------------------------------------------------------------------------
# config_interfaces and disable_routes forwarding (H7)
# ---------------------------------------------------------------------------


class ConfigStyleTagInterface(RestInterface[Tag]):
    """Stands in for an interface built by ``build_interfaces_from_config``.

    The harness treats a config-declared interface no differently from a
    Python one at this point in the pipeline — both are plain
    ``RestInterface`` subclasses — so this fixture exercises the same code
    path without needing a real YAML file.
    """

    prefix = "/tags"
    routes = (RestRoute(use_case=GetTagUseCase, method="GET", path="/{tag_id}"),)


class TestConfigInterfacesForwarding:
    def test_a_config_declared_interface_is_reachable(self) -> None:
        repo: InMemoryRepository[Tag] = InMemoryRepository(Tag)
        repo.seed(Tag(id=1, label="python"))

        harness = HttpTestHarness()
        harness.inject_repo(Tag, repo)
        client = harness.build_app(interfaces=[], config_interfaces=[ConfigStyleTagInterface])

        resp = client.get("/tags/1")
        assert resp.status_code == 200
        assert resp.json()["label"] == "python"

    def test_python_and_config_interfaces_coexist(self) -> None:
        product_repo: InMemoryRepository[Product] = InMemoryRepository(Product)
        product_repo.seed(Product(id=1, name="Widget"))
        tag_repo: InMemoryRepository[Tag] = InMemoryRepository(Tag)
        tag_repo.seed(Tag(id=1, label="python"))

        harness = HttpTestHarness()
        harness.inject_repo(Product, product_repo)
        harness.inject_repo(Tag, tag_repo)
        client = harness.build_app(
            interfaces=[ProductInterface], config_interfaces=[ConfigStyleTagInterface]
        )

        assert client.get("/products/1").json()["name"] == "Widget"
        assert client.get("/tags/1").json()["label"] == "python"


class TestDisableRoutesForwarding:
    def test_a_disabled_route_answers_404(self) -> None:
        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        client = harness.build_app(
            interfaces=[ProductInterface],
            disable_routes=[("GET", "/products/{product_id}")],
        )

        assert client.get("/products/1").status_code == 404
        assert client.post("/products/", json={"name": "still-here"}).status_code == 201


# ---------------------------------------------------------------------------
# Capability keys — a fake is bound as a store binds its repository
# ---------------------------------------------------------------------------


class AcmeItem(LoomStruct):
    id: int
    name: str


class AcmeItemLookup(Readable[AcmeItem], Protocol):
    async def get_by_name(self, name: str) -> AcmeItem | None: ...


# Declared as a store's repository is, over a subscripted base: capability
# detection reads the class's own ``__orig_bases__``.
class AcmeItemFake(InMemoryRepository[AcmeItem], AcmeItemLookup):
    def __init__(self) -> None:
        super().__init__(AcmeItem)

    async def get_by(self, field: str, value: Any, profile: str = "default") -> AcmeItem | None:
        return None

    async def get_by_name(self, name: str) -> AcmeItem | None:
        return None


class GetAcmeItemUseCase(UseCase[AcmeItem, AcmeItem | None]):
    async def execute(self, item_id: int) -> AcmeItem | None:
        return await self.main_repo.get_by_id(item_id)


class AcmeItemInterface(RestInterface[AcmeItem]):
    prefix = "/items"
    routes = (RestRoute(use_case=GetAcmeItemUseCase, method="GET", path="/{item_id}"),)


class TestCapabilityKeys:
    def test_the_fake_is_bound_under_its_standard_and_custom_capabilities(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        composed: list[Application] = []
        compose = Project.compose

        def captured(self: Project, adapter: Adapter[object], **kwargs: Any) -> Application:
            composed.append(compose(self, adapter, **kwargs))
            return composed[-1]

        monkeypatch.setattr(Project, "compose", captured)
        fake = AcmeItemFake()
        harness = HttpTestHarness()
        harness.inject_repo(AcmeItem, fake)
        harness.build_app(interfaces=[AcmeItemInterface])

        container = composed[0].kernel.container
        assert container.resolve(Readable[AcmeItem]) is fake
        assert container.resolve(AcmeItemLookup) is fake
