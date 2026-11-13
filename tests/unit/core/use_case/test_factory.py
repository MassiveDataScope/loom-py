"""Unit tests for UseCaseFactory."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, Protocol, cast

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.graph import ClassNode
from loom.core.di.introspection import KeyDependency, RepoDependency
from loom.core.di.scope import Scope
from loom.core.model import LoomStruct
from loom.core.repository.abc import Listable, RepoFor
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Dummy domain objects
# ---------------------------------------------------------------------------


class IOrderRepo:
    pass


class FakeOrderRepo(IOrderRepo):
    pass


class IEmailService:
    pass


class FakeEmailService(IEmailService):
    pass


class Product(LoomStruct):
    id: int
    name: str


class TaskView(LoomStruct):
    task_id: str
    state: str


class TaskViewRepo(Protocol):
    async def get_by_id(self, obj_id: Any, profile: str = "default") -> TaskView | None: ...


class FakeProductRepo:
    async def get_by_id(self, obj_id: Any, profile: str = "default") -> Product | None:
        _ = profile
        return await asyncio.sleep(0, result=Product(id=int(obj_id), name="p"))

    async def list_paginated(self, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.sleep(0, result=None)

    async def create(self, data: LoomStruct) -> Product:
        return await asyncio.sleep(0, result=Product(id=1, name="p"))

    async def update(self, obj_id: Any, data: LoomStruct) -> Product | None:
        return await asyncio.sleep(0, result=Product(id=int(obj_id), name="p"))

    async def delete(self, obj_id: Any) -> bool:
        return await asyncio.sleep(0, result=True)


class FakeTaskViewRepo:
    async def get_by_id(self, obj_id: Any, profile: str = "default") -> TaskView | None:
        _ = profile
        return await asyncio.sleep(0, result=TaskView(task_id=str(obj_id), state="done"))


# ---------------------------------------------------------------------------
# UseCase fixtures
# ---------------------------------------------------------------------------


class NoDepsUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class SingleDepUseCase(UseCase[Any, str]):
    def __init__(self, repo: IOrderRepo) -> None:
        self._repo = repo

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class MultiDepUseCase(UseCase[Any, str]):
    def __init__(self, repo: IOrderRepo, email: IEmailService) -> None:
        self._repo = repo
        self._email = email

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class MainRepoUseCase(UseCase[Product, str]):
    def __init__(self, main_repo: RepoFor[Product]) -> None:
        super().__init__(main_repo)

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class MixedRepoUseCase(UseCase[Product, str]):
    def __init__(self, main_repo: RepoFor[Product], email: IEmailService) -> None:
        super().__init__(main_repo)
        self._email = email

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class AutoMainRepoUseCase(UseCase[Product, Product]):
    async def execute(self, **kwargs: Any) -> Product:
        return Product(id=1, name="p")


class AutoMainRepoExplicitModelUseCase(UseCase[Product, bool]):
    async def execute(self, **kwargs: Any) -> bool:
        return True


class AutoMainRepoCustomContractUseCase(UseCase[TaskView, TaskView | None, TaskViewRepo]):
    async def execute(self, task_id: str) -> TaskView | None:
        return await self.main_repo.get_by_id(task_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_provider(obj: Any) -> Any:
    def _provider() -> Any:
        return obj

    return _provider


def _dependencies(factory: UseCaseFactory, use_case_type: type) -> list[tuple[str, object]]:
    """The keys and repositories the factory injects into *use_case_type*, read from its graph."""
    return [
        (name, dependency.key if isinstance(dependency, KeyDependency) else dependency)
        for name, dependency in factory.graph.constructor(ClassNode(use_case_type))
        if isinstance(dependency, KeyDependency | RepoDependency)
    ]


def _container_with(*pairs: tuple[type, object]) -> LoomContainer:
    c = LoomContainer()
    for iface, impl in pairs:
        c.register(iface, _make_provider(impl), scope=Scope.REQUEST)
    return c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_no_deps() -> None:
    container = LoomContainer()
    factory = UseCaseFactory(container)
    uc = factory.build(NoDepsUseCase)
    assert isinstance(uc, NoDepsUseCase)


def test_build_single_dep() -> None:
    repo = FakeOrderRepo()
    container = _container_with((IOrderRepo, repo))
    factory = UseCaseFactory(container)

    uc = factory.build(SingleDepUseCase)
    assert isinstance(uc, SingleDepUseCase)
    assert uc._repo is repo


def test_build_multi_deps() -> None:
    repo = FakeOrderRepo()
    email = FakeEmailService()
    container = _container_with((IOrderRepo, repo), (IEmailService, email))
    factory = UseCaseFactory(container)

    uc = factory.build(MultiDepUseCase)
    assert uc._repo is repo
    assert uc._email is email


def test_build_creates_new_instance_each_call() -> None:
    container = _container_with((IOrderRepo, FakeOrderRepo()))
    factory = UseCaseFactory(container)

    a = factory.build(SingleDepUseCase)
    b = factory.build(SingleDepUseCase)
    assert a is not b


def test_build_missing_dep_raises() -> None:
    container = LoomContainer()  # no bindings
    factory = UseCaseFactory(container)

    with pytest.raises(ResolutionError, match="IOrderRepo"):
        factory.build(SingleDepUseCase)


def test_plan_compiled_on_verify_after_register() -> None:
    container = _container_with((IOrderRepo, FakeOrderRepo()))
    factory = UseCaseFactory(container)

    factory.register(SingleDepUseCase)
    factory.verify()
    assert container._plans[SingleDepUseCase] is factory.compiled(SingleDepUseCase)


def test_plan_compiled_on_first_build() -> None:
    container = _container_with((IOrderRepo, FakeOrderRepo()))
    factory = UseCaseFactory(container)

    assert SingleDepUseCase not in container._plans
    factory.build(SingleDepUseCase)
    assert SingleDepUseCase in container._plans
    # Same plan object on second call
    plan_first = container._plans[SingleDepUseCase]
    factory.build(SingleDepUseCase)
    assert container._plans[SingleDepUseCase] is plan_first


def test_register_raises_if_dep_not_in_container() -> None:
    """register() itself doesn't raise — only build() does. Validate separately."""
    container = LoomContainer()
    factory = UseCaseFactory(container)
    # register() only caches dep list, doesn't resolve
    factory.register(SingleDepUseCase)
    # Now build should raise
    with pytest.raises(ResolutionError):
        factory.build(SingleDepUseCase)


def test_build_resolves_repo_for_model() -> None:
    repo = FakeProductRepo()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(MainRepoUseCase)
    assert cast(object, uc.main_repo) is repo


def test_build_resolves_repo_for_model_and_other_deps() -> None:
    repo = FakeProductRepo()
    email = FakeEmailService()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register(IEmailService, lambda: email, scope=Scope.REQUEST)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(MixedRepoUseCase)
    assert cast(object, uc.main_repo) is repo
    assert uc._email is email


def test_build_auto_infers_main_repo_from_use_case_generic() -> None:
    repo = FakeProductRepo()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(AutoMainRepoUseCase)
    assert cast(object, uc.main_repo) is repo


def test_build_auto_uses_first_generic_as_main_model_even_when_result_is_not_model() -> None:
    repo = FakeProductRepo()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(AutoMainRepoExplicitModelUseCase)
    assert cast(object, uc.main_repo) is repo


def test_build_auto_infers_main_repo_with_explicit_contract() -> None:
    repo = FakeTaskViewRepo()
    container = LoomContainer()
    container.register(TaskViewRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(TaskView, TaskViewRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(AutoMainRepoCustomContractUseCase)
    assert uc.main_repo is repo


def _use_case_with_unresolvable_annotations() -> type[UseCase]:
    """Build a UseCase whose ``__init__`` annotations only ``inspect`` can read."""

    def __init__(self: Any, repo: Any) -> None:
        self._repo = repo

    async def execute(self: Any) -> None:
        return None

    __init__.__annotations__ = {"repo": IOrderRepo, "return": "NotImportable"}
    return type(
        "UnresolvableAnnotationsUseCase",
        (UseCase,),
        {"__init__": __init__, "execute": execute},
    )


def test_annotation_free_init_takes_the_inspect_fallback_and_yields_no_deps() -> None:
    class UntypedUseCase(UseCase):
        def __init__(self, repo):
            self._repo = repo

    factory = UseCaseFactory(LoomContainer())

    assert _dependencies(factory, UntypedUseCase) == []


def test_unresolvable_annotations_fall_back_to_the_inspect_signature() -> None:
    use_case_type = _use_case_with_unresolvable_annotations()
    repo = FakeOrderRepo()
    factory = UseCaseFactory(_container_with((IOrderRepo, repo)))

    assert _dependencies(factory, use_case_type) == [("repo", IOrderRepo)]
    assert cast(Any, factory.build(use_case_type))._repo is repo


class ListingUseCase(UseCase[Any, str]):
    def __init__(self, products: Listable[Product]) -> None:
        self._products = products

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class FakeProductListing:
    pass


def test_parametrised_capability_key_is_a_dependency() -> None:
    listing = FakeProductListing()
    container = LoomContainer()
    container.register(Listable[Product], lambda: listing, scope=Scope.APPLICATION)
    factory = UseCaseFactory(container)

    uc = factory.build(ListingUseCase)

    assert cast(object, uc._products) is listing


def test_verify_names_use_case_parameter_and_key() -> None:
    factory = UseCaseFactory(LoomContainer())
    factory.register(SingleDepUseCase)

    with pytest.raises(ResolutionError, match="SingleDepUseCase") as exc_info:
        factory.verify()

    message = str(exc_info.value)
    assert "repo" in message
    assert "IOrderRepo" in message


def test_verify_names_a_missing_capability_key() -> None:
    factory = UseCaseFactory(LoomContainer())
    factory.register(ListingUseCase)

    with pytest.raises(ResolutionError, match="ListingUseCase") as exc_info:
        factory.verify()

    assert "Listable" in str(exc_info.value)
    assert "Product" in str(exc_info.value)


def test_verify_checks_the_model_repository_mapping() -> None:
    factory = UseCaseFactory(LoomContainer())
    factory.register(MainRepoUseCase)

    with pytest.raises(ResolutionError, match="repository for Product"):
        factory.verify()


def test_verify_accepts_a_binding_registered_after_the_use_case() -> None:
    """Hosts register some services after the use cases; only verify() judges."""
    container = LoomContainer()
    factory = UseCaseFactory(container)
    factory.register(SingleDepUseCase)
    container.register(IOrderRepo, FakeOrderRepo, scope=Scope.REQUEST)

    factory.verify()


# ---------------------------------------------------------------------------
# Main repository capability contract (third UseCase generic)
# ---------------------------------------------------------------------------


class ListProductsUseCase(UseCase[Product, str, Listable[Product]]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


def _container_mapping_product_to(repo_type: type) -> LoomContainer:
    container = LoomContainer()
    container.register(repo_type, repo_type, scope=Scope.APPLICATION)
    container.register_repo(Product, repo_type)
    return container


def test_verify_rejects_a_mapped_repository_lacking_the_declared_capability() -> None:
    factory = UseCaseFactory(_container_mapping_product_to(FakeProductRepo))
    factory.register(ListProductsUseCase)

    with pytest.raises(ResolutionError, match="ListProductsUseCase") as exc_info:
        factory.verify()

    message = str(exc_info.value)
    assert "main_repo" in message
    assert "Listable[Product]" in message


def test_verify_accepts_a_mapped_repository_registered_under_the_declared_capability() -> None:
    container = _container_mapping_product_to(FakeProductRepo)
    container.register(Listable[Product], FakeProductRepo, scope=Scope.APPLICATION)
    factory = UseCaseFactory(container)
    factory.register(ListProductsUseCase)

    factory.verify()


def test_build_with_a_capability_contract_still_resolves_through_the_repo_mapping() -> None:
    repo = FakeProductRepo()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, FakeProductRepo)
    container.register(Listable[Product], lambda: FakeProductRepo(), scope=Scope.APPLICATION)

    uc = UseCaseFactory(container).build(ListProductsUseCase)

    assert cast(object, uc.main_repo) is repo


def _task_view_container() -> LoomContainer:
    container = LoomContainer()
    container.register(FakeTaskViewRepo, FakeTaskViewRepo, scope=Scope.APPLICATION)
    container.register_repo(TaskView, FakeTaskViewRepo)
    return container


@pytest.mark.parametrize(
    ("use_case", "container_factory"),
    [
        (AutoMainRepoUseCase, lambda: _container_mapping_product_to(FakeProductRepo)),
        (MainRepoUseCase, lambda: _container_mapping_product_to(FakeProductRepo)),
        (AutoMainRepoCustomContractUseCase, _task_view_container),
    ],
    ids=["default-repo-for-contract", "explicit-repo-for-parameter", "custom-protocol-contract"],
)
def test_verify_treats_a_mapping_as_satisfying_the_main_repo_contract(
    use_case: type[UseCase[Any, Any]], container_factory: Callable[[], LoomContainer]
) -> None:
    factory = UseCaseFactory(container_factory())
    factory.register(use_case)

    factory.verify()


# ---------------------------------------------------------------------------
# Constructor parameters that are values, not dependencies
# ---------------------------------------------------------------------------


class TaggedUseCase(UseCase[Product, str]):
    def __init__(self, main_repo: RepoFor[Product], tags: tuple[str, ...] = ()) -> None:
        self._repo = main_repo
        self._tags = tags

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


_NO_EMAIL = FakeEmailService()


class OptionalListingUseCase(UseCase[Any, str]):
    def __init__(
        self,
        products: Listable[Product] | None = None,
        email: IEmailService = _NO_EMAIL,
    ) -> None:
        self._products = products
        self._email = email

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class ContainerAnnotatedUseCase(UseCase[Any, str]):
    def __init__(self, repo: IOrderRepo, names: list[str]) -> None:
        self._repo = repo
        self._names = names

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


def test_defaulted_parameter_is_not_a_dependency_regardless_of_annotation() -> None:
    container = _container_mapping_product_to(FakeProductRepo)
    factory = UseCaseFactory(container)
    factory.register(TaggedUseCase)
    factory.register(OptionalListingUseCase)

    factory.verify()

    assert [name for name, _ in _dependencies(factory, TaggedUseCase)] == ["main_repo"]
    assert _dependencies(factory, OptionalListingUseCase) == []
    assert cast(Any, factory.build(TaggedUseCase))._tags == ()


def test_plain_generic_container_annotation_is_not_a_dependency() -> None:
    factory = UseCaseFactory(_container_with((IOrderRepo, FakeOrderRepo())))

    assert _dependencies(factory, ContainerAnnotatedUseCase) == [("repo", IOrderRepo)]
    factory.verify()
