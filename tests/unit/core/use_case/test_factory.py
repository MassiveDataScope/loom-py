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
from loom.core.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.use_case import UseCase
from tests.unit.core.doubles import FakeOrderRepo, IOrderRepo

# ---------------------------------------------------------------------------
# Dummy domain objects
# ---------------------------------------------------------------------------


class IEmailService:
    pass


class FakeEmailService(IEmailService):
    pass


class AcmeProduct(LoomStruct):
    id: int
    name: str


class TaskView(LoomStruct):
    task_id: str
    state: str


class TaskViewRepo(Protocol):
    async def get_by_id(self, obj_id: Any, profile: str = "default") -> TaskView | None: ...


class FakeProductRepo:
    async def get_by_id(self, obj_id: Any, profile: str = "default") -> AcmeProduct | None:
        _ = profile
        return await asyncio.sleep(0, result=AcmeProduct(id=int(obj_id), name="p"))

    async def list_paginated(self, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.sleep(0, result=None)

    async def create(self, data: LoomStruct) -> AcmeProduct:
        return await asyncio.sleep(0, result=AcmeProduct(id=1, name="p"))

    async def update(self, obj_id: Any, data: LoomStruct) -> AcmeProduct | None:
        return await asyncio.sleep(0, result=AcmeProduct(id=int(obj_id), name="p"))

    async def delete(self, obj_id: Any) -> bool:
        return await asyncio.sleep(0, result=True)


class FakeTaskViewRepo:
    async def get_by_id(self, obj_id: Any, profile: str = "default") -> TaskView | None:
        _ = profile
        return await asyncio.sleep(0, result=TaskView(task_id=str(obj_id), state="done"))


# ---------------------------------------------------------------------------
# UseCase fixtures
# ---------------------------------------------------------------------------


class AcmeNoDepsUseCase(UseCase[Any, str]):
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


class MainRepoUseCase(UseCase[AcmeProduct, str]):
    def __init__(self, main_repo: RepoFor[AcmeProduct]) -> None:
        super().__init__(main_repo)

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class MixedRepoUseCase(UseCase[AcmeProduct, str]):
    def __init__(self, main_repo: RepoFor[AcmeProduct], email: IEmailService) -> None:
        super().__init__(main_repo)
        self._email = email

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class AutoMainRepoUseCase(UseCase[AcmeProduct, AcmeProduct]):
    async def execute(self, **kwargs: Any) -> AcmeProduct:
        return AcmeProduct(id=1, name="p")


class AutoMainRepoExplicitModelUseCase(UseCase[AcmeProduct, bool]):
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
    c = LoomContainer(classify_key=classify_key, implicit=main_repository)
    for iface, impl in pairs:
        c.register(iface, _make_provider(impl), scope=Scope.REQUEST)
    return c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestUseCaseFactoryBuild:
    def test_build_no_deps(self) -> None:
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        factory = UseCaseFactory(container)
        uc = factory.build(AcmeNoDepsUseCase)
        assert isinstance(uc, AcmeNoDepsUseCase)

    def test_build_single_dep(self) -> None:
        repo = FakeOrderRepo()
        container = _container_with((IOrderRepo, repo))
        factory = UseCaseFactory(container)

        uc = factory.build(SingleDepUseCase)
        assert isinstance(uc, SingleDepUseCase)
        assert uc._repo is repo

    def test_build_multi_deps(self) -> None:
        repo = FakeOrderRepo()
        email = FakeEmailService()
        container = _container_with((IOrderRepo, repo), (IEmailService, email))
        factory = UseCaseFactory(container)

        uc = factory.build(MultiDepUseCase)
        assert uc._repo is repo
        assert uc._email is email

    def test_build_creates_new_instance_each_call(self) -> None:
        container = _container_with((IOrderRepo, FakeOrderRepo()))
        factory = UseCaseFactory(container)

        a = factory.build(SingleDepUseCase)
        b = factory.build(SingleDepUseCase)
        assert a is not b

    def test_build_missing_dep_raises(self) -> None:
        container = LoomContainer(
            classify_key=classify_key, implicit=main_repository
        )  # no bindings
        factory = UseCaseFactory(container)

        with pytest.raises(ResolutionError, match="IOrderRepo"):
            factory.build(SingleDepUseCase)

    def test_verify_compiles_the_plan_every_build_runs(self) -> None:
        container = _container_with((IOrderRepo, FakeOrderRepo()))
        factory = UseCaseFactory(container)
        factory.register(SingleDepUseCase)

        factory.verify()
        plan = container.compiled(SingleDepUseCase)
        factory.build(SingleDepUseCase)

        assert container.compiled(SingleDepUseCase) is plan

    def test_register_accepts_a_missing_dependency_that_build_then_refuses(self) -> None:
        """register() records the class; only build() resolves its dependencies."""
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        factory = UseCaseFactory(container)
        # register() only caches dep list, doesn't resolve
        factory.register(SingleDepUseCase)
        # Now build should raise
        with pytest.raises(ResolutionError):
            factory.build(SingleDepUseCase)

    def test_build_resolves_repo_for_model(self) -> None:
        repo = FakeProductRepo()
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(AcmeProduct, FakeProductRepo)

        factory = UseCaseFactory(container)
        uc = factory.build(MainRepoUseCase)
        assert cast(object, uc.main_repo) is repo

    def test_build_resolves_repo_for_model_and_other_deps(self) -> None:
        repo = FakeProductRepo()
        email = FakeEmailService()
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
        container.register(IEmailService, lambda: email, scope=Scope.REQUEST)
        container.register_repo(AcmeProduct, FakeProductRepo)

        factory = UseCaseFactory(container)
        uc = factory.build(MixedRepoUseCase)
        assert cast(object, uc.main_repo) is repo
        assert uc._email is email

    def test_build_auto_infers_main_repo_from_use_case_generic(self) -> None:
        repo = FakeProductRepo()
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(AcmeProduct, FakeProductRepo)

        factory = UseCaseFactory(container)
        uc = factory.build(AutoMainRepoUseCase)
        assert cast(object, uc.main_repo) is repo

    def test_build_auto_uses_first_generic_as_main_model_even_when_result_is_not_model(
        self,
    ) -> None:
        repo = FakeProductRepo()
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(AcmeProduct, FakeProductRepo)

        factory = UseCaseFactory(container)
        uc = factory.build(AutoMainRepoExplicitModelUseCase)
        assert cast(object, uc.main_repo) is repo

    def test_build_auto_infers_main_repo_with_explicit_contract(self) -> None:
        repo = FakeTaskViewRepo()
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
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


class TestUseCaseFactoryInspectFallback:
    def test_annotation_free_init_takes_the_inspect_fallback_and_yields_no_deps(self) -> None:
        class UntypedUseCase(UseCase):
            def __init__(self, repo):
                self._repo = repo

        factory = UseCaseFactory(LoomContainer(classify_key=classify_key, implicit=main_repository))

        assert _dependencies(factory, UntypedUseCase) == []

    def test_unresolvable_annotations_fall_back_to_the_inspect_signature(self) -> None:
        use_case_type = _use_case_with_unresolvable_annotations()
        repo = FakeOrderRepo()
        factory = UseCaseFactory(_container_with((IOrderRepo, repo)))

        assert _dependencies(factory, use_case_type) == [("repo", IOrderRepo)]
        assert cast(Any, factory.build(use_case_type))._repo is repo


class ListingUseCase(UseCase[Any, str]):
    def __init__(self, products: Listable[AcmeProduct]) -> None:
        self._products = products

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class FakeProductListing:
    pass


class TestUseCaseFactoryCapabilities:
    def test_parametrised_capability_key_is_a_dependency(self) -> None:
        listing = FakeProductListing()
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        container.register(Listable[AcmeProduct], lambda: listing, scope=Scope.APPLICATION)
        factory = UseCaseFactory(container)

        uc = factory.build(ListingUseCase)

        assert cast(object, uc._products) is listing

    def test_verify_names_use_case_parameter_and_key(self) -> None:
        factory = UseCaseFactory(LoomContainer(classify_key=classify_key, implicit=main_repository))
        factory.register(SingleDepUseCase)

        with pytest.raises(ResolutionError, match="SingleDepUseCase") as exc_info:
            factory.verify()

        message = str(exc_info.value)
        assert "repo" in message
        assert "IOrderRepo" in message

    def test_verify_names_a_missing_capability_key(self) -> None:
        factory = UseCaseFactory(LoomContainer(classify_key=classify_key, implicit=main_repository))
        factory.register(ListingUseCase)

        with pytest.raises(ResolutionError, match="ListingUseCase") as exc_info:
            factory.verify()

        assert "Listable" in str(exc_info.value)
        assert "AcmeProduct" in str(exc_info.value)

    def test_verify_checks_the_model_repository_mapping(self) -> None:
        factory = UseCaseFactory(LoomContainer(classify_key=classify_key, implicit=main_repository))
        factory.register(MainRepoUseCase)

        with pytest.raises(ResolutionError, match="repository for AcmeProduct"):
            factory.verify()

    def test_verify_accepts_a_binding_registered_after_the_use_case(self) -> None:
        """Hosts register some services after the use cases; only verify() judges."""
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        factory = UseCaseFactory(container)
        factory.register(SingleDepUseCase)
        container.register(IOrderRepo, FakeOrderRepo, scope=Scope.REQUEST)

        factory.verify()


# ---------------------------------------------------------------------------
# Main repository capability contract (third UseCase generic)
# ---------------------------------------------------------------------------


class ListProductsUseCase(UseCase[AcmeProduct, str, Listable[AcmeProduct]]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


def _container_mapping_product_to(repo_type: type) -> LoomContainer:
    container = LoomContainer(classify_key=classify_key, implicit=main_repository)
    container.register(repo_type, repo_type, scope=Scope.APPLICATION)
    container.register_repo(AcmeProduct, repo_type)
    return container


class TestUseCaseFactoryMainRepository:
    def test_verify_rejects_a_mapped_repository_lacking_the_declared_capability(self) -> None:
        factory = UseCaseFactory(_container_mapping_product_to(FakeProductRepo))
        factory.register(ListProductsUseCase)

        with pytest.raises(ResolutionError, match="ListProductsUseCase") as exc_info:
            factory.verify()

        message = str(exc_info.value)
        assert "main_repo" in message
        assert "Listable[AcmeProduct]" in message

    def test_verify_accepts_a_mapped_repository_registered_under_the_declared_capability(
        self,
    ) -> None:
        container = _container_mapping_product_to(FakeProductRepo)
        container.register(Listable[AcmeProduct], FakeProductRepo, scope=Scope.APPLICATION)
        factory = UseCaseFactory(container)
        factory.register(ListProductsUseCase)

        factory.verify()

    def test_build_with_a_capability_contract_still_resolves_through_the_repo_mapping(self) -> None:
        repo = FakeProductRepo()
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(AcmeProduct, FakeProductRepo)
        container.register(
            Listable[AcmeProduct], lambda: FakeProductRepo(), scope=Scope.APPLICATION
        )

        uc = UseCaseFactory(container).build(ListProductsUseCase)

        assert cast(object, uc.main_repo) is repo


def _task_view_container() -> LoomContainer:
    container = LoomContainer(classify_key=classify_key, implicit=main_repository)
    container.register(FakeTaskViewRepo, FakeTaskViewRepo, scope=Scope.APPLICATION)
    container.register_repo(TaskView, FakeTaskViewRepo)
    return container


class TestUseCaseFactoryMappingSatisfiesContract:
    @pytest.mark.parametrize(
        ("use_case", "container_factory"),
        [
            (AutoMainRepoUseCase, lambda: _container_mapping_product_to(FakeProductRepo)),
            (MainRepoUseCase, lambda: _container_mapping_product_to(FakeProductRepo)),
            (AutoMainRepoCustomContractUseCase, _task_view_container),
        ],
        ids=[
            "default-repo-for-contract",
            "explicit-repo-for-parameter",
            "custom-protocol-contract",
        ],
    )
    def test_verify_treats_a_mapping_as_satisfying_the_main_repo_contract(
        self, use_case: type[UseCase[Any, Any]], container_factory: Callable[[], LoomContainer]
    ) -> None:
        factory = UseCaseFactory(container_factory())
        factory.register(use_case)

        factory.verify()


# ---------------------------------------------------------------------------
# Constructor parameters that are values, not dependencies
# ---------------------------------------------------------------------------


class TaggedUseCase(UseCase[AcmeProduct, str]):
    def __init__(self, main_repo: RepoFor[AcmeProduct], tags: tuple[str, ...] = ()) -> None:
        self._repo = main_repo
        self._tags = tags

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


_NO_EMAIL = FakeEmailService()


class OptionalListingUseCase(UseCase[Any, str]):
    def __init__(
        self,
        products: Listable[AcmeProduct] | None = None,
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


class TestUseCaseFactoryDefaultedParameters:
    def test_defaulted_parameter_is_not_a_dependency_regardless_of_annotation(self) -> None:
        container = _container_mapping_product_to(FakeProductRepo)
        factory = UseCaseFactory(container)
        factory.register(TaggedUseCase)
        factory.register(OptionalListingUseCase)

        factory.verify()

        assert [name for name, _ in _dependencies(factory, TaggedUseCase)] == ["main_repo"]
        assert _dependencies(factory, OptionalListingUseCase) == []
        assert cast(Any, factory.build(TaggedUseCase))._tags == ()

    def test_plain_generic_container_annotation_is_not_a_dependency(self) -> None:
        factory = UseCaseFactory(_container_with((IOrderRepo, FakeOrderRepo())))

        assert _dependencies(factory, ContainerAnnotatedUseCase) == [("repo", IOrderRepo)]
        factory.verify()


class TestDeprecatedBareContainer:
    @pytest.mark.parametrize("use_case", [AutoMainRepoUseCase, MainRepoUseCase])
    def test_a_bare_container_adopts_the_use_case_reading_with_a_warning(
        self, use_case: type[UseCase[AcmeProduct, str]]
    ) -> None:
        repo = FakeProductRepo()
        container = LoomContainer()
        container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(AcmeProduct, FakeProductRepo)

        with pytest.warns(DeprecationWarning, match="UseCaseFactory over a container"):
            factory = UseCaseFactory(container)

        assert cast(object, factory.build(use_case).main_repo) is repo
        assert factory.graph is container.graph

    def test_a_container_reading_another_way_is_refused(self) -> None:
        with pytest.raises(ValueError, match="reads constructors another way"):
            UseCaseFactory(LoomContainer(classify_key=classify_key))
