"""Autowiring: a class binding's required parameters come from the container, as compiled once."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.graph import DependencyGraphError
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope, Kind
from loom.core.identity import ANONYMOUS, Identity
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.repository import RepoFor
from loom.core.stores.repository.registration import classify_key
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.acme import AcmeCaller, AcmeInput


class AcmeCompilerOrder(BaseModel):
    __tablename__ = "di_compiler_acme_orders"

    id: int = ColumnField(primary_key=True)


class AcmeSettings:
    region = "eu"


class AcmeClient:
    def __init__(self, settings: AcmeSettings) -> None:
        self.settings = settings


class AcmeCompilerOrders:
    """The repository of :class:`AcmeCompilerOrder`."""


class AcmeGateway:
    def __init__(
        self, client: AcmeClient, orders: RepoFor[AcmeCompilerOrder], retries: int = 3
    ) -> None:
        self.client, self.orders, self.retries = client, orders, retries


class AcmeCallerAware:
    def __init__(self, caller: object = AcmeCaller()) -> None:
        self.caller = caller


class AcmeUnannotatedClient:
    def __init__(self, client) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
        self.client = client


class AcmeRefused:
    def __init__(self, payload: object = AcmeInput()) -> None:
        self.payload = payload


class AcmeCallerResolver:
    def resolve(self, declaration: object, annotation: object) -> Identity:
        return ANONYMOUS


class TestClosureAutowiring:
    def test_required_parameters_are_autowired_transitively(self) -> None:
        container = LoomContainer(classify_key=classify_key)
        container.register(AcmeSettings, AcmeSettings, scope=Scope.APPLICATION)
        container.register(AcmeClient, AcmeClient)
        container.register(AcmeCompilerOrders, AcmeCompilerOrders, scope=Scope.APPLICATION)
        container.register_repo(AcmeCompilerOrder, AcmeCompilerOrders)
        container.register(AcmeGateway, AcmeGateway)

        gateway = container.resolve(AcmeGateway)

        assert gateway.client.settings is container.resolve(AcmeSettings)
        assert gateway.orders is container.resolve(AcmeCompilerOrders)
        assert gateway.retries == 3

    def test_an_execution_kind_is_resolved_through_its_resolver_per_build(self) -> None:
        container = acme.container()
        container.kinds.bind(acme.CALLER, AcmeCallerResolver())
        container.register(AcmeCallerAware, AcmeCallerAware)

        with ExecutionScope(ANONYMOUS):
            built = container.resolve(AcmeCallerAware)

        assert built.caller is ANONYMOUS

    def test_an_unbound_kind_fails_at_build_naming_the_kind_never_passing_the_sentinel(
        self,
    ) -> None:
        container = acme.container()
        container.register(AcmeCallerAware, AcmeCallerAware)

        with pytest.raises(ResolutionError, match="caller: no resolver is bound for 'acme-caller'"):
            container.resolve(AcmeCallerAware)

    def test_a_resolver_bound_after_the_compile_is_used(self) -> None:
        container = acme.container()
        container.register(AcmeCallerAware, AcmeCallerAware)
        with pytest.raises(ResolutionError):
            container.resolve(AcmeCallerAware)

        container.kinds.bind(acme.CALLER, AcmeCallerResolver())

        assert container.resolve(AcmeCallerAware).caller is ANONYMOUS

    def test_an_unbound_dependency_fails_with_its_path(self) -> None:
        container = LoomContainer()
        container.register(AcmeClient, AcmeClient)

        with pytest.raises(
            DependencyGraphError, match="AcmeClient -> settings: AcmeSettings: no binding"
        ):
            container.resolve(AcmeClient)

    def test_an_unmapped_repository_fails_with_its_path(self) -> None:
        container = LoomContainer(classify_key=classify_key)
        container.register(AcmeSettings, AcmeSettings)
        container.register(AcmeClient, AcmeClient)
        container.register(AcmeGateway, AcmeGateway)

        with pytest.raises(
            DependencyGraphError, match="AcmeGateway: orders: no repository mapping"
        ):
            container.resolve(AcmeGateway)

    def test_a_refused_declaration_fails_with_the_refusal(self) -> None:
        container = acme.container()
        container.register(AcmeRefused, AcmeRefused)

        with pytest.raises(DependencyGraphError, match="AcmeRefused: payload: AcmeInput"):
            container.resolve(AcmeRefused)

    def test_an_unresolvable_parameter_is_left_to_python_as_today(self) -> None:
        container = LoomContainer()
        container.register(AcmeUnannotatedClient, AcmeUnannotatedClient)

        with pytest.raises(TypeError, match="client"):
            container.resolve(AcmeUnannotatedClient)

    def test_without_declared_kinds_a_marker_default_is_kept_as_today(self) -> None:
        container = LoomContainer()
        container.register(AcmeCallerAware, AcmeCallerAware)

        assert isinstance(container.resolve(AcmeCallerAware).caller, AcmeCaller)

    def test_a_kind_declared_on_another_registry_is_not_resolved(self) -> None:
        container = LoomContainer()
        container.kinds.declare_type(AcmeCaller, Kind("acme-other"))
        container.register(AcmeCallerAware, AcmeCallerAware)

        with pytest.raises(ResolutionError, match="'acme-other'"):
            container.resolve(AcmeCallerAware)


class AcmeNamed:
    def __init__(self, name: str) -> None:
        self.name = name


class AcmeLoose:
    def __init__(self, anything: Any) -> None:  # noqa: ANN401 - the annotation under test
        self.anything = anything


class AcmeRawParameter:
    def __init__(self, raw) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
        self.raw = raw


class TestClosureUnresolvableParameters:
    @pytest.mark.parametrize(
        ("cls", "where"), [(AcmeNamed, "name: str"), (AcmeLoose, "anything: Any")]
    )
    def test_a_primitive_or_any_annotation_is_a_key_with_no_binding(
        self, cls: type, where: str
    ) -> None:
        container = LoomContainer(classify_key=classify_key)
        container.register(cls, cls, scope=Scope.APPLICATION)

        with pytest.raises(DependencyGraphError, match=f"{where}: no binding is registered for it"):
            container.validate()

    def test_an_unannotated_parameter_is_left_to_python(self) -> None:
        container = LoomContainer(classify_key=classify_key)
        container.register(AcmeRawParameter, AcmeRawParameter)

        with pytest.raises(TypeError, match="raw"):
            container.resolve(AcmeRawParameter)
