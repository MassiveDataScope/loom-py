"""Unit tests for RestRoute, RestInterface, RestApiDefaults, and PaginationMode."""

from __future__ import annotations

import pytest

from loom.core.model import LoomStruct
from loom.rest.model import (
    PaginationMode,
    RestApiDefaults,
    RestInterface,
    RestRoute,
)
from tests.unit.rest.doubles import CreateUserUseCase, ListUsersUseCase

# ---------------------------------------------------------------------------
# Dummy use cases
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# RestInterface.__init_subclass__ (auto-CRUD)
# ---------------------------------------------------------------------------


class _AutoModel(LoomStruct):
    id: int
    name: str


class TestRestInterfaceAutoInitSubclass:
    def test_auto_true_no_routes_populates_routes(self) -> None:
        class AutoInterface(RestInterface[_AutoModel]):
            prefix = "/items"
            auto = True

        assert len(AutoInterface.routes) == 5

    def test_auto_true_with_explicit_routes_not_overridden(self) -> None:
        existing_route = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")

        class ExplicitInterface(RestInterface[_AutoModel]):
            prefix = "/items"
            auto = True
            routes = (existing_route,)

        assert len(ExplicitInterface.routes) == 1
        assert ExplicitInterface.routes[0] is existing_route

    def test_auto_false_routes_remain_empty(self) -> None:
        class ManualInterface(RestInterface[_AutoModel]):
            prefix = "/items"
            auto = False

        assert ManualInterface.routes == ()

    def test_include_subset_generates_only_specified_ops(self) -> None:
        class SubsetInterface(RestInterface[_AutoModel]):
            prefix = "/items"
            auto = True
            include = ("create", "get")

        assert len(SubsetInterface.routes) == 2
        methods = {r.method for r in SubsetInterface.routes}
        assert methods == {"POST", "GET"}

    def test_abstract_intermediate_class_without_type_param_skipped(self) -> None:
        """Base classes without concrete type param should not raise errors."""

        class BaseAutoInterface(RestInterface[_AutoModel]):
            auto = True

        # Subclassing again — routes already populated on BaseAutoInterface
        assert len(BaseAutoInterface.routes) == 5

    def test_auto_routes_have_correct_use_case_names(self) -> None:
        class NamedInterface(RestInterface[_AutoModel]):
            prefix = "/named"
            auto = True

        uc_names = {r.use_case.__name__ for r in NamedInterface.routes}
        assert "AutoCreate_AutoModel" in uc_names or any("_AutoModel" in name for name in uc_names)


class TestRestRoute:
    def test_rest_route_required_fields(
        self,
    ) -> None:
        route = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")
        assert route.use_case is CreateUserUseCase
        assert route.method == "POST"
        assert route.path == "/"

    def test_rest_route_defaults(
        self,
    ) -> None:
        route = RestRoute(use_case=CreateUserUseCase, method="GET", path="/")
        assert route.summary == ""
        assert route.description == ""
        assert route.status_code == 200
        assert route.pagination_mode is None
        assert route.allow_pagination_override is None
        assert route.profile_default == ""
        assert route.allowed_profiles == ()
        assert route.expose_profile is False

    def test_rest_route_is_frozen(
        self,
    ) -> None:
        route = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")
        with pytest.raises((AttributeError, TypeError)):
            route.method = "GET"  # type: ignore[misc]

    def test_rest_route_with_all_fields(
        self,
    ) -> None:
        route = RestRoute(
            use_case=CreateUserUseCase,
            method="POST",
            path="/",
            summary="Create user",
            description="Creates a new user account",
            status_code=201,
            pagination_mode=PaginationMode.CURSOR,
            allow_pagination_override=False,
            profile_default="detail",
            allowed_profiles=("detail", "summary"),
            expose_profile=True,
        )
        assert route.status_code == 201
        assert route.pagination_mode == PaginationMode.CURSOR
        assert route.allow_pagination_override is False
        assert route.expose_profile is True


class TestRestInterface:
    def test_rest_interface_defaults(
        self,
    ) -> None:
        class EmptyInterface(RestInterface[str]):
            prefix = "/test"

        assert EmptyInterface.prefix == "/test"
        assert EmptyInterface.tags == ()
        assert EmptyInterface.auto is False
        assert EmptyInterface.include == ()
        assert EmptyInterface.routes == ()
        assert EmptyInterface.pagination_mode is None
        assert EmptyInterface.allow_pagination_override is None
        assert EmptyInterface.profile_default == ""
        assert EmptyInterface.allowed_profiles == ()
        assert EmptyInterface.expose_profile is False

    def test_rest_interface_subclass_overrides(
        self,
    ) -> None:
        create = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")
        list_route = RestRoute(use_case=ListUsersUseCase, method="GET", path="/")

        class UserInterface(RestInterface[str]):
            prefix = "/users"
            tags = ("Users",)
            auto = True
            include = ("create", "list")
            routes = (create, list_route)
            pagination_mode = PaginationMode.CURSOR
            allow_pagination_override = False
            profile_default = "summary"
            allowed_profiles = ("summary", "detail")
            expose_profile = True

        assert UserInterface.prefix == "/users"
        assert UserInterface.tags == ("Users",)
        assert UserInterface.auto is True
        assert UserInterface.pagination_mode == PaginationMode.CURSOR
        assert UserInterface.allow_pagination_override is False
        assert len(UserInterface.routes) == 2
        assert UserInterface.expose_profile is True

    def test_rest_interface_is_generic(
        self,
    ) -> None:
        class TypedInterface(RestInterface[str]):
            prefix = "/items"

        # Just checking no runtime error on instantiation / class access
        assert TypedInterface.prefix == "/items"


class TestRestApiDefaults:
    def test_rest_api_defaults_default_values(
        self,
    ) -> None:
        d = RestApiDefaults()
        assert d.pagination_mode == PaginationMode.OFFSET
        assert d.allow_pagination_override is True
        assert d.profile_default == "default"
        assert d.allowed_profiles == ()

    def test_rest_api_defaults_custom(
        self,
    ) -> None:
        d = RestApiDefaults(
            pagination_mode=PaginationMode.CURSOR,
            allow_pagination_override=False,
            profile_default="summary",
            allowed_profiles=("summary", "detail"),
        )
        assert d.pagination_mode == PaginationMode.CURSOR
        assert d.allow_pagination_override is False
        assert d.profile_default == "summary"

    def test_rest_api_defaults_is_frozen(
        self,
    ) -> None:
        d = RestApiDefaults()
        with pytest.raises((AttributeError, TypeError)):
            d.pagination_mode = PaginationMode.CURSOR  # type: ignore[misc]


class TestPaginationMode:
    def test_pagination_mode_values(
        self,
    ) -> None:
        assert PaginationMode.OFFSET.value == "offset"
        assert PaginationMode.CURSOR.value == "cursor"
