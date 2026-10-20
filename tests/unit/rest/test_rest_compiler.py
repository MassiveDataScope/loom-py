"""Unit tests for RestInterfaceCompiler."""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

import msgspec
import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.model import JSON, BaseModel, Cardinality, ColumnField, RelationField, String
from loom.core.repository.abc.query import QuerySpec
from loom.core.use_case import Created, Query
from loom.core.use_case.field_ref import F
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import InterfaceCompilationError, RestInterfaceCompiler, RouteSources
from loom.rest.model import ALL, PaginationMode, RestApiDefaults, RestInterface, RestRoute


class CreateUserUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class GetUserUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class ListUsersUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class UncompiledUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


@pytest.fixture
def use_case_compiler() -> UseCaseCompiler:
    compiler = UseCaseCompiler()
    compiler.compile(CreateUserUseCase)
    compiler.compile(GetUserUseCase)
    compiler.compile(ListUsersUseCase)
    return compiler


@pytest.fixture
def rest_compiler(use_case_compiler: UseCaseCompiler) -> RestInterfaceCompiler:
    return RestInterfaceCompiler(use_case_compiler)


class TestRouteSources:
    """A frozen ``RouteSources`` must not alias the caller's own mutable list."""

    def test_a_python_list_passed_in_is_copied_not_aliased(self) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        interfaces = [IFace]
        sources = RouteSources(python=interfaces)
        interfaces.append(IFace)

        assert len(sources.python) == 1

    def test_fields_are_stored_as_tuples(self) -> None:
        sources = RouteSources(disabled=[("GET", "/a")])
        assert isinstance(sources.disabled, tuple)


class TestCompile:
    def test_compile_single_route(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        result = rest_compiler.compile(IFace)
        assert len(result) == 1
        assert result[0].route.use_case is CreateUserUseCase

    def test_compile_multiple_routes(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),
                RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),
            )

        result = rest_compiler.compile(IFace)
        assert len(result) == 3

    def test_compile_full_path_assembled(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),)

        result = rest_compiler.compile(IFace)
        assert result[0].full_path == "/users/{user_id}"

    def test_compile_prefix_trailing_slash_stripped(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users/"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        result = rest_compiler.compile(IFace)
        assert result[0].full_path == "/users/"

    def test_compile_is_idempotent(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        first = rest_compiler.compile(IFace)
        second = rest_compiler.compile(IFace)
        assert first is second


class TestPaginationPolicy:
    def test_route_overrides_interface_and_global(
        self,
        use_case_compiler: UseCaseCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            pagination_mode = PaginationMode.OFFSET
            routes = (
                RestRoute(
                    use_case=ListUsersUseCase,
                    method="GET",
                    path="/",
                    pagination_mode=PaginationMode.CURSOR,
                ),
            )

        defaults = RestApiDefaults(pagination_mode=PaginationMode.OFFSET)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_pagination_mode == PaginationMode.CURSOR

    def test_interface_overrides_global(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            pagination_mode = PaginationMode.CURSOR
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(pagination_mode=PaginationMode.OFFSET)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_pagination_mode == PaginationMode.CURSOR

    def test_global_applied_when_unset(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(pagination_mode=PaginationMode.CURSOR)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_pagination_mode == PaginationMode.CURSOR

    def test_pagination_override_route_level(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            allow_pagination_override = True
            routes = (
                RestRoute(
                    use_case=ListUsersUseCase,
                    method="GET",
                    path="/",
                    allow_pagination_override=False,
                ),
            )

        defaults = RestApiDefaults(allow_pagination_override=True)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_allow_pagination_override is False

    def test_pagination_override_interface_level(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            allow_pagination_override = False
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(allow_pagination_override=True)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_allow_pagination_override is False

    def test_pagination_override_global_default(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(allow_pagination_override=False)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_allow_pagination_override is False


class TestProfilePolicy:
    def test_profile_default_route_level(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            profile_default = "summary"
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                    profile_default="detail",
                ),
            )

        result = rest_compiler.compile(IFace)
        assert result[0].effective_profile_default == "detail"

    def test_profile_default_interface_level(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            profile_default = "summary"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

        result = rest_compiler.compile(IFace)
        assert result[0].effective_profile_default == "summary"

    def test_profile_default_global(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

        defaults = RestApiDefaults(profile_default="compact")
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_profile_default == "compact"

    def test_allowed_profiles_route_level(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            allowed_profiles = ("summary",)
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                    allowed_profiles=("detail", "full"),
                ),
            )

        result = rest_compiler.compile(IFace)
        assert result[0].effective_allowed_profiles == ("detail", "full")


class TestErrors:
    def test_compile_missing_prefix_raises(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = ""
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        with pytest.raises(InterfaceCompilationError, match="prefix"):
            rest_compiler.compile(IFace)

    def test_compile_prefix_placeholder_raises(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/customers/{customer_id}/tickets"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),)

        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile(IFace)
        message = str(excinfo.value)
        assert "IFace" in message
        assert "'/customers/{customer_id}/tickets'" in message
        assert "path" in message

    def test_compile_empty_routes_raises(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = ()

        with pytest.raises(InterfaceCompilationError, match="routes"):
            rest_compiler.compile(IFace)

    def test_compile_uncompiled_use_case_raises(
        self,
        use_case_compiler: UseCaseCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=UncompiledUseCase, method="POST", path="/"),)

        compiler = RestInterfaceCompiler(use_case_compiler)
        with pytest.raises(InterfaceCompilationError, match="not been compiled"):
            compiler.compile(IFace)

    def test_compile_duplicate_method_path_raises(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
            )

        with pytest.raises(InterfaceCompilationError, match="duplicate"):
            rest_compiler.compile(IFace)

    def test_compile_expose_profile_without_allowed_profiles_raises(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                    expose_profile=True,
                    allowed_profiles=(),
                ),
            )

        with pytest.raises(InterfaceCompilationError, match="expose_profile"):
            rest_compiler.compile(IFace)

    def test_compile_expose_profile_with_allowed_profiles_ok(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                    expose_profile=True,
                    allowed_profiles=("detail",),
                ),
            )

        result = rest_compiler.compile(IFace)
        assert result[0].effective_expose_profile is True

    def test_compile_expose_profile_from_interface_with_allowed_profiles_ok(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            expose_profile = True
            allowed_profiles = ("detail",)
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                ),
            )

        result = rest_compiler.compile(IFace)
        assert result[0].effective_expose_profile is True

    def test_compile_expose_profile_from_interface_without_allowed_profiles_raises(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            expose_profile = True
            allowed_profiles = ()
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                ),
            )

        with pytest.raises(InterfaceCompilationError, match="profile exposure"):
            rest_compiler.compile(IFace)


class TestReadOnly:
    def test_get_route_is_read_only(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

        result = rest_compiler.compile(IFace)
        assert result[0].read_only is True

    def test_post_route_is_not_read_only(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        result = rest_compiler.compile(IFace)
        assert result[0].read_only is False

    def test_mixed_routes_read_only_per_method(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),
            )

        result = rest_compiler.compile(IFace)
        by_method = {cr.route.method: cr.read_only for cr in result}
        assert by_method["GET"] is True
        assert by_method["POST"] is False

    def test_execution_plan_read_only_from_class_flag(
        self, use_case_compiler: UseCaseCompiler
    ) -> None:
        class _ReadOnlyUC(UseCase[Any, str]):
            read_only = True

            async def execute(self) -> str:
                return "ok"

        plan = use_case_compiler.compile(_ReadOnlyUC)
        assert plan.read_only is True

    def test_execution_plan_read_only_defaults_false(
        self, use_case_compiler: UseCaseCompiler
    ) -> None:
        plan = use_case_compiler.compile(GetUserUseCase)
        assert plan.read_only is False


class TestCompileSources:
    """``compile_sources`` — Python-then-config merge, collision, disablement."""

    def test_python_interfaces_compile_before_config_ones(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        class ConfigIFace(RestInterface[str]):
            prefix = "/customers"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),)

        result = rest_compiler.compile_sources(RouteSources(python=[PyIFace], config=[ConfigIFace]))
        assert [r.full_path for r in result] == ["/users/", "/customers/{user_id}"]

    def test_collision_between_origins_names_both(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        class ConfigIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="POST", path="/"),)

        sources = RouteSources(python=[PyIFace], config=[ConfigIFace])
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        message = str(excinfo.value)
        assert "Python interface" in message
        assert "app.rest.interfaces entry" in message
        assert "PyIFace" in message
        assert "ConfigIFace" in message

    def test_collision_between_two_python_interfaces_also_aborts(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """Shared collision tracking also catches two same-origin interfaces.

        Not asked for directly, but a byproduct of tracking every route in
        one dict regardless of origin: two Python interfaces mounting the
        same (method, path) used to shadow silently at the FastAPI layer.
        """

        class FirstIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        class SecondIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="POST", path="/"),)

        sources = RouteSources(python=[FirstIFace, SecondIFace])
        with pytest.raises(InterfaceCompilationError, match="declared twice"):
            rest_compiler.compile_sources(sources)

    def test_disable_routes_removes_the_named_route(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),
            )

        result = rest_compiler.compile_sources(
            RouteSources(python=[PyIFace], disabled=[("GET", "/users/{user_id}")])
        )
        assert [r.full_path for r in result] == ["/users/"]

    def test_disable_routes_matching_nothing_raises(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        sources = RouteSources(python=[PyIFace], disabled=[("GET", "/users/missing")])
        with pytest.raises(
            InterfaceCompilationError, match="matches no route declared by a Python interface"
        ):
            rest_compiler.compile_sources(sources)

    def test_disable_routes_matching_nothing_names_the_eligible_candidates(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """H2: the fail-closed error must name what could have been disabled."""

        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),
            )

        sources = RouteSources(python=[PyIFace], disabled=[("GET", "/users/missing")])
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        message = str(excinfo.value)
        assert "(POST, '/users/')" in message
        assert "(GET, '/users/{user_id}')" in message

    def test_disable_routes_matching_nothing_lists_every_unmatched_entry(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """L9: two bad entries must both be named, not just the first."""

        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        sources = RouteSources(
            python=[PyIFace],
            disabled=[("GET", "/users/missing"), ("DELETE", "/users/also-missing")],
        )
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        message = str(excinfo.value)
        assert "(DELETE, '/users/also-missing')" in message
        assert "(GET, '/users/missing')" in message

    def test_disable_routes_matching_nothing_caps_the_candidate_list(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """H2: a huge route set does not blow up the error message."""

        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = tuple(
                RestRoute(use_case=GetUserUseCase, method="GET", path=f"/{i}") for i in range(25)
            )

        sources = RouteSources(python=[PyIFace], disabled=[("GET", "/users/missing")])
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        message = str(excinfo.value)
        assert "and 5 more" in message

    def test_disabling_a_config_route_names_no_eligible_candidates(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """H2: no Python routes exist to name when only config declares any."""

        class ConfigIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),)

        sources = RouteSources(config=[ConfigIFace], disabled=[("GET", "/users/{user_id}")])
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        assert "no Python-declared route is eligible for disablement" in str(excinfo.value)

    def test_disabling_a_python_route_lets_config_redeclare_it(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """H1: disable the Python route, redeclare the same path in config — no collision."""

        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="GET", path="/{user_id}"),)

        class ConfigIFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(
                    use_case=GetUserUseCase, method="GET", path="/{user_id}", status_code=299
                ),
            )

        result = rest_compiler.compile_sources(
            RouteSources(
                python=[PyIFace], config=[ConfigIFace], disabled=[("GET", "/users/{user_id}")]
            )
        )

        assert len(result) == 1
        assert result[0].interface_name.endswith("ConfigIFace")
        assert result[0].route.status_code == 299

    def test_disabling_a_config_route_is_refused(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """disable_routes only targets Python-declared routes, never config ones."""

        class ConfigIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),)

        sources = RouteSources(config=[ConfigIFace], disabled=[("GET", "/users/{user_id}")])
        with pytest.raises(
            InterfaceCompilationError, match="matches no route declared by a Python interface"
        ):
            rest_compiler.compile_sources(sources)

    def test_same_origin_collision_advice_has_no_python_disable_instruction(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """H2: two Python interfaces colliding get generic advice, not disable-and-redeclare."""

        class FirstIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        class SecondIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="POST", path="/"),)

        sources = RouteSources(python=[FirstIFace, SecondIFace])
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        message = str(excinfo.value)
        assert "disable the Python route" not in message
        assert "remove the duplicate declaration" in message

    def test_config_vs_config_collision_advice_has_no_python_disable_instruction(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """H2: two config interfaces collide — generic advice, no Python route here."""

        class FirstConfigIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        class SecondConfigIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="POST", path="/"),)

        sources = RouteSources(config=[FirstConfigIFace, SecondConfigIFace])
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        message = str(excinfo.value)
        assert "disable the Python route" not in message
        assert "remove the duplicate declaration" in message

    def test_python_vs_config_collision_keeps_the_disable_and_redeclare_advice(
        self, rest_compiler: RestInterfaceCompiler
    ) -> None:
        """H2: the mixed-origin case keeps the specific advice — a Python route exists."""

        class PyIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        class ConfigIFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="POST", path="/"),)

        sources = RouteSources(python=[PyIFace], config=[ConfigIFace])
        with pytest.raises(InterfaceCompilationError) as excinfo:
            rest_compiler.compile_sources(sources)
        assert "disable the Python route" in str(excinfo.value)


def _prepare(
    use_case: type[UseCase[Any, Any]], **route: Any
) -> tuple[RestInterfaceCompiler, type[RestInterface[Any]]]:
    """Compile *use_case* and declare it on one route; ``route`` overrides the route fields."""

    class IFace(RestInterface[Any]):
        prefix = "/items"
        routes = (
            RestRoute(
                use_case=use_case, **{"method": "GET", "path": "/{item_id}/children", **route}
            ),
        )

    use_case_compiler = UseCaseCompiler()
    use_case_compiler.compile(use_case)
    return RestInterfaceCompiler(use_case_compiler), IFace


class TestCreatedResult:
    """A ``Created`` result must wrap one of the other members of the result union."""

    class CreatesOrReturns(UseCase[Any, str | Created[str]]):
        async def execute(self) -> str | Created[str]:
            return "ok"

    class WrapsAnotherType(UseCase[Any, Any]):
        async def execute(self) -> str | Created[int]:
            return "ok"

    class CreatedTwice(UseCase[Any, Any]):
        async def execute(self) -> str | int | Created[str] | Created[int]:
            return "ok"

    class OnlyCreated(UseCase[Any, Any]):
        async def execute(self) -> Created[str]:
            return Created("ok")

    class BareCreated(UseCase[Any, Any]):
        async def execute(self) -> str | Created:  # type: ignore[type-arg]
            return "ok"

    def test_created_on_a_route_whose_status_is_201_is_refused(self) -> None:
        compiler, interface = _prepare(self.CreatesOrReturns, status_code=201)

        with pytest.raises(InterfaceCompilationError, match="201"):
            compiler.compile(interface)

    @pytest.mark.parametrize("use_case", [WrapsAnotherType, CreatedTwice, OnlyCreated, BareCreated])
    def test_any_other_shape_is_refused(self, use_case: type[UseCase[Any, Any]]) -> None:
        compiler, interface = _prepare(use_case)

        with pytest.raises(InterfaceCompilationError, match=r"Created\[T\]"):
            compiler.compile(interface)


class ItemQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True, rename="camel"):
    owner_id: str | None = None
    limit: int = 50


class ProfileQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    profile: str | None = None


class PathClashQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    item_id: str | None = None


class ListQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    tags: list[str] = []


class Owner(msgspec.Struct, frozen=True):
    name: str


class ObjectQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    owner: Owner | None = None


class UnionQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: int | str = 0


class TestDeclaredQuery:
    """A ``Query()`` route accepts only scalar fields that no other parameter claims."""

    class ListItems(UseCase[Any, str]):
        async def execute(self, item_id: str, query: ItemQuery = Query()) -> str:
            return "ok"

    class WithQuerySpec(UseCase[Any, str]):
        async def execute(self, spec: QuerySpec, query: ItemQuery = Query()) -> str:
            return "ok"

    class WithOptionalQuerySpec(UseCase[Any, str]):
        async def execute(self, spec: QuerySpec | None, query: ItemQuery = Query()) -> str:
            return "ok"

    class ProfileField(UseCase[Any, str]):
        async def execute(self, query: ProfileQuery = Query()) -> str:
            return "ok"

    class PathField(UseCase[Any, str]):
        async def execute(self, item_id: str, query: PathClashQuery = Query()) -> str:
            return "ok"

    class ListField(UseCase[Any, str]):
        async def execute(self, query: ListQuery = Query()) -> str:
            return "ok"

    class ObjectField(UseCase[Any, str]):
        async def execute(self, query: ObjectQuery = Query()) -> str:
            return "ok"

    class UnionField(UseCase[Any, str]):
        async def execute(self, query: UnionQuery = Query()) -> str:
            return "ok"

    def test_the_route_holds_the_declared_fields(self) -> None:
        compiler, interface = _prepare(self.ListItems)

        (route,) = compiler.compile(interface)

        assert [(f.name, f.required) for f in route.query_fields] == [
            ("ownerId", False),
            ("limit", False),
        ]

    @pytest.mark.parametrize(
        ("use_case", "reason"),
        [
            (WithQuerySpec, "QuerySpec"),
            (WithOptionalQuerySpec, "QuerySpec"),
            (ProfileField, "'profile'"),
            (PathField, "'item_id'"),
            (ListField, "'tags'"),
            (ObjectField, "'owner'"),
            (UnionField, "'key'"),
        ],
    )
    def test_any_other_declaration_is_refused(
        self, use_case: type[UseCase[Any, Any]], reason: str
    ) -> None:
        compiler, interface = _prepare(use_case)

        with pytest.raises(InterfaceCompilationError, match=reason):
            compiler.compile(interface)


class _Shelf(BaseModel):
    __tablename__ = "rest_compiler_shelves"

    id: int = ColumnField(primary_key=True)
    label: str = ColumnField(length=32)


class _Book(BaseModel):
    __tablename__ = "rest_compiler_books"

    id: int = ColumnField(primary_key=True)
    title: str = ColumnField(length=64)
    pages: int = ColumnField()
    notes: dict[str, Any] = ColumnField(JSON)
    shelf_id: int = ColumnField(foreign_key="rest_compiler_shelves.id")
    shelf: _Shelf | None = RelationField(
        foreign_key="shelf_id", cardinality=Cardinality.MANY_TO_ONE
    )


class _ListBooks(UseCase[_Book, str]):
    async def execute(self, query: QuerySpec) -> str:
        return "ok"


class _ListAnything(UseCase[Any, str]):
    async def execute(self, query: QuerySpec) -> str:
        return "ok"


class _Blob:
    """A type msgspec cannot describe as a query value."""


class _Opaque(BaseModel):
    __tablename__ = "rest_compiler_opaque"

    id: int = ColumnField(primary_key=True)
    blob: _Blob = ColumnField(String(8))


class _ListOpaque(UseCase[_Opaque, str]):
    async def execute(self, query: QuerySpec) -> str:
        return "ok"


def _compile_columns(*interfaces: type[RestInterface[Any]]) -> list[Any]:
    compiler = UseCaseCompiler()
    for interface in interfaces:
        for route in interface.routes:
            compiler.compile(route.use_case)
    rest = RestInterfaceCompiler(compiler)
    return [route for interface in interfaces for route in rest.compile(interface)]


class TestDeclaredColumns:
    def test_a_field_ref_and_a_column_name_resolve_alike(self) -> None:
        class IFace(RestInterface[_Book]):
            prefix = "/books"
            filterable = (F(_Book).pages, "title")
            sortable = ("title", F(_Book).pages)
            routes = (RestRoute(use_case=_ListBooks, method="GET", path="/"),)

        (route,) = _compile_columns(IFace)

        assert route.filter_fields == (("pages", int), ("title", str))
        assert route.sort_fields == ("title", "pages", "id")

    def test_a_route_declaration_overrides_its_interface(self) -> None:
        class IFace(RestInterface[_Book]):
            prefix = "/books"
            filterable = (F(_Book).pages,)
            routes = (
                RestRoute(use_case=_ListBooks, method="GET", path="/"),
                RestRoute(use_case=_ListBooks, method="GET", path="/titled", filterable=("title",)),
                RestRoute(use_case=_ListBooks, method="GET", path="/plain", filterable=()),
            )

        routes = _compile_columns(IFace)

        assert [route.filter_fields for route in routes] == [
            (("pages", int),),
            (("title", str),),
            (),
        ]

    def test_an_auto_crud_list_route_takes_its_interface_declaration(self) -> None:
        class IFace(RestInterface[_Book]):
            prefix = "/books"
            auto = True
            include = ("list",)
            filterable = ("title",)
            sortable = ("pages",)

        (route,) = _compile_columns(IFace)

        assert (route.filter_fields, route.sort_fields) == ((("title", str),), ("pages", "id"))

    def test_a_route_without_a_query_spec_ignores_an_inherited_declaration(self) -> None:
        class IFace(RestInterface[_Book]):
            prefix = "/books"
            filterable = ("title",)
            sortable = ("title",)
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        (route,) = _compile_columns(IFace)

        assert (route.filter_fields, route.sort_fields) == ((), ())

    @pytest.mark.parametrize("kind", ["filterable", "sortable"])
    def test_a_route_without_a_query_spec_cannot_declare_columns(self, kind: str) -> None:
        route = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")

        class IFace(RestInterface[_Book]):
            prefix = "/books"
            routes = (dataclasses.replace(route, **{kind: ("title",)}),)

        with pytest.raises(InterfaceCompilationError, match=f"declares {kind}.*no QuerySpec"):
            _compile_columns(IFace)

    @pytest.mark.parametrize(
        ("declared", "field"),
        [((F(_Shelf).label,), "label"), (("shelf.label",), "shelf.label")],
    )
    def test_a_column_the_route_cannot_use_fails_compilation(
        self, declared: tuple[Any, ...], field: str
    ) -> None:
        class IFace(RestInterface[_Book]):
            prefix = "/books"
            routes = (RestRoute(use_case=_ListBooks, method="GET", path="/", sortable=declared),)

        with pytest.raises(InterfaceCompilationError, match=rf"\(GET, '/'\).*'{field}'"):
            _compile_columns(IFace)

    def test_a_use_case_without_a_model_cannot_declare_or_inherit_columns(self) -> None:
        class Declares(RestInterface[_Book]):
            prefix = "/books"
            routes = (
                RestRoute(use_case=_ListAnything, method="GET", path="/", filterable=("title",)),
            )

        class Inherits(RestInterface[_Book]):
            prefix = "/books"
            sortable = ("title",)
            routes = (RestRoute(use_case=_ListAnything, method="GET", path="/"),)

        with pytest.raises(InterfaceCompilationError, match="declares filterable.*no model"):
            _compile_columns(Declares)
        with pytest.raises(InterfaceCompilationError, match="inherits sortable.*no model"):
            _compile_columns(Inherits)

    def test_a_filterable_column_a_query_value_cannot_decode_fails_compilation(self) -> None:
        class IFace(RestInterface[_Opaque]):
            prefix = "/opaque"
            routes = (
                RestRoute(use_case=_ListOpaque, method="GET", path="/", filterable=("blob",)),
            )

        with pytest.raises(InterfaceCompilationError, match=r"\(GET, '/'\).*'blob'"):
            _compile_columns(IFace)

    def test_all_resolves_the_root_scalar_columns_and_warns_naming_the_route(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        class IFace(RestInterface[_Book]):
            prefix = "/books"
            filterable = ALL
            routes = (RestRoute(use_case=_ListBooks, method="GET", path="/"),)

        with caplog.at_level(logging.WARNING, logger="loom.rest.compiler"):
            (route,) = _compile_columns(IFace)

        assert dict(route.filter_fields) == {"id": int, "title": str, "pages": int, "shelf_id": int}
        (record,) = caplog.records
        assert record.levelno == logging.WARNING
        assert "IFace: route (GET, '/')" in record.getMessage()
