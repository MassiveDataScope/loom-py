"""Unit tests for RestInterfaceCompiler."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import InterfaceCompilationError, RestInterfaceCompiler, RouteSources
from loom.rest.model import PaginationMode, RestApiDefaults, RestInterface, RestRoute


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
