"""Unit tests for loom.rest.config — app.rest.interfaces conversion."""

from __future__ import annotations

import dataclasses
import typing
from typing import Any

import msgspec
import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import InterfaceCompilationError, RestInterfaceCompiler, RouteSources
from loom.rest.config import (
    RestInterfaceConfig,
    RestInterfaceConfigError,
    RestRouteConfig,
    build_interfaces_from_config,
    validate_disable_routes_config,
    validate_interfaces_config,
)
from loom.rest.model import ALL, RestInterface, RestRoute

_MODULE = __name__

# Fields RestRoute/RestRouteConfig declare besides the three required ones
# (use_case, method, path) — used by TestFieldDefaultsMatchThePythonRoute to
# walk every optional field instead of a hand-picked subset.
_ROUTE_REQUIRED_FIELDS = frozenset({"use_case", "method", "path"})


class Ticket(BaseModel):
    __tablename__ = "rest_config_unit_tickets"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    title: str = ColumnField(length=80)


class PingUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class NotAUseCase:
    """A plain class the ``use_case`` reference must not accept."""


class TestBuildInterfacesFromConfig:
    def test_produces_a_real_rest_interface_subclass(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        built = build_interfaces_from_config(cfg)

        assert len(built) == 1
        (interface,) = built
        assert issubclass(interface, RestInterface)
        assert interface.prefix == "/pings"
        assert len(interface.routes) == 1
        assert interface.routes[0].use_case is PingUseCase
        assert interface.routes[0].method == "GET"

    def test_auto_crud_resolves_the_model_reference(self) -> None:
        cfg = {
            "tickets": RestInterfaceConfig(prefix="/tickets", auto=True, model=f"{_MODULE}:Ticket")
        }

        (interface,) = build_interfaces_from_config(cfg)

        assert interface.auto_crud_model is Ticket
        assert len(interface.routes) == 5

    def test_explicit_routes_take_precedence_over_auto(self) -> None:
        cfg = {
            "tickets": RestInterfaceConfig(
                prefix="/tickets",
                auto=True,
                model=f"{_MODULE}:Ticket",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        (interface,) = build_interfaces_from_config(cfg)

        assert len(interface.routes) == 1
        assert interface.auto_crud_model is None


class TestReferenceErrors:
    def test_unresolvable_use_case_module_raises(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(RestRouteConfig(use_case="no.such.module:Ping", method="GET", path="/"),),
            )
        }

        with pytest.raises(RestInterfaceConfigError, match="pings"):
            build_interfaces_from_config(cfg)

    def test_use_case_reference_resolving_to_a_non_use_case_raises(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:NotAUseCase", method="GET", path="/"),
                ),
            )
        }

        with pytest.raises(RestInterfaceConfigError, match="UseCase subclass"):
            build_interfaces_from_config(cfg)

    def test_auto_without_a_model_reference_raises(self) -> None:
        cfg = {"tickets": RestInterfaceConfig(prefix="/tickets", auto=True)}

        with pytest.raises(RestInterfaceConfigError, match="'model'"):
            build_interfaces_from_config(cfg)

    def test_model_reference_resolving_to_a_non_type_raises(self) -> None:
        cfg = {
            "tickets": RestInterfaceConfig(prefix="/tickets", auto=True, model=f"{_MODULE}:_MODULE")
        }

        with pytest.raises(RestInterfaceConfigError, match="does not resolve to a class"):
            build_interfaces_from_config(cfg)


class TestStructuralErrorsNameTheConfigEntry:
    """L6: an empty prefix/routes names the config entry, not a class nobody wrote."""

    def test_empty_prefix_names_the_config_entry(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            build_interfaces_from_config(cfg)
        message = str(excinfo.value)
        assert "app.rest.interfaces.pings" in message
        assert "'prefix'" in message
        assert "RestInterfaceRestInterface" not in message  # no generated-class name leaks in

    def test_empty_routes_without_auto_names_the_config_entry_and_advises_auto_or_routes(
        self,
    ) -> None:
        cfg = {"pings": RestInterfaceConfig(prefix="/pings")}

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            build_interfaces_from_config(cfg)
        message = str(excinfo.value)
        assert "app.rest.interfaces.pings" in message
        assert "'routes'" in message
        assert "RestRoute" not in message  # no advice to declare a Python RestRoute

    def test_auto_with_no_routes_and_a_model_is_not_rejected_here(self) -> None:
        """auto=True defers the emptiness question to CRUD generation, not this guard."""
        cfg = {
            "tickets": RestInterfaceConfig(prefix="/tickets", auto=True, model=f"{_MODULE}:Ticket")
        }

        (interface,) = build_interfaces_from_config(cfg)

        assert len(interface.routes) == 5

    def test_partial_include_typo_names_the_unknown_operation(self) -> None:
        """M2: a typo among otherwise-valid names must not be silently dropped."""
        cfg = {
            "tickets": RestInterfaceConfig(
                prefix="/tickets",
                auto=True,
                model=f"{_MODULE}:Ticket",
                include=("craete", "get"),
            )
        }

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            build_interfaces_from_config(cfg)
        message = str(excinfo.value)
        assert "app.rest.interfaces.tickets" in message
        assert "'include'" in message
        assert "craete" in message
        assert "create" in message  # listed among the valid operations

    def test_include_with_only_typos_names_the_unknown_operation(self) -> None:
        """M2: an all-typo 'include' must not fall through to the empty-routes advice."""
        cfg = {
            "tickets": RestInterfaceConfig(
                prefix="/tickets",
                auto=True,
                model=f"{_MODULE}:Ticket",
                include=("craete",),
            )
        }

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            build_interfaces_from_config(cfg)
        message = str(excinfo.value)
        assert "app.rest.interfaces.tickets" in message
        assert "'include'" in message
        assert "craete" in message
        assert "RestInterface" not in message  # no generated-class name leaks in

    def test_model_without_auto_is_rejected_rather_than_silently_ignored(self) -> None:
        """L4: 'model' has no effect unless 'auto' is set — reject instead of no-op."""
        cfg = {
            "tickets": RestInterfaceConfig(
                prefix="/tickets",
                model=f"{_MODULE}:Ticket",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            build_interfaces_from_config(cfg)
        message = str(excinfo.value)
        assert "app.rest.interfaces.tickets" in message
        assert "'model'" in message
        assert "'auto'" in message


class TestValidateInterfacesConfig:
    """Unknown-key detection on the raw ``app.rest.interfaces`` mapping (H3)."""

    def test_valid_config_passes(self) -> None:
        raw = {
            "pings": {
                "prefix": "/pings",
                "routes": [{"use_case": f"{_MODULE}:PingUseCase", "method": "GET", "path": "/"}],
            }
        }
        validate_interfaces_config(raw)  # does not raise

    def test_unknown_interface_key_names_the_interface_and_valid_keys(self) -> None:
        raw = {"pings": {"prefix": "/pings", "requires_role": ["admin"]}}

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            validate_interfaces_config(raw)
        message = str(excinfo.value)
        assert "app.rest.interfaces.pings" in message
        assert "requires_role" in message
        assert "requires_roles" in message  # listed among the valid keys

    def test_unknown_route_key_names_the_interface_and_valid_keys(self) -> None:
        raw = {
            "pings": {
                "prefix": "/pings",
                "routes": [
                    {
                        "use_case": f"{_MODULE}:PingUseCase",
                        "method": "GET",
                        "path": "/",
                        "statuscode": 200,
                    }
                ],
            }
        }

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            validate_interfaces_config(raw)
        message = str(excinfo.value)
        assert "app.rest.interfaces.pings" in message
        assert "statuscode" in message
        assert "status_code" in message  # listed among the valid keys

    def test_a_non_list_routes_value_is_a_clear_type_error_not_a_stray_unknown_key(self) -> None:
        """L5: a string 'routes' iterated as chars used to abort on "unknown key 'o'"."""
        raw = {"pings": {"prefix": "/pings", "routes": "oops"}}

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            validate_interfaces_config(raw)
        message = str(excinfo.value)
        assert "app.rest.interfaces.pings.routes" in message
        assert "must be a list of route entries" in message
        assert "unknown key" not in message

    def test_a_non_mapping_interface_entry_is_a_clear_type_error(self) -> None:
        # `_section_app_config` decodes app.rest.interfaces as dict[str, Any]
        # precisely so a malformed entry like this one reaches this guard
        # instead of failing earlier inside msgspec with a message that does
        # not name the interface (H3) — no cast needed, entry is really Any.
        raw = {"pings": "oops"}

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            validate_interfaces_config(raw)
        message = str(excinfo.value)
        assert "app.rest.interfaces.pings" in message
        assert "must be a mapping" in message
        assert "unknown key" not in message

    def test_a_non_mapping_route_entry_is_a_clear_type_error(self) -> None:
        raw = {"pings": {"prefix": "/pings", "routes": ["oops"]}}

        with pytest.raises(RestInterfaceConfigError) as excinfo:
            validate_interfaces_config(raw)
        message = str(excinfo.value)
        assert "must be a mapping" in message
        assert "unknown key" not in message


class TestValidateDisableRoutesConfig:
    """Unknown-key detection on the raw ``app.rest.disable_routes`` list (H3)."""

    def test_valid_config_passes(self) -> None:
        validate_disable_routes_config([{"method": "GET", "path": "/pings"}])  # does not raise

    def test_unknown_key_names_it_and_the_valid_keys(self) -> None:
        with pytest.raises(RestInterfaceConfigError) as excinfo:
            validate_disable_routes_config([{"method": "GET", "paths": "/pings"}])
        message = str(excinfo.value)
        assert "app.rest.disable_routes" in message
        assert "paths" in message
        assert "path" in message  # listed among the valid keys

    def test_a_non_mapping_entry_is_a_clear_type_error(self) -> None:
        # `_section_app_config` decodes app.rest.disable_routes as
        # tuple[Any, ...] for the same reason interfaces are — see H3.
        with pytest.raises(RestInterfaceConfigError) as excinfo:
            validate_disable_routes_config(["oops"])
        message = str(excinfo.value)
        assert "app.rest.disable_routes" in message
        assert "must be a mapping" in message
        assert "unknown key" not in message


class TestGeneratedClassNamesAreDistinct:
    """Two config keys that collapse to the same identifier still differ (H5)."""

    def test_underscore_and_hyphen_variants_produce_different_classes(self) -> None:
        cfg = {
            "my_widgets": RestInterfaceConfig(
                prefix="/a",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            ),
            "my-widgets": RestInterfaceConfig(
                prefix="/b",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            ),
        }

        first, second = build_interfaces_from_config(cfg)

        assert first.__qualname__ != second.__qualname__

    def test_collision_message_names_each_config_key_uniquely(self) -> None:
        """A collision between the two now-distinct classes names both, not one twice."""
        uc_compiler = UseCaseCompiler()
        uc_compiler.compile(PingUseCase)
        compiler = RestInterfaceCompiler(uc_compiler)

        cfg = {
            "my_widgets": RestInterfaceConfig(
                prefix="/widgets",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            ),
            "my-widgets": RestInterfaceConfig(
                prefix="/widgets",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            ),
        }
        first, second = build_interfaces_from_config(cfg)

        sources = RouteSources(config=(first, second))
        with pytest.raises(InterfaceCompilationError) as excinfo:
            compiler.compile_sources(sources)
        message = str(excinfo.value)
        assert first.__qualname__ in message
        assert second.__qualname__ in message
        assert first.__qualname__ != second.__qualname__


class TestGeneratedClassesReportTheirOwnModule:
    """A generated class names its origin under introspection, not ``types``."""

    def test_module_is_loom_rest_config_not_types(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        (interface,) = build_interfaces_from_config(cfg)

        assert interface.__module__ == "loom.rest.config"

    def test_qualname_matches_the_class_name(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        (interface,) = build_interfaces_from_config(cfg)

        assert interface.__qualname__ == interface.__name__ == "pingsRestInterface"

    def test_docstring_names_the_config_entry_it_came_from(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        (interface,) = build_interfaces_from_config(cfg)

        assert interface.__doc__ is not None
        assert "app.rest.interfaces.pings" in interface.__doc__


class TestFieldDefaultsMatchThePythonRoute:
    """Field-by-field parity between config defaults and the Python defaults (L3).

    Compares every optional field against a reference ``RestRoute``/
    ``RestInterface`` instance instead of a hand-picked subset of literals,
    and guards that both sides declare the same optional fields — this is
    what actually holds the "field for field" vocabulary decision the module
    docstring promises.
    """

    def test_route_defaults_match_the_python_route_field_by_field(self) -> None:
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        (interface,) = build_interfaces_from_config(cfg)
        built_route = interface.routes[0]
        reference_route = RestRoute(use_case=PingUseCase, method="GET", path="/")

        for f in dataclasses.fields(RestRoute):
            if f.name in _ROUTE_REQUIRED_FIELDS:
                continue
            assert getattr(built_route, f.name) == getattr(reference_route, f.name), f.name

    def test_route_config_declares_the_same_optional_fields_as_the_python_route(self) -> None:
        route_optional_fields = {
            f.name for f in dataclasses.fields(RestRoute) if f.name not in _ROUTE_REQUIRED_FIELDS
        }
        config_optional_fields = {
            f.name
            for f in msgspec.structs.fields(RestRouteConfig)
            if f.name not in _ROUTE_REQUIRED_FIELDS
        }

        assert config_optional_fields == route_optional_fields

    def test_interface_defaults_match_the_python_interface_field_by_field(self) -> None:
        # 'prefix' and 'routes' are required (see TestGeneratedClassesReportTheirOwnModule's
        # sibling in loom.rest.config: _validate_structural rejects either
        # empty), so this compares every *other* optional field against the
        # Python interface's own class defaults.
        cfg = {
            "pings": RestInterfaceConfig(
                prefix="/pings",
                routes=(
                    RestRouteConfig(use_case=f"{_MODULE}:PingUseCase", method="GET", path="/"),
                ),
            )
        }

        (interface,) = build_interfaces_from_config(cfg)

        for field_name in _interface_field_names() - {"prefix", "routes"}:
            assert getattr(interface, field_name) == getattr(RestInterface, field_name), field_name

    def test_interface_config_declares_the_same_fields_as_the_python_interface(self) -> None:
        # 'model' is the one key with no Python equivalent — see the module
        # docstring of loom.rest.config for why.
        config_fields = {
            f.name for f in msgspec.structs.fields(RestInterfaceConfig) if f.name != "model"
        }

        assert config_fields == _interface_field_names()


class TestRestInterfaceConfigErrorIsPublic:
    """L3: the only exception a config-error handler needs is reachable from loom.rest."""

    def test_reexported_from_loom_rest(self) -> None:
        import loom.rest

        assert loom.rest.RestInterfaceConfigError is RestInterfaceConfigError
        assert "RestInterfaceConfigError" in loom.rest.__all__


def _interface_field_names() -> frozenset[str]:
    """Return ``RestInterface``'s overridable field names, excluding ``auto_crud_model``.

    ``auto_crud_model`` is a ``ClassVar`` computed by CRUD generation, not a
    declarable field — config has no key for it, by design.
    """
    hints = typing.get_type_hints(RestInterface)
    return frozenset(
        name for name, hint in hints.items() if typing.get_origin(hint) is not typing.ClassVar
    )


class TestDeclaredColumns:
    def test_column_names_and_all_reach_the_interface_and_route(self) -> None:
        raw = {
            "prefix": "/pings",
            "filterable": ["title"],
            "sortable": "all",
            "routes": [
                {
                    "use_case": f"{_MODULE}:PingUseCase",
                    "method": "GET",
                    "path": "/",
                    "filterable": "all",
                    "sortable": ["title"],
                }
            ],
        }

        (interface,) = build_interfaces_from_config(
            {"pings": msgspec.convert(raw, RestInterfaceConfig)}
        )
        (route,) = interface.routes

        assert (interface.filterable, interface.sortable) == (("title",), ALL)
        assert (route.filterable, route.sortable) == (ALL, ("title",))
