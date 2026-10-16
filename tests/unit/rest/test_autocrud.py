"""Unit tests for loom.rest.autocrud."""

from typing import Any

import msgspec
import pytest

from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.model import BaseModel, ColumnField, LoomStruct
from loom.core.model.enums import Cardinality, ServerDefault
from loom.core.model.relation import RelationField
from loom.core.use_case.constants import CrudOp
from loom.core.use_case.use_case import UseCase
from loom.rest.autocrud import (
    _derive_create_struct,
    _derive_update_struct,
    _get_or_create,
    _id_coerce,
    _server_generated_names,
    build_auto_routes,
)
from loom.rest.model import RestRoute

# ---------------------------------------------------------------------------
# Test fixtures — plain structs (no ColumnField metadata)
# ---------------------------------------------------------------------------


class _IntItem(LoomStruct):
    id: int
    name: str


class _StrItem(LoomStruct):
    id: str
    name: str


class _NoIdItem(LoomStruct):
    name: str


# ---------------------------------------------------------------------------
# Test fixtures — BaseModel subclasses with full column metadata
# ---------------------------------------------------------------------------


class _RichModel(BaseModel):
    """Model with PK, server-default, FK column, and a Relation."""

    __tablename__ = "test_rich_model_autocrud"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=100)
    created_at: str | None = ColumnField(server_default=ServerDefault.NOW)
    customer_id: int = ColumnField(foreign_key="customers.id")
    customer: list[Any] = RelationField(
        foreign_key="customer_id",
        cardinality=Cardinality.MANY_TO_ONE,
    )


class _CompositeModel(BaseModel):
    __tablename__ = "test_composite_model_autocrud"

    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=100)


# ---------------------------------------------------------------------------
# TestBuildAutoRoutes
# ---------------------------------------------------------------------------


class TestBuildAutoRoutes:
    def test_returns_five_routes_when_include_empty(self) -> None:
        routes = build_auto_routes(_IntItem, include=())
        assert len(routes) == 5

    def test_include_filters_ops(self) -> None:
        routes = build_auto_routes(_IntItem, include=(CrudOp.CREATE, CrudOp.GET))
        assert len(routes) == 2
        methods = {r.method for r in routes}
        assert methods == {"POST", "GET"}

    def test_create_has_status_201(self) -> None:
        routes = build_auto_routes(_IntItem, include=(CrudOp.CREATE,))
        assert routes[0].status_code == 201

    def test_non_create_have_status_200(self) -> None:
        routes = build_auto_routes(
            _IntItem, include=(CrudOp.GET, CrudOp.LIST, CrudOp.UPDATE, CrudOp.DELETE)
        )
        for route in routes:
            assert route.status_code == 200

    def test_get_path_is_slash_id(self) -> None:
        routes = build_auto_routes(_IntItem, include=(CrudOp.GET,))
        assert routes[0].path == "/{id}"

    def test_list_path_is_slash(self) -> None:
        routes = build_auto_routes(_IntItem, include=(CrudOp.LIST,))
        assert routes[0].path == "/"

    def test_create_path_is_slash(self) -> None:
        routes = build_auto_routes(_IntItem, include=(CrudOp.CREATE,))
        assert routes[0].path == "/"

    def test_update_path_is_slash_id(self) -> None:
        routes = build_auto_routes(_IntItem, include=(CrudOp.UPDATE,))
        assert routes[0].path == "/{id}"

    def test_delete_path_is_slash_id(self) -> None:
        routes = build_auto_routes(_IntItem, include=(CrudOp.DELETE,))
        assert routes[0].path == "/{id}"

    def test_all_routes_are_rest_route_instances(self) -> None:
        routes = build_auto_routes(_IntItem, include=())
        assert all(isinstance(r, RestRoute) for r in routes)


# ---------------------------------------------------------------------------
# TestAutoCrudCache
# ---------------------------------------------------------------------------


class TestCompositeKeyRoutes:
    def test_default_routes_leave_out_the_by_id_operations(self) -> None:
        routes = build_auto_routes(_CompositeModel, include=())

        assert {(route.method, route.path) for route in routes} == {("POST", "/"), ("GET", "/")}

    def test_requested_by_id_operation_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"\(account_id, id\)"):
            build_auto_routes(_CompositeModel, include=("list", "get"))


class TestAutoCrudCache:
    def test_same_uc_class_returned_on_repeated_call(self) -> None:
        class _Item(LoomStruct):
            id: int

        first = _get_or_create(_Item)
        second = _get_or_create(_Item)
        assert first is second

    def test_different_models_get_different_classes(self) -> None:
        class _A(LoomStruct):
            id: int

        class _B(LoomStruct):
            id: int

        a_ucs = _get_or_create(_A)
        b_ucs = _get_or_create(_B)
        assert a_ucs[CrudOp.CREATE] is not b_ucs[CrudOp.CREATE]

    def test_cache_contains_input_structs(self) -> None:
        class _Cached(LoomStruct):
            name: str

        ucs = _get_or_create(_Cached)
        assert "create_input" in ucs
        assert "update_input" in ucs
        assert issubclass(ucs["create_input"], Command)  # not a CrudOp key
        assert issubclass(ucs["update_input"], Command)  # not a CrudOp key


# ---------------------------------------------------------------------------
# TestAutoCrudUseCaseCompilation
# ---------------------------------------------------------------------------


class TestAutoCrudUseCaseCompilation:
    def test_create_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs[CrudOp.CREATE])
        assert plan is not None
        assert plan.input_binding is not None
        assert plan.input_binding.name == "cmd"

    def test_get_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs[CrudOp.GET])
        assert plan is not None
        assert any(pb.name == "id" for pb in plan.param_bindings)

    def test_list_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs[CrudOp.LIST])
        assert plan is not None
        # query is a ParamBinding (not Input), profile is also a ParamBinding
        param_names = {pb.name for pb in plan.param_bindings}
        assert "query" in param_names

    def test_update_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs[CrudOp.UPDATE])
        assert plan is not None
        assert plan.input_binding is not None
        assert any(pb.name == "id" for pb in plan.param_bindings)

    def test_delete_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs[CrudOp.DELETE])
        assert plan is not None
        assert any(pb.name == "id" for pb in plan.param_bindings)

    def test_auto_uc_orig_bases_has_concrete_model(self) -> None:
        """Factory embeds model in __orig_bases__ so UseCaseFactory can infer repo."""
        import typing

        ucs = _get_or_create(_IntItem)
        create_uc = ucs[CrudOp.CREATE]
        for base in getattr(create_uc, "__orig_bases__", ()):
            if typing.get_origin(base) is UseCase:
                args = typing.get_args(base)
                assert len(args) in (2, 3)
                assert args[0] is _IntItem
                return
        pytest.fail("No UseCase[_IntItem, ...] found in __orig_bases__")

    def test_generated_use_cases_have_typed_return_annotations(self) -> None:
        ucs = _get_or_create(_IntItem)
        for op in CrudOp:
            use_case = ucs[op]
            return_hint = use_case.execute.__annotations__.get("return")
            assert return_hint is not None
            assert return_hint is not Any


# ---------------------------------------------------------------------------
# TestIdCoerce
# ---------------------------------------------------------------------------


class TestIdCoerce:
    def test_int_model_coerces_to_int(self) -> None:
        coerce = _id_coerce(_IntItem)
        assert coerce("42") == 42
        assert isinstance(coerce("1"), int)

    def test_str_model_returns_str(self) -> None:
        coerce = _id_coerce(_StrItem)
        result = coerce("abc")
        assert result == "abc"
        assert isinstance(result, str)

    def test_model_without_id_field_returns_str(self) -> None:
        coerce = _id_coerce(_NoIdItem)
        assert coerce("hello") == "hello"


# ---------------------------------------------------------------------------
# TestDeriveCreateStruct
# ---------------------------------------------------------------------------


class TestDeriveCreateStruct:
    def test_pk_autoincrement_field_excluded(self) -> None:
        """id with primary_key=True, autoincrement=True must not appear in CreateInput."""
        create_input = _derive_create_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(create_input)}
        assert "id" not in field_names

    def test_server_default_field_excluded(self) -> None:
        """Fields with server_default must not appear in CreateInput."""
        create_input = _derive_create_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(create_input)}
        assert "created_at" not in field_names

    def test_relation_field_excluded(self) -> None:
        """Relation descriptor fields must not appear in CreateInput."""
        create_input = _derive_create_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(create_input)}
        assert "customer" not in field_names

    def test_fk_column_included(self) -> None:
        """The FK column field (customer_id) IS a writable column and must be included."""
        create_input = _derive_create_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(create_input)}
        assert "customer_id" in field_names

    def test_writable_column_included(self) -> None:
        """Plain writable columns (name) must be present in CreateInput."""
        create_input = _derive_create_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(create_input)}
        assert "name" in field_names

    def test_create_input_is_command(self) -> None:
        create_input = _derive_create_struct(_RichModel)
        assert issubclass(create_input, Command)
        assert callable(getattr(create_input, "from_payload", None))

    def test_create_input_name(self) -> None:
        create_input = _derive_create_struct(_RichModel)
        assert create_input.__name__ == "_RichModelCreateInput"

    def test_plain_struct_all_fields_included(self) -> None:
        """For a plain struct without ColumnField metadata, all fields are writable."""
        create_input = _derive_create_struct(_IntItem)
        field_names = {f.name for f in msgspec.structs.fields(create_input)}
        assert "id" in field_names
        assert "name" in field_names


# ---------------------------------------------------------------------------
# TestDeriveUpdateStruct
# ---------------------------------------------------------------------------


class TestDeriveUpdateStruct:
    def test_all_writable_fields_have_unset_default(self) -> None:
        """Every field in UpdateInput must have msgspec.UNSET as its default."""
        update_input = _derive_update_struct(_RichModel)
        for sf in msgspec.structs.fields(update_input):
            assert sf.default is msgspec.UNSET, f"Field {sf.name!r} default is not UNSET"

    def test_all_writable_fields_include_unset_type(self) -> None:
        """Every field in UpdateInput must have UnsetType in its type union."""
        from types import UnionType
        from typing import get_args, get_origin

        update_input = _derive_update_struct(_RichModel)
        for sf in msgspec.structs.fields(update_input):
            typ = sf.type
            origin = get_origin(typ)
            assert origin in (UnionType,), f"Field {sf.name!r} type {typ!r} is not a union"
            assert msgspec.UnsetType in get_args(typ), (
                f"Field {sf.name!r} does not include UnsetType"
            )

    def test_pk_excluded_from_update(self) -> None:
        update_input = _derive_update_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(update_input)}
        assert "id" not in field_names

    def test_server_default_excluded_from_update(self) -> None:
        update_input = _derive_update_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(update_input)}
        assert "created_at" not in field_names

    def test_fk_column_in_update(self) -> None:
        update_input = _derive_update_struct(_RichModel)
        field_names = {f.name for f in msgspec.structs.fields(update_input)}
        assert "customer_id" in field_names

    def test_update_input_name(self) -> None:
        update_input = _derive_update_struct(_RichModel)
        assert update_input.__name__ == "_RichModelUpdateInput"


# ---------------------------------------------------------------------------
# TestServerGeneratedNames
# ---------------------------------------------------------------------------


class TestServerGeneratedNames:
    def test_pk_autoincrement_excluded(self) -> None:
        excluded = _server_generated_names(_RichModel)
        assert "id" in excluded

    def test_server_default_excluded(self) -> None:
        excluded = _server_generated_names(_RichModel)
        assert "created_at" in excluded

    def test_writable_column_not_excluded(self) -> None:
        excluded = _server_generated_names(_RichModel)
        assert "name" not in excluded
        assert "customer_id" not in excluded

    def test_plain_struct_has_no_excluded(self) -> None:
        """Plain struct with no ColumnField metadata has no server-generated names."""
        excluded = _server_generated_names(_IntItem)
        assert len(excluded) == 0
