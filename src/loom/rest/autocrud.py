"""Auto-CRUD route and UseCase generators.

Generates concrete :class:`~loom.core.use_case.use_case.UseCase` subclasses and
:class:`~loom.rest.model.RestRoute` objects for the five standard CRUD operations,
given a domain model type.

The generated classes are cached per model — multiple interfaces sharing the same
model reuse the same UseCase classes.

NOTE: This module intentionally omits ``from __future__ import annotations`` so
that annotation expressions are evaluated eagerly at class-definition time.
The factory functions capture ``model`` as a closure variable and embed it
directly in the ``execute`` signature, making ``typing.get_type_hints`` work
without requiring the model name to be present in the module's global namespace.

Usage::

    from loom.rest.autocrud import build_auto_routes

    routes = build_auto_routes(User, include=("create", "get", "list"))
"""

from collections.abc import Callable
from typing import Any, cast, get_type_hints

import msgspec

from loom.core.command import Command
from loom.core.errors import NotFound
from loom.core.model.introspection import get_column_fields, get_id_attributes
from loom.core.repository.abc.query import CursorResult, PageResult, QuerySpec
from loom.core.repository.abc.repo_for import Creatable, Deletable, Listable, Readable, Updatable
from loom.core.repository.registration import capabilities_of
from loom.core.repository.registry import get_repository_registration
from loom.core.use_case.constants import KEY_SEPARATOR, CrudOp
from loom.core.use_case.keys import get_use_case_key, set_use_case_key
from loom.core.use_case.markers import Input
from loom.core.use_case.registry import model_entity_key
from loom.core.use_case.use_case import UseCase

__all__ = [
    "CRUD_OP_CAPABILITY",
    "build_auto_routes",
    "crud_op_of",
    "registered_repository_type",
    "supported_crud_ops",
]

# ---------------------------------------------------------------------------
# Operation metadata
# ---------------------------------------------------------------------------

_ALL_OPS: tuple[CrudOp, ...] = tuple(CrudOp)

_OP_METHOD: dict[str, str] = {
    CrudOp.CREATE: "POST",
    CrudOp.GET: "GET",
    CrudOp.LIST: "GET",
    CrudOp.UPDATE: "PATCH",
    CrudOp.DELETE: "DELETE",
}
_ID_ROUTE_PATH = "/{id}"
_OP_PATH: dict[str, str] = {
    CrudOp.CREATE: "/",
    CrudOp.GET: _ID_ROUTE_PATH,
    CrudOp.LIST: "/",
    CrudOp.UPDATE: _ID_ROUTE_PATH,
    CrudOp.DELETE: _ID_ROUTE_PATH,
}
_OP_STATUS: dict[str, int] = {CrudOp.CREATE: 201}
_BY_ID_OPS = frozenset({CrudOp.GET, CrudOp.UPDATE, CrudOp.DELETE})

CRUD_OP_CAPABILITY: dict[CrudOp, type] = {
    CrudOp.CREATE: Creatable,
    CrudOp.GET: Readable,
    CrudOp.LIST: Listable,
    CrudOp.UPDATE: Updatable,
    CrudOp.DELETE: Deletable,
}
"""Repository capability protocol each CRUD operation needs."""

# ---------------------------------------------------------------------------
# ID coercion
# ---------------------------------------------------------------------------


def _id_coerce(model: type[Any]) -> Callable[[Any], Any]:
    """Return ``int`` coercer when model declares ``id: int``, else identity.

    Args:
        model: Domain model type to inspect.

    Returns:
        ``int`` builtin if the ``id`` field annotation is ``int``,
        otherwise an identity callable that returns the value unchanged.
    """
    try:
        hints = get_type_hints(model)
    except Exception:
        return lambda x: x
    return int if hints.get("id") is int else (lambda x: x)


def _id_annotation(model: type[Any]) -> Any:
    """Return the declared ``id`` annotation for path-parameter typing."""
    try:
        hints = get_type_hints(model)
    except Exception:
        return str
    annotation = hints.get("id", str)
    return annotation if annotation is not Any else str


# ---------------------------------------------------------------------------
# Input struct derivation
# ---------------------------------------------------------------------------


def _server_generated_names(model: type[Any]) -> frozenset[str]:
    """Return field names that are server-assigned and must not appear in write inputs.

    Excludes primary keys, autoincrement columns, and columns with server defaults.

    Args:
        model: Domain model type to inspect.

    Returns:
        Frozenset of field names that the server generates automatically.
    """
    excluded: set[str] = set()
    for name, info in get_column_fields(model).items():
        f = info.field
        if f.primary_key or f.autoincrement or f.server_default is not None:
            excluded.add(name)
    return frozenset(excluded)


def _field_tuple(
    name: str,
    typ: Any,
    sf: msgspec.structs.FieldInfo,
) -> tuple[str, Any, Any]:
    """Build a ``defstruct`` field specification from a struct field's default.

    ``UNSET`` and ``NODEFAULT`` are both treated as "no default" (required field),
    because ``UNSET`` on a column field whose type does not include ``UnsetType``
    is msgspec's sentinel for a required field.

    Args:
        name: Field name.
        typ: Resolved Python type (``Annotated`` metadata stripped).
        sf: Struct field info from ``msgspec.structs.fields``.

    Returns:
        ``(name, type, default)``, where the default is ``msgspec.NODEFAULT`` for
        required fields and a ``msgspec.field(...)`` wrapper for default factories.
    """
    if sf.default is msgspec.NODEFAULT or sf.default is msgspec.UNSET:
        return (name, typ, msgspec.NODEFAULT)
    if sf.default_factory is not msgspec.NODEFAULT:
        return (name, typ, msgspec.field(default_factory=sf.default_factory))
    return (name, typ, sf.default)


def _derive_create_struct(model: type[Any]) -> type[Command]:
    """Derive a ``{ModelName}CreateInput`` command with only user-writable fields.

    Excluded: primary keys, autoincrement columns, server-default columns,
    relations, and projections.  Types are taken from ``get_type_hints(model)``
    without ``include_extras`` so that ``Annotated`` metadata is stripped while
    ``Optional`` unions are preserved.

    Args:
        model: Domain model type.

    Returns:
        A ``Command`` subclass named ``{ModelName}CreateInput``.

    Example::

        CreateInput = _derive_create_struct(User)
        # CreateInput has only writable fields — not id or server timestamps
    """
    excluded = _server_generated_names(model)
    writable_names = set(get_column_fields(model).keys()) - excluded
    hints = get_type_hints(model)
    field_defs: list[tuple[str, Any, Any]] = [
        _field_tuple(name, hints[name], sf)
        for sf in msgspec.structs.fields(model)
        for name in (sf.name,)
        if name in writable_names and name in hints
    ]
    return cast(
        type[Command],
        msgspec.defstruct(
            f"{model.__name__}CreateInput",
            field_defs,
            bases=(Command,),
            module=__name__,
            kw_only=True,
            frozen=True,
        ),
    )


def _derive_update_struct(model: type[Any]) -> type[Command]:
    """Derive a ``{ModelName}UpdateInput`` command with all writable fields optional.

    Every included field has its type widened to ``T | UnsetType`` and its default
    set to ``msgspec.UNSET``, giving PATCH semantics: fields absent from the
    request body remain ``UNSET`` and are not written to the database.

    Args:
        model: Domain model type.

    Returns:
        A ``Command`` subclass named ``{ModelName}UpdateInput``.

    Example::

        UpdateInput = _derive_update_struct(User)
        # UpdateInput(name="Alice") leaves all other fields UNSET (not updated)
    """
    excluded = _server_generated_names(model)
    writable_names = set(get_column_fields(model).keys()) - excluded
    hints = get_type_hints(model)
    field_defs: list[tuple[str, Any, Any]] = [
        (name, hints[name] | msgspec.UnsetType, msgspec.UNSET)
        for sf in msgspec.structs.fields(model)
        for name in (sf.name,)
        if name in writable_names and name in hints
    ]
    return cast(
        type[Command],
        msgspec.defstruct(
            f"{model.__name__}UpdateInput",
            field_defs,
            bases=(Command,),
            module=__name__,
            kw_only=True,
            frozen=True,
        ),
    )


# ---------------------------------------------------------------------------
# UseCase class factories
# ---------------------------------------------------------------------------


def _make_create(model: type[Any], create_input: type[Any]) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that creates a new entity via the main repository.

    Args:
        model: Domain model type used as TModel and TResult.
        create_input: Derived struct containing only user-writable fields.

    Returns:
        A concrete UseCase subclass named ``AutoCreate<ModelName>``.
    """

    class AutoCreate(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(self, cmd: create_input = Input()) -> model:  # type: ignore[valid-type]
            return cast(Any, await self.main_repo.create(cmd))  # type: ignore[no-any-return]

    AutoCreate.__name__ = f"AutoCreate{model.__name__}"
    AutoCreate.__qualname__ = f"AutoCreate{model.__name__}"
    AutoCreate.__module__ = __name__
    AutoCreate.__doc__ = f"Create a new {model.__name__}."
    return AutoCreate


def _make_get(
    model: type[Any],
    id_type: Any,
    coerce: Callable[[Any], Any],
) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that fetches a single entity by id.

    Args:
        model: Domain model type.
        coerce: Callable that converts the raw path-param string to the id type.

    Returns:
        A concrete UseCase subclass named ``AutoGet<ModelName>``.
    """

    class AutoGet(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(
            self,
            id: id_type,  # pyright: ignore[reportInvalidTypeForm]
            profile: str = "default",
        ) -> model:  # type: ignore[valid-type]
            entity = await self.main_repo.get_by_id(coerce(id), profile=profile)
            if entity is None:
                raise NotFound(model.__name__, id=coerce(id))
            return cast(model, entity)  # type: ignore[valid-type]

    AutoGet.__name__ = f"AutoGet{model.__name__}"
    AutoGet.__qualname__ = f"AutoGet{model.__name__}"
    AutoGet.__module__ = __name__
    AutoGet.__doc__ = f"Get a {model.__name__} by id."
    return AutoGet


def _make_list(model: type[Any]) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that lists entities via a QuerySpec.

    Args:
        model: Domain model type.

    Returns:
        A concrete UseCase subclass named ``AutoList<ModelName>``.
    """

    class AutoList(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(
            self,
            query: QuerySpec,
            profile: str = "default",
        ) -> PageResult[model] | CursorResult[model]:  # type: ignore[valid-type]
            repo: Any = self.main_repo
            return await repo.list_with_query(query, profile=profile)  # type: ignore[no-any-return]

    AutoList.__name__ = f"AutoList{model.__name__}"
    AutoList.__qualname__ = f"AutoList{model.__name__}"
    AutoList.__module__ = __name__
    AutoList.__doc__ = f"List {model.__name__} with filtering, sorting and pagination."
    return AutoList


def _make_update(
    model: type[Any],
    update_input: type[Any],
    id_type: Any,
    coerce: Callable[[Any], Any],
) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that applies a partial update to an entity.

    Args:
        model: Domain model type.
        update_input: Derived struct with all fields optional (PATCH semantics).
        coerce: Callable that converts the raw path-param string to the id type.

    Returns:
        A concrete UseCase subclass named ``AutoUpdate<ModelName>``.
    """

    class AutoUpdate(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(
            self,
            id: id_type,  # pyright: ignore[reportInvalidTypeForm]
            cmd: update_input = Input(),  # type: ignore[valid-type]
        ) -> model:  # type: ignore[valid-type]
            updated = await self.main_repo.update(coerce(id), cmd)
            if updated is None:
                raise NotFound(model.__name__, id=coerce(id))
            return cast(model, updated)  # type: ignore[valid-type]

    AutoUpdate.__name__ = f"AutoUpdate{model.__name__}"
    AutoUpdate.__qualname__ = f"AutoUpdate{model.__name__}"
    AutoUpdate.__module__ = __name__
    AutoUpdate.__doc__ = f"Partially update an existing {model.__name__}."
    return AutoUpdate


def _make_delete(
    model: type[Any],
    id_type: Any,
    coerce: Callable[[Any], Any],
) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that deletes an entity by id.

    Args:
        model: Domain model type.
        coerce: Callable that converts the raw path-param string to the id type.

    Returns:
        A concrete UseCase subclass named ``AutoDelete<ModelName>``.
    """

    class AutoDelete(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(self, id: id_type) -> bool:  # pyright: ignore[reportInvalidTypeForm]
            return await self.main_repo.delete(coerce(id))

    AutoDelete.__name__ = f"AutoDelete{model.__name__}"
    AutoDelete.__qualname__ = f"AutoDelete{model.__name__}"
    AutoDelete.__module__ = __name__
    AutoDelete.__doc__ = f"Delete a {model.__name__} by id."
    return AutoDelete


# ---------------------------------------------------------------------------
# Per-model cache
# ---------------------------------------------------------------------------

_UC_CACHE: dict[type[Any], dict[str, Any]] = {}


def _get_or_create(model: type[Any]) -> dict[str, Any]:
    """Return cached (or freshly built) UseCase classes for all five operations.

    The returned dict contains the five operation keys (``"create"``, ``"get"``,
    ``"list"``, ``"update"``, ``"delete"``) plus ``"create_input"`` and
    ``"update_input"`` holding the derived write-input structs.

    Args:
        model: Domain model type.

    Returns:
        Mapping of operation name to UseCase class, plus input struct entries.
    """
    if model in _UC_CACHE:
        return _UC_CACHE[model]
    coerce = _id_coerce(model)
    id_type = _id_annotation(model)
    create_input = _derive_create_struct(model)
    update_input = _derive_update_struct(model)
    result: dict[str, Any] = {
        CrudOp.CREATE: _make_create(model, create_input),
        CrudOp.GET: _make_get(model, id_type, coerce),
        CrudOp.LIST: _make_list(model),
        CrudOp.UPDATE: _make_update(model, update_input, id_type, coerce),
        CrudOp.DELETE: _make_delete(model, id_type, coerce),
        "create_input": create_input,
        "update_input": update_input,
    }
    entity = model_entity_key(model)
    for operation in _ALL_OPS:
        use_case_type = cast(type[Any], result[operation])
        set_use_case_key(use_case_type, f"{entity}{KEY_SEPARATOR}{operation}")
    _UC_CACHE[model] = result
    return result


# ---------------------------------------------------------------------------
# Capability detection
# ---------------------------------------------------------------------------


def supported_crud_ops(repository_type: type[Any]) -> frozenset[CrudOp]:
    """Return the CRUD operations *repository_type* declares a capability for.

    Args:
        repository_type: The repository class that will serve the model.

    Returns:
        The operations whose protocol in :data:`CRUD_OP_CAPABILITY` is in
        ``capabilities_of(repository_type)``.
    """
    capabilities = capabilities_of(repository_type)
    return frozenset(op for op, proto in CRUD_OP_CAPABILITY.items() if proto in capabilities)


def registered_repository_type(model: type[Any]) -> type[Any] | None:
    """Return the class an explicit ``repository_for`` registration names for *model*."""
    registration = get_repository_registration(model)
    if registration is None or not isinstance(registration.repository_type, type):
        return None
    return registration.repository_type


def _supported_ops(model: type[Any]) -> tuple[CrudOp, ...]:
    """Return the CRUD ops to mount for *model* when ``include`` is empty.

    Delegates to :func:`supported_crud_ops` for the class an explicit
    ``repository_for`` registration names.  Without a registration the model
    is served by the default repository the persistence backend builds later,
    so every operation is mounted.  A model with a composite primary key gets
    no ``/{id}`` operation, since one path segment cannot carry its key.

    Args:
        model: Domain model type.

    Returns:
        The supported operations in :data:`CRUD_OP_CAPABILITY` order.
    """
    repository_type = registered_repository_type(model)
    supported = (
        frozenset(_ALL_OPS) if repository_type is None else supported_crud_ops(repository_type)
    )
    if _composite_key(model):
        supported -= _BY_ID_OPS
    return tuple(op for op in CRUD_OP_CAPABILITY if op in supported)


def _composite_key(model: type[Any]) -> tuple[str, ...] | None:
    """Return the key columns of *model* when its primary key is composite, else ``None``."""
    try:
        names = get_id_attributes(model)
    except ValueError:
        return None
    return names if len(names) > 1 else None


def crud_op_of(use_case_type: type[Any]) -> CrudOp:
    """Return the CRUD operation a generated use case implements.

    Derived from the use-case key ``<entity>:<operation>`` that
    :func:`_get_or_create` sets on every generated class.

    Args:
        use_case_type: A use case class produced by :func:`build_auto_routes`.

    Returns:
        The operation the class implements.

    Raises:
        ValueError: When the class was not generated by this module.
    """
    key = get_use_case_key(use_case_type)
    if key is None:
        raise ValueError(f"{use_case_type.__qualname__} is not a generated CRUD use case")
    _entity, _separator, operation = key.rpartition(KEY_SEPARATOR)
    try:
        return CrudOp(operation)
    except ValueError as exc:
        raise ValueError(
            f"{use_case_type.__qualname__} is not a generated CRUD use case (key {key!r})"
        ) from exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_auto_routes(
    model: type[Any],
    include: tuple[str, ...],
) -> "tuple[Any, ...]":
    """Return ``RestRoute`` objects for the requested CRUD operations.

    Lazily imports :class:`~loom.rest.model.RestRoute` to avoid circular
    imports at module load time.

    Args:
        model: Domain model type for which routes should be generated.
        include: Subset of operation names to expose.  If empty, the
            supported operations are derived automatically from the
            repository's ``-able`` capability protocols via its MRO.
            An explicit list is honoured as-is.

    Returns:
        Tuple of :class:`~loom.rest.model.RestRoute` instances, one per
        requested operation.

    Raises:
        ValueError: If *include* names a ``/{id}`` operation and the model's
            primary key is composite.

    Example::

        routes = build_auto_routes(User, include=("create", "get", "list"))
    """
    from loom.rest.model import RestRoute  # lazy — avoids circular import

    key = _composite_key(model)
    by_id = [str(op) for op in include if op in _BY_ID_OPS]
    if key is not None and by_id:
        raise ValueError(
            f"{model.__name__} has the composite primary key ({', '.join(key)}); "
            f"the /{{id}} path cannot address one row, so {', '.join(by_id)} cannot be generated"
        )
    ops = include or _supported_ops(model)
    use_cases = _get_or_create(model)
    return tuple(
        RestRoute(
            use_case=use_cases[op],
            method=_OP_METHOD[op],
            path=_OP_PATH[op],
            status_code=_OP_STATUS.get(op, 200),
        )
        for op in ops
        if op in _OP_METHOD
    )
