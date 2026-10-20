"""Declarative REST interface model.

Defines the data structures that describe how UseCases are exposed over HTTP.
No FastAPI or transport-specific code lives here — this module is the
declaration layer; :mod:`loom.rest.compiler` validates and compiles it at
startup and :mod:`loom.rest.router_runtime` binds it to FastAPI.

Usage::

    class UserRestInterface(RestInterface[User]):
        prefix = "/users"
        tags = ("Users",)
        auto = True
        include = ("create", "get", "list", "update")
        routes = (
            RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
            RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),
            RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),
            RestRoute(
                use_case=UpdateUserUseCase,
                method="PATCH",
                path="/{user_id}",
                summary="Partial update user",
            ),
        )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Generic, Literal, TypeAlias, TypeVar, get_args, get_origin

from loom.core.repository.abc.query import PaginationMode
from loom.core.use_case.field_ref import FieldRef
from loom.core.use_case.use_case import UseCase

T = TypeVar("T")

__all__ = [
    "ALL",
    "Filterable",
    "PaginationMode",
    "RestApiDefaults",
    "RestInterface",
    "RestRoute",
]


class _AllFields(Enum):
    ALL = "all"


ALL = _AllFields.ALL
"""Escape hatch for ``filterable``: every scalar field of the route's model.

Compiling a route that uses it logs a warning naming the route: a client may
then filter on any column, a field added later included.
"""

Filterable: TypeAlias = tuple[FieldRef | str, ...] | Literal[_AllFields.ALL]
"""Fields a ``QuerySpec`` route lets a client filter on.

Each entry is a field reference rooted at the route's model
(``F(Order).warehouse.code``) or the same path as a dotted string
(``"warehouse.code"``), or :data:`ALL`.
"""


@dataclass(frozen=True)
class RestRoute:
    """Declaration of a single HTTP endpoint bound to a UseCase.

    All fields except ``use_case``, ``method``, and ``path`` are optional and
    override interface-level or global defaults when set.

    Args:
        use_case: Concrete UseCase class that handles this endpoint.
        method: HTTP method in uppercase (``"GET"``, ``"POST"``, etc.).
        path: Path relative to the interface prefix.  May include path
            parameters (e.g. ``"/{user_id}"``).
        summary: Short OpenAPI summary for the endpoint.
        description: Longer OpenAPI description.
        status_code: Default HTTP success status code.  Defaults to ``200``.
        pagination_mode: Override pagination strategy for this route.
            When ``None``, inherits from the interface or global default.
        profile_default: Default query profile for this route.
            When empty, inherits from the interface or global default.
        allowed_profiles: Profiles that callers may request.  When empty,
            inherits from the interface or global default.
        expose_profile: Whether to expose ``?profile=`` as a public query
            parameter for this route.  Defaults to ``False``.
        allow_pagination_override: Whether callers may override the resolved
            pagination mode using query parameters.  When ``None``, inherits
            from the interface or global default.
        requires_roles: Roles that grant access to this route.  A caller
            holding **any** of them is served; anyone else is refused before
            the use case is constructed.  Empty inherits the interface
            declaration, and an empty interface declaration leaves the route
            open.
        filterable: Fields a client may filter this route's ``QuerySpec``
            on (see :data:`Filterable`).  ``None`` inherits the interface
            declaration; a route that declares none, here or on its
            interface, answers ``400 unsupported_query`` to any filter.

    Example::

        RestRoute(
            use_case=UpdateUserUseCase,
            method="PATCH",
            path="/{user_id}",
            summary="Partial update user",
            status_code=200,
            requires_roles=("user_admin",),
        )
    """

    use_case: type[UseCase[Any, Any, Any]]
    method: str
    path: str
    summary: str = ""
    description: str = ""
    status_code: int = 200
    pagination_mode: PaginationMode | None = None
    allow_pagination_override: bool | None = None
    profile_default: str = ""
    allowed_profiles: tuple[str, ...] = ()
    expose_profile: bool = False
    requires_roles: tuple[str, ...] = ()
    filterable: Filterable | None = None


def _extract_model_type(cls: type[Any]) -> type[Any] | None:
    """Return the concrete model type from ``RestInterface[Model]``.

    Walks ``__orig_bases__`` to find the parameterised ``RestInterface``
    and extracts its first type argument.  Returns ``None`` when the class
    does not carry a concrete type parameter (e.g. intermediate abstract
    bases).

    Args:
        cls: ``RestInterface`` subclass to inspect.

    Returns:
        Concrete model type, or ``None`` if not resolvable.
    """
    for base in getattr(cls, "__orig_bases__", ()):
        if get_origin(base) is not RestInterface:
            continue
        args = get_args(base)
        if args and isinstance(args[0], type):
            return args[0]
    return None


class RestInterface(Generic[T]):
    """Base class for declarative REST interface definitions.

    Subclass to expose one or more UseCases under a common HTTP prefix.
    All class attributes have sensible defaults; override only what differs.

    Attributes:
        prefix: URL prefix for all routes in this interface (e.g. ``"/users"``).
            Must not contain a path placeholder; declare it in the route
            ``path``.
        tags: OpenAPI tags applied to every route.
        auto: When ``True``, signals intent to use standard CRUD URL conventions.
        include: Whitelist of CRUD operation names to expose when ``auto=True``.
            Accepted values: ``"create"``, ``"get"``, ``"list"``, ``"update"``,
            ``"delete"``.  Empty tuple means all operations are allowed.
        auto_crud_model: Model whose repository the generated CRUD routes need;
            set only when routes were actually generated.
        routes: Explicit route declarations.  Custom routes always override
            auto-CRUD routes with the same ``(method, path)``.
        pagination_mode: Default pagination strategy for list endpoints in this
            interface.  Overridden per-route by :attr:`RestRoute.pagination_mode`.
        profile_default: Default query profile for routes in this interface.
        allowed_profiles: Profiles available to callers of routes in this
            interface.
        expose_profile: Whether this interface publicly accepts
            ``?profile=...`` by default. Can be overridden per-route.
        allow_pagination_override: Whether list endpoints may switch
            pagination mode from query parameters by default.
        requires_roles: Roles granting access to every route of this
            interface.  Overridden per-route by
            :attr:`RestRoute.requires_roles`.
        filterable: Fields a client may filter the ``QuerySpec`` routes of
            this interface on, auto-CRUD ``list`` included.  Overridden
            per-route by :attr:`RestRoute.filterable`.

    Example::

        class OrderRestInterface(RestInterface[Order]):
            prefix = "/orders"
            tags = ("Orders",)
            auto = True
            include = ("create", "get", "list")
            routes = (
                RestRoute(use_case=CreateOrderUseCase, method="POST", path="/"),
                RestRoute(use_case=GetOrderUseCase, method="GET", path="/{order_id}"),
                RestRoute(use_case=ListOrdersUseCase, method="GET", path="/"),
            )
    """

    prefix: str = ""
    tags: tuple[str, ...] = ()
    auto: bool = False
    auto_crud_model: ClassVar[type[Any] | None] = None
    include: tuple[str, ...] = ()
    routes: tuple[RestRoute, ...] = ()
    pagination_mode: PaginationMode | None = None
    allow_pagination_override: bool | None = None
    profile_default: str = ""
    allowed_profiles: tuple[str, ...] = ()
    expose_profile: bool = False
    requires_roles: tuple[str, ...] = ()
    filterable: Filterable | None = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-populate routes when ``auto=True`` and no routes are declared.

        Fires whenever a subclass of ``RestInterface`` is defined.  Calls
        :func:`~loom.rest.autocrud.build_auto_routes` only when:

        - ``cls.auto`` is ``True``
        - ``cls.routes`` is empty (user-declared routes take precedence)
        - The class carries a concrete model type parameter
        """
        super().__init_subclass__(**kwargs)
        if not cls.auto or cls.routes:
            return
        model_type = _extract_model_type(cls)
        if model_type is None:
            return
        from loom.rest.autocrud import build_auto_routes  # lazy — avoids circular import

        cls.routes = build_auto_routes(model_type, cls.include)
        cls.auto_crud_model = model_type


@dataclass(frozen=True)
class RestApiDefaults:
    """Global REST API defaults applied when interface or route level is unset.

    Args:
        pagination_mode: Default pagination strategy.  Defaults to
            :attr:`PaginationMode.OFFSET`.
        profile_default: Default query profile name.  Defaults to
            ``"default"``.
        allowed_profiles: Globally allowed profiles.  Defaults to empty
            (no restriction imposed at global level).
        allow_pagination_override: Whether query parameters may override the
            configured pagination mode.  Defaults to ``True`` for backwards
            compatibility.
        max_limit: Ceiling applied to ``?limit=``.  A larger request is clamped
            to this value, so no single call can turn into a full scan.

    Example::

        defaults = RestApiDefaults(
            pagination_mode=PaginationMode.CURSOR,
            profile_default="summary",
            max_limit=200,
        )
    """

    pagination_mode: PaginationMode = PaginationMode.OFFSET
    allow_pagination_override: bool = True
    profile_default: str = "default"
    allowed_profiles: tuple[str, ...] = field(default_factory=tuple)
    max_limit: int = 1000
