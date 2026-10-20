"""Declare REST interfaces from configuration instead of a Python class.

``app.rest.interfaces`` mirrors :class:`~loom.rest.model.RestInterface` and
:class:`~loom.rest.model.RestRoute` field for field: the same key means the
same thing, with the same default, so the existing Python documentation
keeps teaching the YAML form too.

The one key with no Python equivalent is ``model`` — a ``module:Symbol``
reference to the entity auto-CRUD needs, since YAML cannot spell the generic
parameter ``RestInterface[Model]`` carries in Python. It is required only
when an interface asks for auto-CRUD (``auto: true``) and declares no
explicit routes.

Every reference (``use_case`` and ``model``) is resolved through the same
offline ``module:symbol`` reader the AI artifact format already uses
(:func:`loom.core.symbols.import_symbol`), so a bad reference fails
the same way at startup, whichever pillar wrote it.

Conversion produces the exact same :class:`~loom.rest.model.RestRoute` and
:class:`~loom.rest.model.RestInterface` objects a Python subclass would —
there is no second compiler: the dynamic class is fed through
``RestInterface.__init_subclass__`` like any other subclass, so ``auto``
CRUD generation runs unchanged, and the objects it produces are handed to
the same :class:`~loom.rest.compiler.RestInterfaceCompiler` used for Python
interfaces.
"""

from __future__ import annotations

import types
from collections.abc import Mapping, Sequence
from typing import Any, Literal

import msgspec

from loom.core.config.errors import ConfigError
from loom.core.repository.abc.query import PaginationMode
from loom.core.symbols import import_symbol
from loom.core.use_case.constants import CrudOp
from loom.core.use_case.use_case import UseCase
from loom.rest.model import ALL, Filterable, RestInterface, RestRoute

__all__ = [
    "DisableRouteConfig",
    "RestInterfaceConfig",
    "RestInterfaceConfigError",
    "RestRouteConfig",
    "build_interfaces_from_config",
    "validate_disable_routes_config",
    "validate_interfaces_config",
]

_VALID_CRUD_OPS = frozenset(CrudOp)


class RestInterfaceConfigError(ConfigError):
    """Raised when an ``app.rest.interfaces`` entry cannot be built.

    Covers an unresolved ``module:Symbol`` reference (``use_case`` or
    ``model``), an ``auto`` interface with no ``model`` to derive its CRUD
    routes from, a ``model`` given without ``auto``, and an ``include``
    entry that names no known CRUD operation.

    Args:
        message: Human-readable description naming the interface at fault.
    """


class RestRouteConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """One route of an ``app.rest.interfaces.<name>.routes`` entry.

    Field names and defaults mirror :class:`~loom.rest.model.RestRoute`
    exactly. ``use_case``, ``method`` and ``path`` are the only required
    keys; everything else inherits the same default the Python route does.

    Unknown keys abort startup rather than being silently dropped — see
    :func:`validate_interfaces_config` for the diagnostic this produces.

    Args:
        use_case: ``module:Symbol`` reference to the ``UseCase`` subclass.
        method: HTTP method in uppercase.
        path: Path relative to the interface prefix.
        summary: Short OpenAPI summary.
        description: Longer OpenAPI description.
        status_code: Default HTTP success status code.
        pagination_mode: Override pagination strategy for this route.
        allow_pagination_override: Whether callers may override pagination.
        profile_default: Default query profile for this route.
        allowed_profiles: Profiles callers may request.
        expose_profile: Whether ``?profile=`` is publicly accepted.
        requires_roles: Roles that grant access to this route.
        filterable: Dotted paths of the fields callers may filter on, or
            ``all`` for :data:`~loom.rest.model.ALL`.
    """

    use_case: str
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
    filterable: tuple[str, ...] | Literal["all"] | None = None


class RestInterfaceConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """One ``app.rest.interfaces.<name>`` entry.

    Field names and defaults mirror :class:`~loom.rest.model.RestInterface`
    exactly, except ``model``, which has no Python equivalent (see module
    docstring).

    Unknown keys abort startup rather than being silently dropped — see
    :func:`validate_interfaces_config` for the diagnostic this produces.

    Args:
        prefix: URL prefix for every route in this interface.
        tags: OpenAPI tags applied to every route.
        auto: Whether to generate standard CRUD routes.
        include: Whitelist of CRUD operation names, when ``auto`` is set. Each
            entry must be one of ``create``, ``get``, ``list``, ``update``,
            ``delete`` — an unrecognised entry aborts startup rather than
            being silently dropped.
        routes: Explicit route declarations.
        model: ``module:Symbol`` reference to the entity auto-CRUD serves.
            Required only when ``auto`` is set and ``routes`` is empty;
            rejected when ``auto`` is not set, since it would otherwise
            resolve and then be silently ignored.
        pagination_mode: Default pagination strategy for this interface.
        allow_pagination_override: Whether pagination may be overridden.
        profile_default: Default query profile for this interface.
        allowed_profiles: Profiles available to callers of this interface.
        expose_profile: Whether ``?profile=`` is publicly accepted by default.
        requires_roles: Roles granting access to every route of this interface.
        filterable: Dotted paths of the fields callers may filter the
            interface's routes on, or ``all`` for :data:`~loom.rest.model.ALL`.
    """

    prefix: str = ""
    tags: tuple[str, ...] = ()
    auto: bool = False
    include: tuple[str, ...] = ()
    routes: tuple[RestRouteConfig, ...] = ()
    model: str = ""
    pagination_mode: PaginationMode | None = None
    allow_pagination_override: bool | None = None
    profile_default: str = ""
    allowed_profiles: tuple[str, ...] = ()
    expose_profile: bool = False
    requires_roles: tuple[str, ...] = ()
    filterable: tuple[str, ...] | Literal["all"] | None = None


class DisableRouteConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """One ``app.rest.disable_routes`` entry.

    Both keys are required and name the route exactly as published: the full
    path, prefix included.

    Unknown keys abort startup rather than being silently dropped — see
    :func:`validate_disable_routes_config` for the diagnostic this produces.

    Args:
        method: HTTP method in uppercase.
        path: Full path of the route to disable, prefix included.
    """

    method: str
    path: str


def _import_reference(interface_name: str, field: str, reference: str) -> object:
    """Resolve a ``module:Symbol`` reference, naming interface and field on failure."""

    try:
        return import_symbol(reference)
    except (ImportError, AttributeError, ValueError) as exc:
        raise RestInterfaceConfigError(
            f"app.rest.interfaces.{interface_name}: {field} {reference!r} does not "
            f"resolve ({exc}). Use the 'module:Symbol' form of an importable name."
        ) from exc


def _resolve_use_case(interface_name: str, reference: str) -> type[UseCase[Any, Any, Any]]:
    symbol = _import_reference(interface_name, "use_case", reference)
    if not (isinstance(symbol, type) and issubclass(symbol, UseCase)):
        raise RestInterfaceConfigError(
            f"app.rest.interfaces.{interface_name}: use_case {reference!r} does not "
            "resolve to a UseCase subclass."
        )
    return symbol


def _resolve_model(interface_name: str, reference: str) -> type[Any]:
    symbol = _import_reference(interface_name, "model", reference)
    if not isinstance(symbol, type):
        raise RestInterfaceConfigError(
            f"app.rest.interfaces.{interface_name}: model {reference!r} does not "
            "resolve to a class."
        )
    return symbol


def _build_route(interface_name: str, route_cfg: RestRouteConfig) -> RestRoute:
    return RestRoute(
        use_case=_resolve_use_case(interface_name, route_cfg.use_case),
        method=route_cfg.method,
        path=route_cfg.path,
        summary=route_cfg.summary,
        description=route_cfg.description,
        status_code=route_cfg.status_code,
        pagination_mode=route_cfg.pagination_mode,
        allow_pagination_override=route_cfg.allow_pagination_override,
        profile_default=route_cfg.profile_default,
        allowed_profiles=route_cfg.allowed_profiles,
        expose_profile=route_cfg.expose_profile,
        requires_roles=route_cfg.requires_roles,
        filterable=_filterable(route_cfg.filterable),
    )


def _filterable(value: tuple[str, ...] | Literal["all"] | None) -> Filterable | None:
    """Map the YAML ``all`` spelling to :data:`~loom.rest.model.ALL`."""
    return ALL if isinstance(value, str) else value


def _class_name(interface_name: str) -> str:
    """Build a readable class name from *interface_name*, always distinct."""
    # Embeds the config key verbatim instead of sanitising it into an
    # identifier-safe form: sanitising would fold 'my_widgets' and
    # 'my-widgets' to the same identifier, and a collision between them
    # would then name the same class twice in the compiler's error, hiding
    # which config entry each side came from. A Python class name has no
    # source-level identifier constraint, so the verbatim key is safe here.
    return f"{interface_name}RestInterface"


def _interface_base(interface_name: str, cfg: RestInterfaceConfig) -> type[RestInterface[Any]]:
    """Return the base ``RestInterface`` requests CRUD generation subclass from.

    Parameterises ``RestInterface[Model]`` when ``model`` is given, exactly as
    a Python author would to enable auto-CRUD. Raises when ``auto`` CRUD is
    requested with no explicit routes and no ``model`` to derive them from —
    the one case configuration cannot express without it.
    """
    wants_generated_routes = cfg.auto and not cfg.routes
    if not cfg.model:
        if wants_generated_routes:
            raise RestInterfaceConfigError(
                f"app.rest.interfaces.{interface_name}: 'auto' CRUD needs a 'model' "
                "reference (module:Symbol) — YAML cannot spell the interface's "
                "generic parameter the way a Python subclass does."
            )
        return RestInterface
    model_type = _resolve_model(interface_name, cfg.model)
    return RestInterface[model_type]  # type: ignore[valid-type]


def _validate_structural(interface_name: str, cfg: RestInterfaceConfig) -> None:
    """Reject the three structural faults ``RestInterfaceCompiler`` would otherwise catch.

    Runs before the dynamic class is generated, so the diagnostic names the
    config entry that actually caused it — the same way :func:`_resolve_model`
    and :func:`_resolve_use_case` already do — instead of the compiler naming
    a generated class nobody wrote by hand, and in the ``routes`` case,
    advising to "declare a RestRoute", which makes no sense for a
    config-declared interface.

    An unknown ``include`` entry is caught here too: left to
    :func:`~loom.rest.autocrud.build_auto_routes`, it is silently dropped
    (a typo removes an endpoint with no warning) instead of aborting startup.
    A ``model`` given without ``auto: true`` is caught for the same reason:
    ``RestInterface.__init_subclass__`` returns before CRUD generation reads
    it, so the reference would otherwise resolve, then be silently ignored.
    """
    if not cfg.prefix:
        raise RestInterfaceConfigError(
            f"app.rest.interfaces.{interface_name}: 'prefix' must be a non-empty string "
            "(e.g. '/users')."
        )
    if not cfg.auto and not cfg.routes:
        raise RestInterfaceConfigError(
            f"app.rest.interfaces.{interface_name}: 'routes' is empty. Declare at least one "
            "route, or set 'auto: true' with a 'model' reference to generate CRUD routes."
        )
    if cfg.model and not cfg.auto:
        raise RestInterfaceConfigError(
            f"app.rest.interfaces.{interface_name}: 'model' is set but 'auto' is not. "
            "'model' only takes effect for auto-CRUD generation — set 'auto: true', "
            "or remove 'model' if this interface only declares explicit 'routes'."
        )
    unknown_ops = [op for op in cfg.include if op not in _VALID_CRUD_OPS]
    if unknown_ops:
        valid = ", ".join(sorted(_VALID_CRUD_OPS))
        raise RestInterfaceConfigError(
            f"app.rest.interfaces.{interface_name}: 'include' has unknown operation(s) "
            f"{unknown_ops!r}. Valid operations are: {valid}."
        )


def _build_interface_class(
    interface_name: str, cfg: RestInterfaceConfig
) -> type[RestInterface[Any]]:
    _validate_structural(interface_name, cfg)
    class_name = _class_name(interface_name)
    namespace: dict[str, Any] = {
        "prefix": cfg.prefix,
        "tags": cfg.tags,
        "auto": cfg.auto,
        "include": cfg.include,
        "routes": tuple(_build_route(interface_name, route) for route in cfg.routes),
        "pagination_mode": cfg.pagination_mode,
        "allow_pagination_override": cfg.allow_pagination_override,
        "profile_default": cfg.profile_default,
        "allowed_profiles": cfg.allowed_profiles,
        "expose_profile": cfg.expose_profile,
        "requires_roles": cfg.requires_roles,
        "filterable": _filterable(cfg.filterable),
        # types.new_class defaults __module__ to this function's caller frame
        # (typically "types" itself, via __set_name__ machinery), which would
        # make every traceback, repr, and log line naming this class point at
        # the standard library instead of the config entry it was built from.
        # Setting it explicitly here is what keeps the "a generated class is
        # a subclass like any other" claim true under introspection too.
        "__module__": __name__,
        "__qualname__": class_name,
        "__doc__": (
            f"RestInterface generated from app.rest.interfaces.{interface_name}. "
            "See loom.rest.config.build_interfaces_from_config."
        ),
    }
    base = _interface_base(interface_name, cfg)
    # types.new_class (not type()) resolves a parameterised generic base
    # (RestInterface[Model]) through __mro_entries__, exactly as
    # 'class X(RestInterface[Model])' does. Runs the same __init_subclass__ a
    # Python subclass runs, including the 'auto' CRUD generation hook — no
    # second compiler, no second code path.
    return types.new_class(class_name, (base,), exec_body=lambda ns: ns.update(namespace))


def build_interfaces_from_config(
    interfaces: Mapping[str, RestInterfaceConfig],
) -> tuple[type[RestInterface[Any]], ...]:
    """Convert ``app.rest.interfaces`` into ``RestInterface`` subclasses.

    Produces the same objects a Python ``RestInterface`` subclass would, in
    the same order the mapping declares them, ready for the same
    :class:`~loom.rest.compiler.RestInterfaceCompiler` that compiles the
    Python-declared interfaces.

    Args:
        interfaces: Decoded ``app.rest.interfaces`` section.

    Returns:
        One dynamically built ``RestInterface`` subclass per entry.

    Raises:
        RestInterfaceConfigError: If a reference does not resolve, an
            ``auto`` interface declares no routes and no ``model``,
            ``include`` names an operation that is not a CRUD operation, or
            ``model`` is set without ``auto``.
    """
    return tuple(_build_interface_class(name, cfg) for name, cfg in interfaces.items())


def _known_field_names(struct_type: type[msgspec.Struct]) -> frozenset[str]:
    return frozenset(field.name for field in msgspec.structs.fields(struct_type))


_INTERFACE_FIELDS = _known_field_names(RestInterfaceConfig)
_ROUTE_FIELDS = _known_field_names(RestRouteConfig)
_DISABLE_ROUTE_FIELDS = _known_field_names(DisableRouteConfig)


def _reject_unknown_keys(target: str, entry: Any, valid: frozenset[str]) -> None:
    """Reject an unknown key in *entry*, after rejecting a non-mapping *entry* itself.

    The type guard matters here specifically: without it, a scalar entry
    (e.g. a route list mistakenly written as a string) iterates its
    characters instead of its keys, and the error names a stray character
    as an "unknown key" — a diagnostic that hides the real mistake in a
    module whose entire purpose is the quality of its diagnostics.
    """
    if not isinstance(entry, Mapping):
        raise RestInterfaceConfigError(
            f"{target} must be a mapping, got {type(entry).__name__}: {entry!r}."
        )
    for key in entry:
        if key not in valid:
            raise RestInterfaceConfigError(
                f"{target}: unknown key {key!r}. Valid keys are: {', '.join(sorted(valid))}."
            )


def validate_interfaces_config(raw: Mapping[str, Any]) -> None:
    """Reject an unknown key anywhere in a raw ``app.rest.interfaces`` mapping.

    Runs on the *raw*, not-yet-decoded mapping so the error can name the
    interface a typo occurred in — information ``msgspec`` does not carry
    for a ``dict`` value, unlike a field on a named struct. Complements the
    ``forbid_unknown_fields=True`` on :class:`RestInterfaceConfig` and
    :class:`RestRouteConfig`, which still reject a typo reached through any
    other conversion path (e.g. a test constructing one directly).

    Each entry is read as ``Any``, not ``Mapping[str, Any]``: the caller
    decodes the section that way on purpose (see
    :func:`~loom.rest.fastapi.auto._section_app_config`) so a malformed
    entry — a string where a mapping belongs — reaches the type guard below
    instead of failing earlier inside ``msgspec`` with a diagnostic that
    does not name the interface.

    Args:
        raw: ``app.rest.interfaces`` as a plain mapping, before conversion
            to :class:`RestInterfaceConfig`.

    Raises:
        RestInterfaceConfigError: If an interface or one of its routes uses
            a key that struct does not declare, or either is not a mapping,
            or ``routes`` is not a list of route entries.
    """
    for name, entry in raw.items():
        _reject_unknown_keys(f"app.rest.interfaces.{name}", entry, _INTERFACE_FIELDS)
        routes = entry.get("routes", ())
        if not isinstance(routes, list | tuple):
            raise RestInterfaceConfigError(
                f"app.rest.interfaces.{name}.routes must be a list of route entries, "
                f"got {type(routes).__name__}: {routes!r}."
            )
        for route in routes:
            _reject_unknown_keys(f"app.rest.interfaces.{name}: route", route, _ROUTE_FIELDS)


def validate_disable_routes_config(raw: Sequence[Any]) -> None:
    """Reject an unknown key anywhere in a raw ``app.rest.disable_routes`` list.

    Each entry is read as ``Any``, not ``Mapping[str, Any]``, for the same
    reason :func:`validate_interfaces_config` does — see its docstring.

    Args:
        raw: ``app.rest.disable_routes`` as a plain sequence, before
            conversion to :class:`DisableRouteConfig`.

    Raises:
        RestInterfaceConfigError: If an entry uses a key
            :class:`DisableRouteConfig` does not declare, or is not a
            mapping.
    """
    for entry in raw:
        _reject_unknown_keys("app.rest.disable_routes", entry, _DISABLE_ROUTE_FIELDS)
