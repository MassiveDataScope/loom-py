"""RestInterface compiler.

Validates and compiles :class:`~loom.rest.model.RestInterface` declarations
into :class:`CompiledRoute` records at startup.  No reflection or validation
occurs after compilation.

Compilation steps per interface:

1. Validate the interface: a non-empty ``prefix`` without a placeholder and
   a non-empty ``routes``.
2. For each :class:`~loom.rest.model.RestRoute`:

   - Verify ``method`` and a compiled :class:`~loom.core.engine.plan.ExecutionPlan`.
   - Resolve the result type and the type a ``Created`` result wraps,
     refusing a ``Created`` that does not wrap another member of the result
     or sits on a route whose ``status_code`` is ``201``.
   - Refuse a ``(method, path)`` pair declared twice in the interface.
   - Resolve the pagination mode, the default and allowed profiles
     (route → interface → global), and profile exposure and the required
     roles (route → interface).
   - Read the fields of a ``Query()`` parameter, refusing a non-scalar field,
     a name shared with a path parameter or ``profile``, or a ``QuerySpec``
     parameter next to it.
   - Refuse ``expose_profile`` without ``allowed_profiles``.
"""

from __future__ import annotations

import functools
import inspect
import operator
import types
import typing
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import Any, NamedTuple

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.metrics import MetricsAdapter
from loom.core.engine.plan import ExecutionPlan
from loom.core.model import loom_type
from loom.core.model._json_schema import DEFS, DEFS_PREFIX, REF, definition, without_null
from loom.core.repository.abc.query import QuerySpec
from loom.core.use_case.outcome import Created
from loom.rest.constants import QueryParam
from loom.rest.model import PaginationMode, RestApiDefaults, RestInterface, RestRoute


class _RouteOrigin(Enum):
    """Which pillar declared a route — the label a collision error names.

    Python-declared interfaces are compiled first, so they always win this
    label, never precedence over which route is kept — there is none.
    """

    PYTHON = "a Python interface"
    CONFIG = "an app.rest.interfaces entry"


class _SeenRoute(NamedTuple):
    """A previously compiled route, tracked for collision detection.

    Args:
        origin: Origin that declared this route — used to pick the right
            advice when a later route collides with this one.
        label: Human-readable ``"{origin} ({interface})"`` used in the error.
    """

    origin: _RouteOrigin
    label: str


def _collision_message(
    key: tuple[str, str],
    existing: _SeenRoute,
    new_origin: _RouteOrigin,
    new_label: str,
) -> str:
    """Build a collision error, advising to disable-and-redeclare only when it applies.

    Disabling a Python route and redeclaring it in ``app.rest.interfaces``
    only resolves a collision that involves a Python-declared route on one
    side. A same-origin collision (two Python interfaces, or two YAML
    entries) needs the duplicate removed instead — there is no second
    origin to redeclare into.
    """
    header = f"route ({key[0]}, {key[1]!r}) is declared twice: {existing.label} and {new_label}."
    if existing.origin == new_origin:
        advice = " A route may be declared by only one origin; remove the duplicate declaration."
    else:
        advice = (
            " A route may be declared by only one origin; disable the "
            "Python route and redeclare it in app.rest.interfaces to "
            "change it per environment instead."
        )
    return header + advice


_MAX_LISTED_CANDIDATES = 20


def _format_candidates(routes: Sequence[CompiledRoute]) -> str:
    """Render the ``(method, full_path)`` of every *routes* entry, capped.

    *routes* are the Python-declared routes actually eligible for
    disablement — what an operator fixing a typo needs to see, since the
    entry that failed to match named none of them.
    """
    candidates = sorted({(r.route.method.upper(), r.full_path) for r in routes})
    if not candidates:
        return "no Python-declared route is eligible for disablement."
    shown = candidates[:_MAX_LISTED_CANDIDATES]
    rendered = ", ".join(f"({method}, {path!r})" for method, path in shown)
    omitted = len(candidates) - len(shown)
    suffix = f", and {omitted} more" if omitted else ""
    return f"eligible routes are: {rendered}{suffix}."


def _apply_disablement(
    routes: list[CompiledRoute], disabled: Sequence[tuple[str, str]]
) -> list[CompiledRoute]:
    """Drop every Python-declared route named by *disabled*.

    Only Python-declared routes are eligible: a route declared in
    ``app.rest.interfaces`` is removed by deleting it from the config
    instead, so there is nothing for this mechanism to do there.

    Raises:
        InterfaceCompilationError: If an entry matches no Python-declared
            route.
    """
    if not disabled:
        return routes
    targets = {(method.upper(), path) for method, path in disabled}
    matched: set[tuple[str, str]] = set()
    kept: list[CompiledRoute] = []
    for route in routes:
        key = (route.route.method.upper(), route.full_path)
        if key in targets:
            matched.add(key)
            continue
        kept.append(route)
    unmatched = targets - matched
    if unmatched:
        rendered = ", ".join(f"({method}, {path!r})" for method, path in sorted(unmatched))
        raise InterfaceCompilationError(
            f"app.rest.disable_routes names {rendered}, which "
            "matches no route declared by a Python interface. This only "
            "covers routes declared in code (Python RestInterface "
            "subclasses) — a config-declared route is removed by deleting "
            "it from app.rest.interfaces, and a route mounted outside "
            "compile_sources (e.g. the health check) is never a target. "
            "Name the full path, prefix included, of a Python-declared "
            "route that would otherwise be published — check for a typo "
            "or a route already removed. "
            f"{_format_candidates(routes)}"
        )
    return kept


@dataclass(frozen=True)
class RouteSources:
    """Everything that decides which routes mount, and from which origin.

    Groups the three inputs :meth:`RestInterfaceCompiler.compile_sources` and
    :func:`~loom.rest.fastapi.app.create_fastapi_app` need — the Python
    interfaces, the config-declared ones, and the routes disabled between
    them — into the one value both call sites pass along together.

    Args:
        python: ``RestInterface`` subclasses declared in code. Compiled
            first, deterministically.
        config: ``RestInterface`` subclasses built from
            ``app.rest.interfaces`` (see :mod:`loom.rest.config`). Compiled
            after *python*.
        disabled: ``(method, full_path)`` pairs to drop from the routes
            declared by *python* — see
            :meth:`RestInterfaceCompiler.compile_sources` for why this never
            targets *config*.
    """

    python: Sequence[type[RestInterface[Any]]] = ()
    config: Sequence[type[RestInterface[Any]]] = ()
    disabled: Sequence[tuple[str, str]] = ()

    def __post_init__(self) -> None:
        # A frozen dataclass only stops rebinding the field itself; a caller
        # passing a plain list still owns that list and can mutate it after
        # construction, silently changing what this "frozen" value compiles.
        # object.__setattr__ is the documented way to write to a frozen
        # instance's own __init__/__post_init__.
        object.__setattr__(self, "python", tuple(self.python))
        object.__setattr__(self, "config", tuple(self.config))
        object.__setattr__(self, "disabled", tuple(self.disabled))


class InterfaceCompilationError(Exception):
    """Raised when a RestInterface fails structural validation at startup.

    Args:
        message: Human-readable description of the compilation failure.

    Example::

        raise InterfaceCompilationError(
            "UserRestInterface: duplicate route (GET, /{user_id})"
        )
    """


def _execute_hints(use_case_type: type[Any]) -> dict[str, Any]:
    try:
        return typing.get_type_hints(use_case_type.execute)
    except Exception:
        return {}


def _split_created(use_case_type: type[Any]) -> tuple[Any, Any]:
    """Split the result type of ``execute`` around its ``Created`` member.

    Args:
        use_case_type: Use case whose ``execute`` return annotation is read.

    Returns:
        The result type without ``Created``, answered with the route's
        status, and the type ``Created`` wraps, answered with ``201``; that
        second item is ``None`` when the result names no ``Created``.

    Raises:
        InterfaceCompilationError: ``Created`` appears more than once, is not
            parameterised, or wraps a type that is not another member of the
            result union.
    """
    result_type = _execute_hints(use_case_type).get("return", Any)
    is_union = typing.get_origin(result_type) in (typing.Union, types.UnionType)
    members = typing.get_args(result_type) if is_union else (result_type,)
    created = [m for m in members if m is Created or typing.get_origin(m) is Created]
    if not created:
        return result_type, None
    others = tuple(m for m in members if m not in created)
    wrapped = typing.get_args(created[0])
    if len(created) > 1 or not wrapped or wrapped[0] not in others:
        raise InterfaceCompilationError(
            f"{use_case_type.__qualname__}.execute returns {result_type!r}: a use case "
            "that may create declares `T | Created[T]`, with one Created wrapping "
            "one of the other members of the result."
        )
    return functools.reduce(operator.or_, others), wrapped[0]


_SCALAR_TYPES = frozenset({"string", "integer", "number", "boolean"})


@dataclass(frozen=True)
class QueryField:
    """One query parameter a ``Query()`` route accepts.

    Args:
        name: Wire name of the parameter.
        required: ``True`` when the field has no default.
        schema: JSON Schema of the value without its ``null`` branch. An enum
            is a ``$ref`` into the ``$defs`` the schema carries.
    """

    name: str
    required: bool
    schema: Mapping[str, Any]


def _declared_query_fields(query_type: type[Any]) -> tuple[QueryField, ...]:
    """Read the query parameters a ``Query()`` type declares, in declaration order.

    Raises:
        ValueError: A field is not a scalar a query parameter can carry.
    """
    document = loom_type(query_type).schema()
    definitions = document.get(DEFS, {})
    root = definition(document, definitions)
    required = set(root.get("required", ()))
    return tuple(
        _query_field(name, prop, name in required, definitions)
        for name, prop in root.get("properties", {}).items()
    )


def _query_field(
    name: str, prop: Mapping[str, Any], required: bool, definitions: Mapping[str, Any]
) -> QueryField:
    schema = without_null(prop)
    target = definition(schema, definitions)
    if "enum" not in target and target.get("type") not in _SCALAR_TYPES:
        raise ValueError(
            f"query field {name!r} must be a string (UUID, date-time and other string "
            "formats included), integer, number, boolean, enum or Literal, optionally None."
        )
    ref = schema.get(REF)
    if ref is not None:
        schema = {**schema, DEFS: {ref.removeprefix(DEFS_PREFIX): target}}
    return QueryField(name=name, required=required, schema=schema)


def names_query_spec(annotation: Any) -> bool:
    """Whether *annotation* is ``QuerySpec`` or a union holding it."""
    is_union = typing.get_origin(annotation) in (typing.Union, types.UnionType)
    return annotation is QuerySpec or (is_union and QuerySpec in typing.get_args(annotation))


@dataclass(frozen=True)
class CompiledRoute:
    """Fully resolved route ready for transport binding.

    Produced by :class:`RestInterfaceCompiler` for each valid route declaration.
    All policy fields are resolved — no further lookup is needed at request time.

    Args:
        interface_name: Qualified name of the originating ``RestInterface``.
        route: Original :class:`~loom.rest.model.RestRoute` declaration.
        full_path: Absolute HTTP path (``interface.prefix + route.path``).
        effective_pagination_mode: Resolved pagination strategy after applying
            route → interface → global precedence.
        effective_profile_default: Resolved default profile name.
        effective_allowed_profiles: Resolved set of allowed profiles.
        effective_expose_profile: Whether ``?profile=...`` is publicly
            accepted for this route.
        effective_allow_pagination_override: Whether callers may override
            pagination mode via query parameters for this route.
        effective_requires_roles: Resolved roles granting access to this
            route, after applying route → interface precedence.  Empty means
            the route declares no role requirement.
        effective_max_limit: Ceiling applied to the ``?limit=`` query
            parameter of this route.
        query_fields: Parameters the route's ``Query()`` declares; empty
            when the use case declares none.
        result_type: Result type of ``execute`` without its ``Created``
            member, answered with the route's ``status_code``.
        created_type: Type a ``Created`` result wraps, answered with ``201``;
            ``None`` when the use case never returns ``Created``.
        read_only: ``True`` for GET routes — instructs the executor to skip
            the ``UnitOfWork`` transaction for this request.
        interface_tags: OpenAPI tags inherited from the parent ``RestInterface``.
    """

    interface_name: str
    route: RestRoute
    full_path: str
    effective_pagination_mode: PaginationMode
    effective_profile_default: str
    effective_allowed_profiles: tuple[str, ...]
    effective_expose_profile: bool
    effective_allow_pagination_override: bool
    read_only: bool = False
    interface_tags: tuple[str, ...] = ()
    execute_param_types: tuple[tuple[str, Any], ...] = ()
    effective_requires_roles: tuple[str, ...] = ()
    effective_max_limit: int = 1000
    query_fields: tuple[QueryField, ...] = ()
    result_type: Any = Any
    created_type: Any = None


class RestInterfaceCompiler:
    """Compiles RestInterface declarations into CompiledRoute records.

    Validates structure, resolves policies, and caches the result.  Designed
    to run once at startup, driven by :func:`~loom.core.bootstrap.bootstrap.bootstrap_app`
    or the FastAPI composition root.

    Args:
        use_case_compiler: Compiler that holds the cached
            :class:`~loom.core.engine.plan.ExecutionPlan` registry.
        defaults: Global REST API defaults applied when interface/route
            level is unset.  Defaults to :class:`~loom.rest.model.RestApiDefaults`
            with offset pagination.
        metrics: Optional metrics adapter.  Reserved for future use
            (HTTP metrics are emitted at request time by the router runtime).

    Example::

        compiler = RestInterfaceCompiler(use_case_compiler)
        routes = compiler.compile(UserRestInterface)
    """

    def __init__(
        self,
        use_case_compiler: UseCaseCompiler,
        defaults: RestApiDefaults | None = None,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._uc_compiler = use_case_compiler
        self._defaults = defaults or RestApiDefaults()
        self._metrics = metrics
        self._cache: dict[type[RestInterface[Any]], list[CompiledRoute]] = {}

    def compile(self, interface: type[RestInterface[Any]]) -> list[CompiledRoute]:
        """Compile ``interface`` and return the list of :class:`CompiledRoute`.

        Compilation is idempotent: repeated calls with the same class return
        the cached result.

        Args:
            interface: ``RestInterface`` subclass to compile.

        Returns:
            Ordered list of compiled routes ready for transport binding.

        Raises:
            InterfaceCompilationError: If the interface fails validation.
        """
        if interface in self._cache:
            return self._cache[interface]

        result = self._compile_fresh(interface)
        self._cache[interface] = result
        return result

    def compile_sources(self, sources: RouteSources) -> list[CompiledRoute]:
        """Compile interfaces from both origins into one deterministic route set.

        Python-declared interfaces compile first. ``sources.disabled`` is
        then applied to that Python-only set — see :func:`_apply_disablement`
        for why it never targets config-declared routes. Only after
        disablement do ``app.rest.interfaces`` entries compile and join the
        result. A ``(method, path)`` collision between what remains aborts
        naming both origins involved — order only decides which origin the
        message names first, it grants neither side precedence.

        This sequencing is what makes overriding a route per environment
        possible: disabling a Python route removes it, and only then does the
        collision check run, so the config redeclaration mounts cleanly
        instead of colliding with a route that would otherwise still be
        considered occupied.

        Args:
            sources: Interfaces to compile, grouped by origin, plus the
                Python routes to drop before the merge.

        Returns:
            Compiled routes, Python-declared ones first (minus any disabled),
            followed by the config-declared ones.

        Raises:
            InterfaceCompilationError: On a same- or cross-origin collision,
                or a ``disabled`` entry matching no Python-declared route.
        """
        seen: dict[tuple[str, str], _SeenRoute] = {}

        python_routes = _apply_disablement(self._compile_many(sources.python), sources.disabled)
        self._track_routes(python_routes, _RouteOrigin.PYTHON, seen)

        config_routes = self._compile_many(sources.config)
        self._track_routes(config_routes, _RouteOrigin.CONFIG, seen)

        return [*python_routes, *config_routes]

    def _compile_many(self, interfaces: Sequence[type[RestInterface[Any]]]) -> list[CompiledRoute]:
        """Compile every interface in declaration order, without collision tracking."""
        compiled: list[CompiledRoute] = []
        for interface in interfaces:
            compiled.extend(self.compile(interface))
        return compiled

    def _track_routes(
        self,
        routes: Sequence[CompiledRoute],
        origin: _RouteOrigin,
        seen: dict[tuple[str, str], _SeenRoute],
    ) -> None:
        """Record each route's key against *seen*, raising on a collision.

        *seen* is mutated in place. Runs against the routes that actually
        survive to the merged result, so a disabled-then-dropped Python route
        is never registered — a later config redeclaration of the same key
        finds nothing occupying it.
        """
        for route in routes:
            key = (route.route.method.upper(), route.full_path)
            new_label = f"{origin.value} ({route.interface_name})"
            if key in seen:
                raise InterfaceCompilationError(
                    _collision_message(key, seen[key], origin, new_label)
                )
            seen[key] = _SeenRoute(origin, new_label)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _compile_fresh(self, interface: type[RestInterface[Any]]) -> list[CompiledRoute]:
        iface_name = interface.__qualname__
        prefix = interface.prefix.rstrip("/")

        self._validate_interface(interface)

        seen: dict[tuple[str, str], RestRoute] = {}
        compiled: list[CompiledRoute] = []

        for route in interface.routes:
            plan, result_type, created_type = self._validate_route(iface_name, route)

            key = (route.method.upper(), route.path)
            if key in seen:
                raise InterfaceCompilationError(
                    f"{iface_name}: duplicate route "
                    f"({route.method.upper()}, {route.path!r}).  "
                    "Each (method, path) pair must be unique within a RestInterface."
                )
            seen[key] = route

            full_path = f"{prefix}{route.path}" if route.path else prefix or "/"

            compiled.append(
                CompiledRoute(
                    interface_name=iface_name,
                    route=route,
                    full_path=full_path,
                    effective_pagination_mode=self._resolve_pagination(route, interface),
                    effective_profile_default=self._resolve_profile_default(route, interface),
                    effective_allowed_profiles=self._resolve_allowed_profiles(route, interface),
                    effective_expose_profile=self._resolve_expose_profile(route, interface),
                    effective_allow_pagination_override=self._resolve_allow_pagination_override(
                        route, interface
                    ),
                    read_only=route.method.upper() == "GET",
                    interface_tags=interface.tags,
                    execute_param_types=self._resolve_execute_param_types(route.use_case),
                    effective_requires_roles=self._resolve_requires_roles(route, interface),
                    effective_max_limit=self._defaults.max_limit,
                    query_fields=self._resolve_query_fields(iface_name, route, plan),
                    result_type=result_type,
                    created_type=created_type,
                )
            )

            effective = compiled[-1]
            if effective.effective_expose_profile and not effective.effective_allowed_profiles:
                raise InterfaceCompilationError(
                    f"{iface_name}: route ({route.method.upper()}, {route.path!r}) "
                    "has profile exposure enabled but no allowed profiles. "
                    "Declare at least one allowed profile."
                )

        return compiled

    def _validate_interface(self, interface: type[RestInterface[Any]]) -> None:
        iface_name = interface.__qualname__
        if not interface.prefix:
            raise InterfaceCompilationError(
                f"{iface_name}: 'prefix' must be a non-empty string (e.g. '/users')."
            )
        if "{" in interface.prefix:
            raise InterfaceCompilationError(
                f"{iface_name}: 'prefix' {interface.prefix!r} contains a path "
                "placeholder.  Placeholders in 'prefix' are not bound to the use case; "
                "put the placeholder in the route 'path' instead."
            )
        if not interface.routes:
            raise InterfaceCompilationError(
                f"{iface_name}: 'routes' is empty.  Declare at least one RestRoute to expose."
            )

    def _validate_route(self, iface_name: str, route: RestRoute) -> tuple[ExecutionPlan, Any, Any]:
        if not route.method:
            raise InterfaceCompilationError(
                f"{iface_name}: RestRoute for {route.use_case.__qualname__!r} is missing 'method'."
            )
        plan = self._uc_compiler.get_plan(route.use_case)
        if plan is None:
            raise InterfaceCompilationError(
                f"{iface_name}: route ({route.method.upper()}, {route.path!r}) "
                f"references {route.use_case.__qualname__!r} which has not been "
                "compiled.  Pass it to bootstrap_app(use_cases=[...]) first."
            )
        result_type, created_type = _split_created(route.use_case)
        if created_type is not None and route.status_code == HTTPStatus.CREATED:
            raise InterfaceCompilationError(
                f"{iface_name}: route ({route.method.upper()}, {route.path!r}) has "
                "status_code 201 and its use case returns Created, which answers 201 "
                "itself; declare the status of the other result instead."
            )
        return plan, result_type, created_type

    @staticmethod
    def _resolve_query_fields(
        iface_name: str, route: RestRoute, plan: ExecutionPlan
    ) -> tuple[QueryField, ...]:
        binding = plan.query_binding
        if binding is None:
            return ()
        where = f"{iface_name}: route ({route.method.upper()}, {route.path!r})"
        if any(names_query_spec(pb.annotation) for pb in plan.param_bindings):
            raise InterfaceCompilationError(
                f"{where}: Query() cannot be combined with a QuerySpec parameter."
            )
        try:
            fields = _declared_query_fields(binding.query_type)
        except ValueError as exc:
            raise InterfaceCompilationError(f"{where}: {exc}") from exc
        clashes = [
            f.name for f in fields if f.name == QueryParam.PROFILE or f"{{{f.name}}}" in route.path
        ]
        if clashes:
            raise InterfaceCompilationError(
                f"{where}: query field {clashes[0]!r} has the name of a path parameter "
                "or of 'profile'."
            )
        return fields

    def _resolve_pagination(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> PaginationMode:
        return route.pagination_mode or interface.pagination_mode or self._defaults.pagination_mode

    def _resolve_profile_default(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> str:
        return route.profile_default or interface.profile_default or self._defaults.profile_default

    def _resolve_allowed_profiles(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> tuple[str, ...]:
        if route.allowed_profiles:
            return route.allowed_profiles
        if interface.allowed_profiles:
            return interface.allowed_profiles
        return self._defaults.allowed_profiles

    def _resolve_requires_roles(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> tuple[str, ...]:
        if route.requires_roles:
            return route.requires_roles
        return interface.requires_roles

    def _resolve_expose_profile(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> bool:
        if route.expose_profile:
            return True
        return interface.expose_profile

    def _resolve_allow_pagination_override(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> bool:
        if route.allow_pagination_override is not None:
            return route.allow_pagination_override
        if interface.allow_pagination_override is not None:
            return interface.allow_pagination_override
        return self._defaults.allow_pagination_override

    def _resolve_execute_param_types(
        self,
        use_case_type: type[Any],
    ) -> tuple[tuple[str, Any], ...]:
        signature = inspect.signature(use_case_type.execute)
        hints = _execute_hints(use_case_type)

        result: list[tuple[str, Any]] = []
        variadic = (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        for name, param in signature.parameters.items():
            if name == "self" or param.kind in variadic:
                continue
            annotation = hints.get(name, param.annotation)
            result.append((name, annotation))
        return tuple(result)
