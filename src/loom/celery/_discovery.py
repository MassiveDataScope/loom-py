"""What a Celery worker serves: jobs, use cases, models and callbacks.

A worker reads ``app.discovery`` (``modules`` or ``manifest``) on top of the
jobs and use cases its entry point passes explicitly.  The result feeds
:meth:`loom.celery.adapter.CeleryAdapter.recipe`.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar

import msgspec

from loom.celery.config import JobConfig, apply_job_config
from loom.celery.constants import WorkerManifestAttr
from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.discovery._utils import collect_from_modules, import_modules
from loom.core.engine.compilable import Compilable
from loom.core.job.job import Job
from loom.core.model import BaseModel
from loom.rest.autocrud import build_auto_routes

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# ---------------------------------------------------------------------------
# Local config structs — private to this module
# ---------------------------------------------------------------------------


class _DiscoveryModules(msgspec.Struct, kw_only=True):
    include: list[str] = msgspec.field(default_factory=list)


class _DiscoveryManifest(msgspec.Struct, kw_only=True):
    module: str = ""


class _DiscoveryConfig(msgspec.Struct, kw_only=True):
    mode: Literal["modules", "manifest"] = "modules"
    modules: _DiscoveryModules = msgspec.field(default_factory=_DiscoveryModules)
    manifest: _DiscoveryManifest = msgspec.field(default_factory=_DiscoveryManifest)


class _WorkerAppConfig(msgspec.Struct, kw_only=True):
    discovery: _DiscoveryConfig = msgspec.field(default_factory=_DiscoveryConfig)


# ---------------------------------------------------------------------------
# Worker manifest — public typed contract for manifest-mode discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerManifest:
    """The components a worker serves, as discovery resolves them.

    A manifest module does not declare an instance of this type (a module-level
    ``MANIFEST`` is refused): it declares the module-level lists ``JOBS``,
    ``USE_CASES``, ``INTERFACES``, ``MODELS`` and ``CALLBACKS``, any of them
    optional but at least one present. :func:`bootstrap_worker` reads them when
    ``app.discovery.mode == "manifest"`` is configured in YAML.

    Attributes:
        jobs: Job subclasses to register as Celery tasks.
        use_cases: UseCase subclasses to compile in the worker so that
            callbacks can invoke them via ``ApplicationInvoker``.
        interfaces: ``RestInterface`` subclasses whose route use-cases
            are extracted and compiled automatically.
        models: Model classes for which AutoCRUD use cases are compiled.
        callbacks: Job callback classes to register as Celery tasks.

    Example::

        # app/worker_manifest.py
        from app.product.jobs import SendRestockEmailJob
        from app.product.use_cases import GetProductUseCase
        from app.product.model import Product

        JOBS = [SendRestockEmailJob]
        USE_CASES = [GetProductUseCase]
        MODELS = [Product]
    """

    jobs: Sequence[type[Job[Any]]] = field(default_factory=list)
    use_cases: Sequence[type[Compilable]] = field(default_factory=list)
    interfaces: Sequence[type[Any]] = field(default_factory=list)
    models: Sequence[type[Any]] = field(default_factory=list)
    callbacks: Sequence[type[Any]] = field(default_factory=list)


@dataclass(frozen=True)
class _WorkerResolved:
    """Resolved worker graph after discovery/explicit merging."""

    compilables: tuple[type[Compilable], ...]
    jobs: tuple[type[Job[Any]], ...]
    models: tuple[type[BaseModel], ...]
    callbacks: tuple[type[Any], ...]


# ---------------------------------------------------------------------------
# Job config override helper
# ---------------------------------------------------------------------------


def _apply_job_config_if_present(
    ctx: ConfigContext,
    job_type: type[Job[Any]],
) -> None:
    """Apply ``JobConfig`` overrides from YAML to ``job_type``'s ClassVars.

    Silently skips when the ``jobs.<JobTypeName>`` section is absent — the
    Job's ClassVar defaults remain unchanged.

    Args:
        ctx: Resolved configuration context.
        job_type: Concrete ``Job`` subclass to configure.
    """
    try:
        cfg = ctx.section(f"{ConfigKey.JOBS}.{job_type.__name__}", JobConfig)
        apply_job_config(job_type, cfg)
    except ConfigError:
        pass  # no override for this job — use ClassVar defaults


def _use_cases_from_interfaces(interfaces: Sequence[type[Any]]) -> tuple[type[Compilable], ...]:
    """Extract UseCase classes declared on RestInterface routes.

    Each ``RestInterface.routes`` entry carries a ``use_case`` class.
    This helper flattens all routes across the given interfaces and
    returns the unique use-case types so they can be compiled in the
    worker process alongside explicit jobs.

    Args:
        interfaces: Sequence of ``RestInterface`` subclasses.

    Returns:
        Tuple of UseCase classes found across all route declarations.
    """
    return tuple(
        route.use_case
        for iface in interfaces
        for route in getattr(iface, "routes", ())
        if getattr(route, "use_case", None) is not None
    )


# ---------------------------------------------------------------------------
# Sequence utilities
# ---------------------------------------------------------------------------


def _merge_unique(first: Sequence[_T], second: Sequence[_T]) -> tuple[_T, ...]:
    """Merge two sequences preserving declaration order and uniqueness."""
    seen: set[Any] = set()
    result: list[_T] = []
    for item in (*first, *second):
        if item not in seen:
            result.append(item)
            seen.add(item)
    return tuple(result)


# ---------------------------------------------------------------------------
# Callback discovery
# ---------------------------------------------------------------------------


def _is_callback_class(value: type[Any]) -> bool:
    """Return ``True`` when *value* looks like a job callback class."""
    if value.__name__ == "NullJobCallback":
        return False
    on_success = getattr(value, "on_success", None)
    on_failure = getattr(value, "on_failure", None)
    return callable(on_success) and callable(on_failure)


def _discover_callbacks_from_modules(modules: list[Any]) -> tuple[type[Any], ...]:
    """Collect local callback classes from imported modules."""
    flat: list[type[Any]] = [
        cls
        for module in modules
        for _, cls in inspect.getmembers(module, inspect.isclass)
        if cls.__module__ == module.__name__ and _is_callback_class(cls)
    ]
    return tuple(dict.fromkeys(flat))


# ---------------------------------------------------------------------------
# Manifest reading
# ---------------------------------------------------------------------------


def _reject_private_manifest(module: Any, module_path: str) -> None:
    """Raise if the module exposes a private MANIFEST struct.

    The old ``MANIFEST = WorkerManifest(...)`` pattern is no longer part
    of the public API.  Components must be declared via the public list
    attributes (MODELS / USE_CASES / INTERFACES / JOBS / CALLBACKS).
    """
    if getattr(module, WorkerManifestAttr.MANIFEST, None) is None:
        return
    raise ValueError(
        f"{module_path!r}.{WorkerManifestAttr.MANIFEST} is not part of the public manifest API. "
        f"Declare components via {WorkerManifestAttr.MODELS}/{WorkerManifestAttr.USE_CASES}/"
        f"{WorkerManifestAttr.INTERFACES}/{WorkerManifestAttr.JOBS}/"
        f"{WorkerManifestAttr.CALLBACKS}."
    )


def _read_manifest_lists(module: Any, module_path: str) -> WorkerManifest:
    """Read manifest using public list attributes (MODELS/USE_CASES/JOBS/...)."""
    models = tuple(getattr(module, WorkerManifestAttr.MODELS, ()))
    use_cases = tuple(getattr(module, WorkerManifestAttr.USE_CASES, ()))
    interfaces = tuple(getattr(module, WorkerManifestAttr.INTERFACES, ()))
    jobs = tuple(getattr(module, WorkerManifestAttr.JOBS, ()))
    callbacks = tuple(getattr(module, WorkerManifestAttr.CALLBACKS, ()))

    if not (models or use_cases or interfaces or jobs or callbacks):
        expected = (
            f"{WorkerManifestAttr.MODELS}/{WorkerManifestAttr.USE_CASES}/"
            f"{WorkerManifestAttr.INTERFACES}/{WorkerManifestAttr.JOBS}/"
            f"{WorkerManifestAttr.CALLBACKS}"
        )
        raise ValueError(
            f"Manifest module {module_path!r} exposes no components. Expected {expected}."
        )

    return WorkerManifest(
        models=models,
        use_cases=use_cases,
        interfaces=interfaces,
        jobs=jobs,
        callbacks=callbacks,
    )


def _read_worker_manifest(module_path: str) -> WorkerManifest:
    """Read a :class:`WorkerManifest` from a manifest module.

    Args:
        module_path: Dotted Python import path of the manifest module.

    Returns:
        A normalized :class:`WorkerManifest`.
    """
    module = importlib.import_module(module_path)
    _reject_private_manifest(module, module_path)
    return _read_manifest_lists(module, module_path)


# ---------------------------------------------------------------------------
# AutoCRUD and compilables helpers
# ---------------------------------------------------------------------------


def _autocrud_use_cases_from_models(
    models: Sequence[type[Any]],
) -> tuple[type[Compilable], ...]:
    """Extract AutoCRUD use-case classes for the given model types.

    Calls ``build_auto_routes(model, ())`` for each model to generate
    all five standard CRUD routes and returns their use-case classes.

    Args:
        models: Model classes to extract AutoCRUD use cases from.

    Returns:
        Unique use-case types from all generated routes.
    """
    flat: list[type[Compilable]] = [
        route.use_case for model in models for route in build_auto_routes(model, ())
    ]
    return tuple(dict.fromkeys(flat))


def _build_worker_compilables(manifest: WorkerManifest) -> tuple[type[Compilable], ...]:
    """Build the full compilables set from a :class:`WorkerManifest`.

    Merges use cases from explicit declarations, interface routes, and
    AutoCRUD model generation — then folds in the job classes.

    Args:
        manifest: Typed worker manifest with all component declarations.

    Returns:
        Deduplicated tuple of all compilable types (use cases + jobs).
    """
    use_cases = _merge_unique(
        _merge_unique(manifest.use_cases, _use_cases_from_interfaces(manifest.interfaces)),
        _autocrud_use_cases_from_models(manifest.models),
    )
    return _merge_unique(use_cases, manifest.jobs)


# ---------------------------------------------------------------------------
# Worker graph construction
# ---------------------------------------------------------------------------


def _resolved_from_manifest(manifest: WorkerManifest) -> _WorkerResolved:
    """Build a normalized worker graph from a fully populated WorkerManifest."""
    return _WorkerResolved(
        compilables=_build_worker_compilables(manifest),
        jobs=tuple(manifest.jobs),
        models=tuple(manifest.models),
        callbacks=tuple(manifest.callbacks),
    )


def _discover_from_modules(discovery_cfg: _DiscoveryConfig) -> _WorkerResolved:
    """Discover worker components from module include paths."""
    modules = import_modules(discovery_cfg.modules.include)
    models, use_cases, interfaces, discovered_jobs = collect_from_modules(modules)
    callbacks = _discover_callbacks_from_modules(modules)
    return _resolved_from_manifest(
        WorkerManifest(
            models=tuple(models),
            use_cases=tuple(use_cases),
            interfaces=tuple(interfaces),
            jobs=tuple(discovered_jobs),
            callbacks=callbacks,
        )
    )


def _discover_from_manifest(discovery_cfg: _DiscoveryConfig) -> _WorkerResolved:
    """Discover worker components from a manifest module."""
    return _resolved_from_manifest(_read_worker_manifest(discovery_cfg.manifest.module))


def _discover_compilables_from_config(
    discovery_cfg: _DiscoveryConfig,
) -> _WorkerResolved:
    if discovery_cfg.mode == "modules":
        return _discover_from_modules(discovery_cfg)
    # mode == "manifest" — Literal type guarantees no other value is possible
    return _discover_from_manifest(discovery_cfg)


def _try_discover_components(ctx: ConfigContext) -> _WorkerResolved:
    """Best-effort discovery; returns empty _WorkerResolved if config is absent."""
    try:
        app_cfg = ctx.section(ConfigKey.APP, _WorkerAppConfig)
        return _discover_compilables_from_config(app_cfg.discovery)
    except (ConfigError, RuntimeError, ValueError, TypeError):
        return _WorkerResolved(compilables=(), jobs=(), models=(), callbacks=())


def _resolve_compilables_and_jobs(
    ctx: ConfigContext,
    explicit_jobs: Sequence[type[Job[Any]]],
    explicit_use_cases: Sequence[type[Compilable]],
) -> _WorkerResolved:
    """Resolve worker components from explicit args and/or discovery config."""
    if explicit_jobs or explicit_use_cases:
        jobs = tuple(explicit_jobs)
        compilables = _merge_unique(explicit_use_cases, jobs)
        discovered = _try_discover_components(ctx)
        declared = set(compilables)
        extra = tuple(c for c in discovered.compilables if c not in declared)
        return _WorkerResolved(
            compilables=(*compilables, *extra),
            jobs=jobs,
            models=discovered.models,
            callbacks=discovered.callbacks,
        )

    try:
        app_cfg = ctx.section(ConfigKey.APP, _WorkerAppConfig)
    except ConfigError as exc:
        raise RuntimeError(
            "No jobs provided and no app.discovery configured for worker bootstrap."
        ) from exc
    discovered = _discover_compilables_from_config(app_cfg.discovery)
    if not discovered.jobs:
        _logger.warning(
            "no Job classes discovered: the worker starts with no task registered. "
            "Add JOBS to manifest or include job modules in app.discovery."
        )
    return discovered


def _emit_worker_init_graph(components: _WorkerResolved) -> None:
    """Emit a concise worker graph summary (Celery-style init visibility)."""
    from loom.core.logger import get_logger

    logger = get_logger("loom.celery.bootstrap")
    logger.info(
        "[BOOT] Worker init graph",
        jobs=len(components.jobs),
        use_cases=len(components.compilables) - len(components.jobs),
        callbacks=len(components.callbacks),
        models=len(components.models),
    )
