"""Transport-neutral job and compilable discovery for workers."""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, TypeVar

from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.discovery._utils import collect_from_modules, import_modules
from loom.core.discovery.config import DiscoveryConfig, decode_discovery
from loom.core.discovery.worker_manifest import WorkerManifestAttr
from loom.core.engine.compilable import Compilable
from loom.core.job.config import apply_job_config, read_job_config
from loom.core.job.job import Job
from loom.core.model import BaseModel

_logger = logging.getLogger(__name__)

AutoCRUDUseCases = Callable[[Sequence[type[Any]]], tuple[type[Compilable], ...]]
"""Optional compiler for model-generated use cases."""

_T = TypeVar("_T")

_WORKER_MODES: Final = ("modules", "manifest")
"""The ``app.discovery.mode`` values a worker scans; the first is its default."""


# ---------------------------------------------------------------------------
# Worker manifest — public typed contract for manifest-mode discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerManifest:
    """The components a worker serves, as discovery resolves them.

    A manifest module does not declare an instance of this type (a module-level
    ``MANIFEST`` is refused): it declares the module-level lists ``JOBS``,
    ``USE_CASES``, ``INTERFACES``, ``MODELS`` and ``CALLBACKS``, any of them
    optional but at least one present, and may declare ``AGENTS``, which a
    worker reads only to refuse. Discovery reads them when
    ``app.discovery.mode == "manifest"`` is configured.

    Attributes:
        jobs: Job subclasses to register for execution.
        use_cases: UseCase subclasses to compile in the worker so that
            callbacks can invoke them via ``ApplicationInvoker``.
        interfaces: ``RestInterface`` subclasses whose route use-cases
            are extracted and compiled automatically.
        models: Model classes available to the transport for optional generated use cases.
        callbacks: Job callback classes to register for execution.
        agents: Agent artifact paths or globs the manifest declares, for an
            entry point to refuse: a worker hosts no agent runtime.

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
    agents: Sequence[str] = field(default_factory=list)


@dataclass(frozen=True)
class WorkerComponents:
    """Resolved worker graph after discovery/explicit merging.

    ``agent_specs`` are the agents the manifest declares, which no worker hosts.
    """

    compilables: tuple[type[Compilable], ...]
    jobs: tuple[type[Job[Any]], ...]
    models: tuple[type[BaseModel], ...]
    callbacks: tuple[type[Any], ...]
    agent_specs: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Job config override helper
# ---------------------------------------------------------------------------


def apply_job_overrides(
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
    apply_job_config(job_type, read_job_config(ctx, job_type))


def use_cases_from_interfaces(interfaces: Sequence[type[Any]]) -> tuple[type[Compilable], ...]:
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


def merge_unique(first: Sequence[_T], second: Sequence[_T]) -> tuple[_T, ...]:
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
    agents = tuple(getattr(module, WorkerManifestAttr.AGENTS, ()))

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
        agents=agents,
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


def _build_worker_compilables(
    manifest: WorkerManifest,
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> tuple[type[Compilable], ...]:
    """Build the full compilables set from a :class:`WorkerManifest`.

    Merges use cases from explicit declarations, interface routes, and
    AutoCRUD model generation — then folds in the job classes.

    Args:
        manifest: Typed worker manifest with all component declarations.

    Returns:
        Deduplicated tuple of all compilable types (use cases + jobs).
    """
    use_cases = merge_unique(
        merge_unique(manifest.use_cases, use_cases_from_interfaces(manifest.interfaces)),
        autocrud_use_cases(manifest.models) if autocrud_use_cases else (),
    )
    return merge_unique(use_cases, manifest.jobs)


# ---------------------------------------------------------------------------
# Worker graph construction
# ---------------------------------------------------------------------------


class DuplicateJobName(ConfigError):
    """Two declared jobs have the same wire qualname."""


def _check_duplicate_job_names(jobs: Sequence[type[Job[Any]]]) -> None:
    seen: set[str] = set()
    for job_type in jobs:
        name = job_type.__qualname__
        if name in seen:
            raise DuplicateJobName(f"Duplicate job qualname {name!r}")
        seen.add(name)


def _resolved_from_manifest(
    manifest: WorkerManifest,
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
    """Build a normalized worker graph from a fully populated WorkerManifest."""
    return WorkerComponents(
        compilables=_build_worker_compilables(manifest, autocrud_use_cases),
        jobs=tuple(manifest.jobs),
        models=tuple(manifest.models),
        callbacks=tuple(manifest.callbacks),
        agent_specs=tuple(manifest.agents),
    )


def _discover_from_modules(
    discovery_cfg: DiscoveryConfig,
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
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
        ),
        autocrud_use_cases,
    )


def _discover_from_manifest(
    discovery_cfg: DiscoveryConfig,
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
    """Discover worker components from a manifest module."""
    return _resolved_from_manifest(
        _read_worker_manifest(discovery_cfg.manifest.module), autocrud_use_cases
    )


def _worker_discovery(ctx: ConfigContext) -> DiscoveryConfig:
    """Decode ``app.discovery`` as a worker reads it.

    The mode defaults to ``modules``, and keys the section does not declare
    are ignored.

    Raises:
        ConfigError: When ``app`` is absent, the section fails validation
            or its mode is neither ``modules`` nor ``manifest``.
    """
    app = ctx.section(ConfigKey.APP, dict[str, Any])
    discovery = decode_discovery(
        app.get("discovery", {}), default_mode=_WORKER_MODES[0], strict=False
    )
    if discovery.mode not in _WORKER_MODES:
        raise ConfigError(
            f"app.discovery.mode {discovery.mode!r} is not one a worker scans: "
            f"{' or '.join(_WORKER_MODES)}"
        )
    return discovery


def _discover_compilables_from_config(
    discovery_cfg: DiscoveryConfig,
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
    if discovery_cfg.mode == "modules":
        return _discover_from_modules(discovery_cfg, autocrud_use_cases)
    # _worker_discovery admits no mode but modules and manifest.
    return _discover_from_manifest(discovery_cfg, autocrud_use_cases)


def _try_discover_components(
    ctx: ConfigContext,
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
    """Best-effort discovery; returns empty WorkerComponents if config is absent."""
    try:
        return _discover_compilables_from_config(_worker_discovery(ctx), autocrud_use_cases)
    except (ConfigError, RuntimeError, ValueError, TypeError):
        return WorkerComponents(compilables=(), jobs=(), models=(), callbacks=())


def resolve_worker_components(
    ctx: ConfigContext,
    explicit_jobs: Sequence[type[Job[Any]]],
    explicit_use_cases: Sequence[type[Compilable]],
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
    """Resolve worker components from explicit args and/or discovery config."""
    if explicit_jobs or explicit_use_cases:
        jobs = tuple(explicit_jobs)
        _check_duplicate_job_names(jobs)
        compilables = merge_unique(explicit_use_cases, jobs)
        discovered = _try_discover_components(ctx, autocrud_use_cases)
        declared = set(compilables)
        extra = tuple(c for c in discovered.compilables if c not in declared)
        return WorkerComponents(
            compilables=(*compilables, *extra),
            jobs=jobs,
            models=discovered.models,
            callbacks=discovered.callbacks,
            agent_specs=discovered.agent_specs,
        )

    try:
        discovery_cfg = _worker_discovery(ctx)
    except ConfigError as exc:
        raise RuntimeError(
            "No jobs provided and no app.discovery configured for worker bootstrap."
        ) from exc
    discovered = _discover_compilables_from_config(discovery_cfg, autocrud_use_cases)
    _check_duplicate_job_names(discovered.jobs)
    if not discovered.jobs:
        _logger.warning(
            "no Job classes discovered: the worker starts with no jobs registered. "
            "Add JOBS to manifest or include job modules in app.discovery."
        )
    return discovered
