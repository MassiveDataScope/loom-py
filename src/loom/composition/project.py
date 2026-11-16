"""The project an application composes from: its configuration, environment and code."""

from __future__ import annotations

import functools
import importlib
import importlib.util
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import msgspec

from loom.composition import phases
from loom.composition.application import Application
from loom.composition.resources import Resource, ResourceKind, closing
from loom.core.cache.wiring import CacheGateways
from loom.core.checks import DeployReport, run_deploy_checks
from loom.core.config import (
    ConfigContext,
    ConfigError,
    ConfigKey,
    ConfigResolver,
    with_default_resolvers,
)
from loom.core.config.overrides import load_application_config
from loom.core.discovery.config import DiscoveryConfig, discover
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.core.logger import get_logger
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores import resolve_store_backend
from loom.core.stores.catalog import StoreCatalog, StoreKey, load_catalog

if TYPE_CHECKING:
    from loom.composition.recipe import Adapter
    from loom.core.di.container import LoomContainer
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.stores import StoreBackend


T = TypeVar("T")


class AppConfig(msgspec.Struct, kw_only=True):
    """The keys of ``app`` every entry point reads; a transport reads its own."""

    name: str
    entry: str | None = None
    job_service: str | None = None
    code_path: str = DEFAULT_CODE_PATH
    discovery: DiscoveryConfig = msgspec.field(default_factory=DiscoveryConfig)


@dataclass(frozen=True, kw_only=True)
class CheckedProject:
    """A configuration loaded as written, and the deploy checks run on it.

    Args:
        config: Loaded configuration, placeholders left unresolved.
        environment: Environment the overlay was picked for.
        report: Findings of the deploy checks.
        skipped: Check ids or areas left out of the run.
        config_root: Directory of the first configuration file.
        code_path: Application code directory, already on ``sys.path``.
    """

    config: ConfigContext
    environment: Environment
    report: DeployReport
    skipped: tuple[str, ...]
    config_root: Path
    code_path: Path


@dataclass(frozen=True, kw_only=True)
class Project:
    """What an application declares: configuration, environment and code, not yet composed.

    Args:
        config: Loaded configuration, overrides applied.
        environment: Environment resolved once for the whole application.
        code_path: Application code directory, already on ``sys.path``.
        config_root: Directory of the first configuration file.
        app: The ``app`` keys every entry point reads.
        observability: Logging, tracing and metrics, configured before
            anything else can fail.
        observability_config: The ``observability`` section *observability*
            was built from; an adapter reads its own keys there.
    """

    config: ConfigContext
    environment: Environment
    code_path: Path
    config_root: Path
    app: AppConfig
    observability: ObservabilityRuntime
    observability_config: ObservabilityConfig

    @classmethod
    def load(
        cls,
        *config_paths: str,
        config_dir: str | None = None,
        environment: Environment | str | None = None,
        code_path: str | None = None,
        resolvers: Sequence[ConfigResolver] = (),
        caller: str = "Project.load",
    ) -> Project:
        """Load the configuration, environment and code an application composes from.

        Args:
            *config_paths: See :func:`~loom.composition.create_app`.
            config_dir: See :func:`~loom.composition.create_app`.
            environment: See :func:`~loom.composition.create_app`.
            code_path: See :func:`~loom.composition.create_app`.
            resolvers: See :func:`~loom.composition.create_app`.
            caller: Entry point named when the paths are invalid. Loom's own entry
                points pass their name; callers leave the default.

        Returns:
            The loaded project.

        Raises:
            ConfigError: When the configuration is incomplete or invalid.
        """
        resolved_environment = resolve_environment(environment)
        paths = select_config_paths(config_paths, config_dir, resolved_environment, caller=caller)
        ctx = load_application_config(
            *paths, resolvers=with_default_resolvers(resolvers), environment=resolved_environment
        )
        observability_config = ctx.section_or_default(
            ConfigKey.OBSERVABILITY, ObservabilityConfig, ObservabilityConfig()
        )
        observability = ObservabilityRuntime.from_config(
            observability_config, environment=resolved_environment
        )
        if not ctx.has(f"{ConfigKey.APP}.name"):
            raise ConfigError(
                f"{ConfigKey.APP}.name is required: name the application once in the YAML; "
                "it labels the API, the worker and their metrics"
            )
        app_cfg = ctx.section(ConfigKey.APP, AppConfig)
        config_root = Path(paths[0]).resolve().parent
        return cls(
            config=ctx,
            environment=resolved_environment,
            code_path=add_code_path(
                code_path if code_path is not None else app_cfg.code_path, config_root
            ),
            config_root=config_root,
            app=app_cfg,
            observability=observability,
            observability_config=observability_config,
        )

    @classmethod
    def check(
        cls,
        *config_paths: str,
        config_dir: str | None = None,
        environment: Environment | str | None = None,
        skip: tuple[str, ...] = (),
    ) -> CheckedProject:
        """Load the configuration as written and run every deploy check, whatever the environment.

        Composes nothing, connects to nothing, builds no observability, requires
        no ``app.name`` and resolves no placeholder; loom's checks read keys as
        written. ``app.code_path`` joins ``sys.path`` and ``checks.modules``
        import from it. The packages registering loom's own checks are
        imported, the REST ones when the ``rest`` extra is installed.

        Args:
            *config_paths: See :func:`~loom.composition.create_app`.
            config_dir: See :func:`~loom.composition.create_app`.
            environment: See :func:`~loom.composition.create_app`.
            skip: Check ids, or dotted areas such as ``"loom.rest"``, the
                process does not run.

        Returns:
            The loaded configuration with the findings and the silenced ids.

        Raises:
            ConfigError: When the configuration does not load, ``app.code_path``
                is not a literal path, ``lambda`` is one startup would refuse,
                or the ``checks`` section is invalid.
        """
        from loom.serverless.aws.lambda_.checks import check_triggers

        importlib.import_module("loom.core.stores.repository")
        importlib.import_module("loom.core.stores.audit_checks")
        if importlib.util.find_spec("fastapi") is not None:
            importlib.import_module("loom.rest")
        resolved_environment = resolve_environment(environment)
        paths = select_config_paths(
            config_paths, config_dir, resolved_environment, caller="loom check"
        )
        ctx = load_application_config(
            *paths, resolvers=with_default_resolvers(()), environment=resolved_environment
        )
        config_root = Path(paths[0]).resolve().parent
        raw_code_path = ctx.raw(f"{ConfigKey.APP}.code_path", DEFAULT_CODE_PATH)
        if not isinstance(raw_code_path, str) or "${" in raw_code_path:
            raise ConfigError(
                "app.code_path must be a literal path for loom check: a placeholder or a "
                "non-string value cannot name the directory its check modules import from"
            )
        code_path = add_code_path(raw_code_path, config_root)
        check_triggers(ctx)
        return CheckedProject(
            config=ctx,
            environment=resolved_environment,
            report=run_deploy_checks(ctx, skip=skip),
            skipped=skip,
            config_root=config_root,
            code_path=code_path,
        )

    def discover(self) -> DiscoveryResult:
        """Discover the use cases, models, interfaces and agents ``app.discovery`` names."""
        return discover(self.app.discovery)

    @functools.cached_property
    def catalog(self) -> StoreCatalog:
        """The stores ``stores`` declares, loaded once for the whole composition.

        Raises:
            ConfigError: When the store catalog does not load, a retired
                persistence key or a connection-only ``stores.default`` included.
        """
        return load_catalog(self.config)

    def resolve_store_backend(self, key: StoreKey, *, default: str) -> StoreBackend:
        """Build the backend ``stores.<key>.backend`` names; *default* without that store.

        Raises:
            ConfigError: As :attr:`catalog`, or when no backend is
                registered under that name.
        """
        spec = self.catalog.specs.get(key)
        return resolve_store_backend(default if spec is None else spec.backend)

    def compose(
        self,
        adapter: Adapter[object],
        *,
        modules: Sequence[Callable[[LoomContainer], None]] = (),
    ) -> Application:
        """Compose the application *adapter* asks for, without mounting a transport.

        Opens nothing: building a backend creates its engine or client, not a
        connection. :meth:`Application.check` and :meth:`Application.running`
        are what reach the outside. Logs one ``loom.composition.composed`` info
        event with the milliseconds of each phase.

        Args:
            adapter: Whose recipe to compose.
            modules: DI modules run before repositories are registered; see
                :func:`~loom.composition.create_app`.

        Returns:
            The composed, closed application.

        Raises:
            ConfigError: When a model belongs to a store the catalog does not
                serve, or the store section is invalid, or a job backend cannot be built.
            JoinedAutocommitStore: When a use case declares ``Joined`` on a store
                that cannot roll back.
            UncomposedWriteStore: When a use case or job writes to a store with
                no unit of work.
            DeployCheckError: When a deploy check of level error fails in a
                deployed environment.
            ReservedBindingError: When a module binds a framework-owned key.
        """
        logger = get_logger(__name__)
        clock = phases.PhaseClock()
        recipe = clock.lap("plan", phases.plan(self, adapter))
        stores = clock.lap("build_stores", phases.build_stores(self, recipe))
        kernel, sql, job_resources = clock.lap(
            "register", phases.register(self, recipe, stores, modules=modules, logger=logger)
        )
        analysis = clock.lap("analysed", recipe.analysed(kernel.factory.graph))
        ai = clock.lap("wire_ai", phases.wire_ai(self, recipe, kernel, analysis, sql))
        phases.seal(kernel, analysis, recipe, catalog=self.catalog, factories=stores.factories)
        clock.lap("seal", None)
        gateways = kernel.container.resolve_or_none(CacheGateways)
        cache = () if gateways is None else gateways.named()
        discovered = recipe.discovered
        application = Application(
            kernel=kernel,
            config=self.config,
            environment=self.environment,
            code_path=self.code_path,
            discovered=discovered,
            observability=self.observability,
            sql=sql,
            ai=ai,
            job_settings=stores.job_settings,
            resources=(
                *job_resources,
                *(
                    Resource(kind=ResourceKind.CACHE, name=name, manager=closing(gateway.close))
                    for name, gateway in cache
                ),
                *(resource for resource in (sql.resource, ai.resource) if resource is not None),
                *stores.resources,
            ),
        )
        logger.info(
            "loom.composition.composed",
            adapter=type(adapter).__qualname__,
            use_cases=len(discovered.use_cases),
            jobs=len(discovered.jobs),
            backends=stores.store_backends,
            keys=len(kernel.container.bindings()),
            modules=len(modules),
            overrides=len(recipe.overrides),
            agents=len(ai.plans),
            mcp_servers=len(ai.mcp_servers),
            phase_ms=clock.ms,
            duration_ms=clock.total_ms,
        )
        return application

    def serve(
        self,
        adapter: Adapter[T],
        *,
        modules: Sequence[Callable[[LoomContainer], None]] = (),
    ) -> T:
        """Compose the application *adapter* asks for and mount its transport on it.

        Args:
            adapter: The transport, and how it wants the application composed.
            modules: See :meth:`compose`.

        Returns:
            What *adapter* binds: an ASGI app, a Celery app, a handler.

        Raises:
            ConfigError: As :meth:`compose`.
            DeployCheckError: As :meth:`compose`.
            ReservedBindingError: As :meth:`compose`.
        """
        return adapter.bind(self.compose(adapter, modules=modules))
