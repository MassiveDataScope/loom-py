"""The project an application composes from: its configuration, environment and code."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import msgspec

from loom.composition import phases
from loom.composition.application import Application
from loom.connections.aws import AwsClients
from loom.connections.resolvers import with_built_in_resolvers
from loom.core.cache.wiring import CacheGateways
from loom.core.config import (
    ConfigContext,
    ConfigError,
    ConfigKey,
    ConfigResolver,
)
from loom.core.config.keys import INTERPOLATION
from loom.core.config.overrides import load_application_config
from loom.core.discovery.scanner import PackageScanner
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.core.logger import get_logger
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.observability import Observability
from loom.core.resources import Resource, ResourceKind, closing
from loom.core.stores import resolve_store_plugin
from loom.core.stores.catalog import StoreCatalog, StoreKey, load_catalog

if TYPE_CHECKING:
    from loom.composition.recipe import Runtime
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.stores import StorePlugin


T = TypeVar("T")


class AppConfig(msgspec.Struct, kw_only=True):
    """The keys of ``app`` every entry point reads; a transport reads its own.

    ``entry_points`` lists entry-point groups: the module of every installed
    entry point in each group is scanned like an ``app.packages`` entry.
    """

    name: str
    entry: str | None = None
    job_service: str | None = None
    code_path: str = DEFAULT_CODE_PATH
    packages: list[str] = msgspec.field(default_factory=list)
    entry_points: list[str] = msgspec.field(default_factory=list)
    agents: list[str] = msgspec.field(default_factory=list)

    @classmethod
    def reject_misdeclared(cls, ctx: ConfigContext) -> None:
        """Fail on the ``app`` rules that hold before any key is resolved.

        Args:
            ctx: The application configuration.

        Raises:
            ConfigError: When ``app.name`` is missing (a ``${...}`` placeholder
                counts as written, none is resolved), the removed ``app.discovery`` is
                declared, or an ``app.entry_points`` group has no installed entry point.
        """
        if ctx.raw(f"{ConfigKey.APP}.name") is None:
            raise ConfigError(
                f"{ConfigKey.APP}.name is required: name the application once in the YAML; "
                "it labels the API, the worker and their metrics"
            )
        if ctx.raw("app.discovery") is not None:
            raise ConfigError("app.discovery is removed; declare app.packages and app.agents")
        declared = ctx.raw(f"{ConfigKey.APP}.entry_points", [])
        if isinstance(declared, list):
            literal_groups = [g for g in declared if isinstance(g, str) and INTERPOLATION not in g]
            PackageScanner(entry_points=literal_groups).package_names()

    @classmethod
    def from_config(cls, ctx: ConfigContext) -> AppConfig:
        """Read the ``app`` section of *ctx*, resolving its placeholders.

        Args:
            ctx: The application configuration.

        Returns:
            The validated keys.

        Raises:
            ConfigError: As :meth:`reject_misdeclared`, or when the section
                does not resolve or fails validation.
        """
        cls.reject_misdeclared(ctx)
        return ctx.section(ConfigKey.APP, cls)


@dataclass(frozen=True, kw_only=True)
class LoadedConfig:
    """The configuration of an application as the files and the environment give it.

    Args:
        config: Loaded configuration, overrides applied.
        environment: Environment resolved once for the whole application.
        config_root: Directory of the first configuration file.
    """

    config: ConfigContext
    environment: Environment
    config_root: Path

    @classmethod
    def from_paths(
        cls,
        config_paths: Sequence[str],
        *,
        config_dir: str | None,
        environment: Environment | str | None,
        resolvers: Sequence[ConfigResolver],
        caller: str,
    ) -> LoadedConfig:
        """Pick the files for the environment and load them, overrides applied.

        Raises:
            ConfigError: When the paths are invalid or the configuration is invalid.
        """
        resolved_environment = resolve_environment(environment)
        paths = select_config_paths(config_paths, config_dir, resolved_environment, caller=caller)
        return cls(
            config=load_application_config(
                *paths,
                resolvers=with_built_in_resolvers(resolvers),
                environment=resolved_environment,
            ),
            environment=resolved_environment,
            config_root=Path(paths[0]).resolve().parent,
        )


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
            was built from; a runtime reads its own keys there.
    """

    config: ConfigContext
    environment: Environment
    code_path: Path
    config_root: Path
    app: AppConfig
    observability: Observability
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
        loaded = LoadedConfig.from_paths(
            config_paths,
            config_dir=config_dir,
            environment=environment,
            resolvers=resolvers,
            caller=caller,
        )
        observability_config = loaded.config.section_or_default(
            ConfigKey.OBSERVABILITY, ObservabilityConfig, ObservabilityConfig()
        )
        observability = Observability.from_config(
            observability_config, environment=loaded.environment
        )
        app_cfg = AppConfig.from_config(loaded.config)
        return cls(
            config=loaded.config,
            environment=loaded.environment,
            code_path=add_code_path(
                code_path if code_path is not None else app_cfg.code_path, loaded.config_root
            ),
            config_root=loaded.config_root,
            app=app_cfg,
            observability=observability,
            observability_config=observability_config,
        )

    @functools.cached_property
    def discovered(self) -> DiscoveryResult:
        """What the scan of ``app.packages`` found, with the ``app.agents`` globs; scanned once.

        Raises:
            ConfigError: When a package cannot be scanned.
        """
        return PackageScanner(self.app.packages, entry_points=self.app.entry_points).discover(
            agents=self.app.agents
        )

    @functools.cached_property
    def catalog(self) -> StoreCatalog:
        """The stores ``stores`` declares, loaded once for the whole composition.

        Raises:
            ConfigError: When the store catalog does not load, a retired
                persistence key or a connection-only ``stores.default`` included.
        """
        return load_catalog(self.config)

    def resolve_store_plugin(self, key: StoreKey, *, default: str) -> StorePlugin:
        """Build the backend ``stores.<key>.backend`` names; *default* without that store.

        Raises:
            ConfigError: As :attr:`catalog`, or when no backend is
                registered under that name.
        """
        spec = self.catalog.specs.get(key)
        return resolve_store_plugin(default) if spec is None else spec.resolve_plugin()

    def compose(
        self,
        runtime: Runtime[object],
    ) -> Application:
        """Compose the application *runtime* asks for, without mounting a transport.

        Opens nothing: building a backend creates its engine or client, not a
        connection. :meth:`Application.check` and :meth:`Application.running`
        are what reach the outside. Logs one ``loom.composition.composed`` info
        event with the milliseconds of each phase.

        Args:
            runtime: Whose recipe to compose.

        Returns:
            The composed, closed application.

        Raises:
            ConfigError: When a package in ``app.packages`` cannot be scanned,
                a model belongs to a store the catalog does not serve, or the
                store section is invalid, or a job transport cannot be built, or two
                resources share a ``kind:name`` or a readiness key.
            JoinedAutocommitStore: When a use case declares ``Joined`` on a store
                that cannot roll back.
            UncomposedWriteStore: When a use case or job writes to a store with
                no unit of work.
            DeployCheckError: When a deploy check of level error fails in a
                deployed environment.
            ReservedBindingError: When an installer binds a framework-owned key.
        """
        logger = get_logger(__name__)
        clock = phases.PhaseClock()
        clock.lap("scan", self.discovered)
        recipe = clock.lap("plan", phases.plan(self, runtime))
        aws = AwsClients()
        stores = clock.lap("build_stores", phases.build_stores(self, recipe, aws))
        installers = phases.build_installers(self.config, recipe.discovered.installers)
        kernel, sql, job_resources = clock.lap(
            "register", phases.register(self, recipe, stores, aws, installers, logger=logger)
        )
        analysis = clock.lap("analysed", recipe.analysed(kernel.factory.graph))
        ai = clock.lap("wire_ai", phases.wire_ai(self, recipe, kernel, analysis, sql))
        phases.seal(kernel, analysis, recipe, catalog=self.catalog, factories=stores.factories)
        clock.lap("seal", None)
        gateways = kernel.container.resolve_or_none(CacheGateways)
        cache = () if gateways is None else gateways.named()
        discovered = recipe.discovered
        roster = phases.ResourceRoster()
        roster.add(
            "loom",
            (
                Resource(
                    kind=ResourceKind.AWS,
                    name="clients",
                    manager=closing(lambda: asyncio.to_thread(aws.close)),
                ),
                *job_resources,
                *(
                    Resource(kind=ResourceKind.CACHE, name=name, manager=closing(gateway.close))
                    for name, gateway in cache
                ),
                *(resource for resource in (sql.resource, ai.resource) if resource is not None),
                *stores.resources,
            ),
        )
        for installer in installers:
            roster.add(type(installer).__qualname__, installer.resources(kernel.container))
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
            resources=roster.resources,
        )
        logger.info(
            "loom.composition.composed",
            runtime=type(runtime).__qualname__,
            use_cases=len(discovered.use_cases),
            jobs=len(discovered.jobs),
            store_plugins=stores.store_plugins,
            keys=len(kernel.container.bindings()),
            installers=len(discovered.installers),
            overrides=len(recipe.overrides),
            agents=len(ai.plans),
            mcp_servers=len(ai.mcp_servers),
            phase_ms=clock.ms,
            duration_ms=clock.total_ms,
        )
        return application

    def serve(
        self,
        runtime: Runtime[T],
    ) -> T:
        """Compose the application *runtime* asks for and mount its transport on it.

        Args:
            runtime: The transport, and how it wants the application composed.

        Returns:
            What *runtime* mounts: an ASGI app, a Celery app, a handler.

        Raises:
            ConfigError: As :meth:`compose`.
            DeployCheckError: As :meth:`compose`.
            ReservedBindingError: As :meth:`compose`.
        """
        return runtime.mount(self.compose(runtime))
