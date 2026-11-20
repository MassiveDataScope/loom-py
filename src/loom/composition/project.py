"""The project an application composes from: its configuration, environment and code."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import msgspec

from loom.composition import phases
from loom.composition.application import Application
from loom.composition.resources import Resource, ResourceKind, closing
from loom.core.cache.wiring import CacheGateways
from loom.core.checks import CheckScope, DeployReport, run_deploy_checks
from loom.core.config import (
    ConfigContext,
    ConfigError,
    ConfigKey,
    ConfigResolver,
    with_default_resolvers,
)
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import convert_section
from loom.core.config.overrides import load_application_config
from loom.core.connections.aws import AwsClients
from loom.core.discovery.scanner import PackageScanner
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.core.logger import get_logger
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores import resolve_store_backend
from loom.core.stores.catalog import StoreCatalog, StoreKey, load_catalog

if TYPE_CHECKING:
    from loom.composition.recipe import Adapter
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.stores import StoreBackend


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
class CheckedProject:
    """A configuration loaded as written, and the deploy checks run on it.

    Args:
        config: Loaded configuration, placeholders left unresolved.
        environment: Environment the overlay was picked for.
        report: Findings of the deploy checks, and the paths checked and skipped.
        config_root: Directory of the first configuration file.
        code_path: Application code directory, already on ``sys.path``.
    """

    config: ConfigContext
    environment: Environment
    report: DeployReport
    config_root: Path
    code_path: Path

    def unknown_paths(self, paths: Iterable[str]) -> tuple[str, ...]:
        """Return the *paths* no installed check set and no declared configuration key matches."""
        known = {*self.report.checked, *(skipped.path for skipped in self.report.skipped)}
        return tuple(p for p in paths if p not in known and self.config.raw(p) is None)

    @functools.cached_property
    def project(self) -> Project:
        """The :class:`Project` of the configuration as written: nothing resolved, nothing observed.

        Raises:
            ConfigError: When the ``app`` section does not convert.
        """
        return Project(
            config=self.config,
            environment=self.environment,
            code_path=self.code_path,
            config_root=self.config_root,
            app=convert_section(self.config.raw(ConfigKey.APP), ConfigKey.APP, AppConfig),
            observability=ObservabilityRuntime.noop(),
            observability_config=ObservabilityConfig(),
        )

    @property
    def discovered(self) -> DiscoveryResult:
        """What the scan of ``app.packages``, as written, found; the project scans once.

        Raises:
            ConfigError: When a package cannot be scanned.
        """
        return self.project.discovered


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
        app_cfg = AppConfig.from_config(ctx)
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
        excluded: Collection[str] = (),
    ) -> CheckedProject:
        """Load the configuration as written and run every deploy check, whatever the environment.

        Composes nothing, connects to nothing, builds no observability and
        resolves no placeholder; loom's checks read keys as written. It applies
        the structural rules start-up applies: the ``app`` rules of
        :meth:`AppConfig.reject_misdeclared` and the installed backend of every
        declared store. ``app.code_path`` joins ``sys.path`` and ``checks.modules``
        import from it. The check packages installed under the
        ``loom.deploy_checks`` entry-point group run for the paths the
        configuration declares; ``app.rest`` always runs when ``app.entry`` is ``fastapi``.

        Args:
            *config_paths: See :func:`~loom.composition.create_app`.
            config_dir: See :func:`~loom.composition.create_app`.
            environment: See :func:`~loom.composition.create_app`.
            excluded: Configuration paths whose checks the process does not
                run, such as ``"app.rest"`` for a process serving no HTTP.

        Returns:
            The loaded configuration with the findings and the silenced ids.

        Raises:
            ConfigError: When the configuration does not load, breaks an ``app``
                rule, declares a store whose backend is unknown or not
                installed, ``app.code_path`` is not a literal path, or the
                ``checks`` section is invalid.
        """
        resolved_environment = resolve_environment(environment)
        paths = select_config_paths(
            config_paths, config_dir, resolved_environment, caller="loom check"
        )
        ctx = load_application_config(
            *paths, resolvers=with_default_resolvers(()), environment=resolved_environment
        )
        AppConfig.reject_misdeclared(ctx)
        load_catalog(ctx).require_installed()
        config_root = Path(paths[0]).resolve().parent
        raw_code_path = ctx.raw(f"{ConfigKey.APP}.code_path", DEFAULT_CODE_PATH)
        if not isinstance(raw_code_path, str) or INTERPOLATION in raw_code_path:
            raise ConfigError(
                "app.code_path must be a literal path for loom check: a placeholder or a "
                "non-string value cannot name the directory its check modules import from"
            )
        code_path = add_code_path(raw_code_path, config_root)
        scope = CheckScope.declared_by(ctx, excluded=excluded)
        return CheckedProject(
            config=ctx,
            environment=resolved_environment,
            report=run_deploy_checks(ctx, scope=scope),
            config_root=config_root,
            code_path=code_path,
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

    def resolve_store_backend(self, key: StoreKey, *, default: str) -> StoreBackend:
        """Build the backend ``stores.<key>.backend`` names; *default* without that store.

        Raises:
            ConfigError: As :attr:`catalog`, or when no backend is
                registered under that name.
        """
        spec = self.catalog.specs.get(key)
        return resolve_store_backend(default) if spec is None else spec.resolve_backend()

    def compose(
        self,
        adapter: Adapter[object],
    ) -> Application:
        """Compose the application *adapter* asks for, without mounting a transport.

        Opens nothing: building a backend creates its engine or client, not a
        connection. :meth:`Application.check` and :meth:`Application.running`
        are what reach the outside. Logs one ``loom.composition.composed`` info
        event with the milliseconds of each phase.

        Args:
            adapter: Whose recipe to compose.

        Returns:
            The composed, closed application.

        Raises:
            ConfigError: When a package in ``app.packages`` cannot be scanned,
                a model belongs to a store the catalog does not serve, or the
                store section is invalid, or a job backend cannot be built.
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
        recipe = clock.lap("plan", phases.plan(self, adapter))
        aws = AwsClients()
        stores = clock.lap("build_stores", phases.build_stores(self, recipe, aws))
        kernel, sql, job_resources = clock.lap(
            "register", phases.register(self, recipe, stores, aws, logger=logger)
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
        logger.info(
            "loom.composition.composed",
            adapter=type(adapter).__qualname__,
            use_cases=len(discovered.use_cases),
            jobs=len(discovered.jobs),
            backends=stores.store_backends,
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
        adapter: Adapter[T],
    ) -> T:
        """Compose the application *adapter* asks for and mount its transport on it.

        Args:
            adapter: The transport, and how it wants the application composed.

        Returns:
            What *adapter* binds: an ASGI app, a Celery app, a handler.

        Raises:
            ConfigError: As :meth:`compose`.
            DeployCheckError: As :meth:`compose`.
            ReservedBindingError: As :meth:`compose`.
        """
        return adapter.bind(self.compose(adapter))
