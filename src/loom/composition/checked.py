"""``loom check``: the configuration as written, and the deploy checks run on it."""

from __future__ import annotations

import functools
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from pathlib import Path

from loom.composition import phases
from loom.composition.project import AppConfig, LoadedConfig, Project
from loom.core.checks import CheckScope, DeployReport, run_deploy_checks
from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import convert_section
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.observability import ObservabilityRuntime
from loom.core.stores.catalog import load_catalog


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

    @classmethod
    def load(
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
                installed, ``app.code_path`` is not a literal path, the
                ``checks`` section is invalid, or the section of a discovered
                ``ConfiguredInstaller`` is missing or invalid, as at start-up.
        """
        loaded = LoadedConfig.from_paths(
            config_paths,
            config_dir=config_dir,
            environment=environment,
            resolvers=(),
            caller="loom check",
        )
        ctx = loaded.config
        AppConfig.reject_misdeclared(ctx)
        load_catalog(ctx).require_installed()
        raw_code_path = ctx.raw(f"{ConfigKey.APP}.code_path", DEFAULT_CODE_PATH)
        if not isinstance(raw_code_path, str) or INTERPOLATION in raw_code_path:
            raise ConfigError(
                "app.code_path must be a literal path for loom check: a placeholder or a "
                "non-string value cannot name the directory its check modules import from"
            )
        code_path = add_code_path(raw_code_path, loaded.config_root)
        checked = cls(
            config=ctx,
            environment=loaded.environment,
            report=run_deploy_checks(ctx, scope=CheckScope.declared_by(ctx, excluded=excluded)),
            config_root=loaded.config_root,
            code_path=code_path,
        )
        phases.check_installers(ctx, checked.discovered.installers)
        return checked

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
