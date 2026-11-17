"""Deploy checks: settings that are unsafe once an application serves real traffic.

A check is a function of the loaded configuration registered under a stable
id ``<area>.<E|W><nnn>``; loom's own areas start with ``loom.``
(``loom.rest.E001``). The letter is the level: ``E`` fails startup, ``W`` is
logged. A check returns a :class:`Finding` when the configuration is unsafe,
``None`` otherwise::

    @deploy_check("billing.invoices.W001")
    def _no_test_gateway(config: ConfigContext) -> Finding | None:
        if config.section_or_default("billing.gateway", str, "") != "sandbox":
            return None
        return Finding(message="billing uses the sandbox gateway", hint="set billing.gateway")

A check registers in a :class:`CheckSet`. Each ``loom.deploy_checks``
entry point targets a ``CHECKS`` set and is named by the dotted configuration
path whose settings its checks judge (``"observability.log"``, ``stores``,
``"stores.settings.cursors"``, ``"app.rest"``); a package that ships checks
registers there too. A set is imported only when the process uses its path:
the path is declared in the configuration or the process requires it, and the
process does not leave it out (:class:`CheckScope`). A third-party module a
used set needs that is not installed is a :class:`~loom.core.config.errors.ConfigError`,
never a finding, so it cannot be silenced. An application lists the modules
holding its own checks under ``checks.modules``; they register in
:data:`DEPLOY_CHECKS`, which always runs, so the same checks run at startup
and in ``loom check --deploy``.

The application factories call :func:`enforce_deploy_checks`, which runs the
checks only in production and staging. ``loom check --deploy`` runs
:func:`run_deploy_checks` through ``Project.check``, which runs them whatever
the environment, for CI.
An id listed under ``checks.silenced`` in the configuration does not run, and
enforcing logs a warning naming every silenced id. This module's logger has
its own WARNING level, so a quieter parent does not hide these warnings.
"""

from __future__ import annotations

import importlib
import logging
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

import msgspec

from loom.core.config.errors import ConfigError
from loom.core.config.keys import INTERPOLATION, ConfigKey
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.plugins.entrypoints import MissingDependency, list_entry_points

if TYPE_CHECKING:
    from importlib.metadata import EntryPoint

    from loom.core.config.context import ConfigContext

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.WARNING)

CHECK_ID: Final = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.[EW]\d{3}")
"""Shape of a check id: a dotted area, then ``E`` or ``W`` and three digits."""

SILENCED_KEY: Final = "checks.silenced"
"""Configuration key listing the check ids accepted on purpose."""

DEPLOY_CHECKS_GROUP: Final = "loom.deploy_checks"
"""Entry-point group whose entries name a configuration path and target its :class:`CheckSet`."""

MODULES_KEY: Final = "checks.modules"
"""Configuration key listing the modules that register the application's checks."""


class CheckLevel(StrEnum):
    """Severity of a deploy check, given by the letter of its id."""

    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class Finding:
    """What a check found unsafe, and how to fix it.

    Attributes:
        message: The unsafe setting, naming its configuration key.
        hint: The change that makes it safe.
    """

    message: str
    hint: str


@dataclass(frozen=True)
class CheckResult:
    """A finding reported under the id of the check that made it.

    Attributes:
        id: Stable id of the check.
        message: The unsafe setting, naming its configuration key.
        hint: The change that makes it safe.
    """

    id: str
    message: str
    hint: str

    @property
    def level(self) -> CheckLevel:
        """Level of the check, read from the letter of its id."""
        return CheckLevel.ERROR if self.id.rsplit(".", 1)[1][0] == "E" else CheckLevel.WARNING

    def describe(self) -> str:
        """Return ``<id>: <message> Hint: <hint>`` on one line."""
        return f"{self.id}: {self.message} Hint: {self.hint}"


DeployCheck = Callable[["ConfigContext"], Finding | None]
"""A check: the loaded configuration in, a :class:`Finding` or ``None`` out."""


class SkipReason(StrEnum):
    """Why the deploy checks of a configuration path did not run."""

    NOT_DECLARED = "not declared"
    EXCLUDED = "left out by the process"


@dataclass(frozen=True)
class SkippedCheck:
    """The checks of a configuration path left out of a run.

    Attributes:
        path: Configuration path the checks validate.
        reason: Why they did not run.
    """

    path: str
    reason: SkipReason

    def describe(self) -> str:
        """Return ``<path> (<reason>)``."""
        return f"{self.path} ({self.reason})"


@dataclass(frozen=True)
class CheckScope:
    """Which configuration paths a process runs the checks of.

    Attributes:
        required: Paths the process uses even when the configuration does not
            declare them: a FastAPI process serves ``app.rest`` with its defaults.
        excluded: Paths the process never uses: a worker serves no ``app.rest``.
    """

    required: frozenset[str] = frozenset()
    excluded: frozenset[str] = frozenset()

    @classmethod
    def without_rest(cls) -> CheckScope:
        """Return the scope of a process that serves no HTTP."""
        return cls(excluded=frozenset({ConfigKey.APP_REST}))

    def skip_reason(self, path: str, config: ConfigContext) -> SkipReason | None:
        """Say why the checks of *path* do not run.

        Args:
            path: Configuration path the checks validate.
            config: Loaded configuration, read as written.

        Returns:
            ``None`` when the process uses *path*.
        """
        if path in self.excluded:
            return SkipReason.EXCLUDED
        if path in self.required or config.raw(path) is not None:
            return None
        return SkipReason.NOT_DECLARED


UNSCOPED: Final = CheckScope()
"""The scope of a process that requires and leaves out nothing."""


class CheckSet:
    """Deploy checks by id, in registration order."""

    def __init__(self) -> None:
        self._checks: dict[str, DeployCheck] = {}

    def register(self, check_id: str) -> Callable[[DeployCheck], DeployCheck]:
        """Return a decorator registering a check under *check_id*.

        Raises:
            ValueError: When *check_id* is outside the id scheme or taken.
        """
        _require_id(check_id)
        if check_id in self._checks:
            raise ValueError(f"deploy check {check_id!r} is already registered")

        def _register(check: DeployCheck) -> DeployCheck:
            self._checks[check_id] = check
            return check

        return _register

    def items(self) -> tuple[tuple[str, DeployCheck], ...]:
        """Return the registered ids with their checks, in registration order."""
        return tuple(self._checks.items())


DEPLOY_CHECKS: Final = CheckSet()
"""The application's checks, fed by ``checks.modules``; always run."""

deploy_check: Final = DEPLOY_CHECKS.register
"""Decorator registering a check in :data:`DEPLOY_CHECKS` under an id."""


@dataclass(frozen=True)
class InstalledChecks:
    """The :class:`CheckSet` one ``loom.deploy_checks`` entry point targets.

    Args:
        entry: The entry point; its name is the configuration path it validates.
    """

    entry: EntryPoint

    @property
    def path(self) -> str:
        """Configuration path the checks validate."""
        return self.entry.name

    def load(self) -> CheckSet:
        """Import the set the entry point targets.

        Returns:
            The set.

        Raises:
            ModuleNotFoundError: When the missing module belongs to loom.
            ConfigError: When a third-party module the set needs is not
                installed, naming the path, the module and the extras.
        """
        try:
            loaded: CheckSet = self.entry.load()
        except ModuleNotFoundError as exc:
            missing = MissingDependency.of(exc, self.entry)
            if missing is None:
                raise
            dist = self.entry.dist
            owner = self.entry.module if dist is None else f"{self.entry.module}, {dist.name}"
            raise ConfigError(
                f"{self.path} is used but its deploy checks ({owner}) {missing.describe()}, "
                f"or pass --skip {self.path} for a process that does not use it"
            ) from exc
        return loaded


@dataclass(frozen=True)
class DeployReport:
    """Outcome of one run of the deploy checks.

    Attributes:
        results: Every finding, in registration order of its check.
        silenced: Ids listed under ``checks.silenced``, in configuration order.
        checked: Configuration paths whose installed checks ran.
        skipped: Configuration paths whose installed checks did not run.
    """

    results: tuple[CheckResult, ...]
    silenced: tuple[str, ...]
    checked: tuple[str, ...] = ()
    skipped: tuple[SkippedCheck, ...] = ()

    @property
    def errors(self) -> tuple[CheckResult, ...]:
        """The findings that fail startup."""
        return tuple(r for r in self.results if r.level is CheckLevel.ERROR)

    @property
    def warnings(self) -> tuple[CheckResult, ...]:
        """The findings that are logged."""
        return tuple(r for r in self.results if r.level is CheckLevel.WARNING)


@dataclass(frozen=True)
class _Selection:
    """The checks a run executes, and the installed paths it ran or left out."""

    checks: dict[str, DeployCheck]
    checked: tuple[str, ...]
    skipped: tuple[SkippedCheck, ...]

    @classmethod
    def choose(cls, config: ConfigContext, scope: CheckScope, application: CheckSet) -> _Selection:
        """Load the installed sets *scope* uses, then add *application*.

        Raises:
            ModuleNotFoundError: When a used set misses a module of loom.
            ConfigError: When a used set misses a third-party module.
            ValueError: When two sets register one check id.
        """
        checks: dict[str, DeployCheck] = {}
        checked: list[str] = []
        skipped: list[SkippedCheck] = []
        installed = (InstalledChecks(e) for e in list_entry_points(DEPLOY_CHECKS_GROUP))
        for entry in sorted(installed, key=lambda e: e.path):
            reason = scope.skip_reason(entry.path, config)
            if reason is not None:
                skipped.append(SkippedCheck(entry.path, reason))
                continue
            checked.append(entry.path)
            cls._add(checks, entry.load())
        cls._add(checks, application)
        return cls(checks=checks, checked=tuple(checked), skipped=tuple(skipped))

    @staticmethod
    def _add(checks: dict[str, DeployCheck], check_set: CheckSet) -> None:
        for check_id, check in check_set.items():
            if check_id in checks:
                raise ValueError(f"deploy check {check_id!r} is registered by two check sets")
            checks[check_id] = check


class DeployCheckError(ConfigError):
    """Raised at startup when a deploy check of level error has a finding.

    Args:
        environment: Environment the checks ran in.
        results: The error findings.
    """

    def __init__(self, environment: Environment, results: Iterable[CheckResult]) -> None:
        self.environment = environment
        self.results = tuple(results)
        lines = "\n".join(f"  {result.describe()}" for result in self.results)
        super().__init__(
            f"deploy checks failed in the {str(environment)!r} environment:\n{lines}\n"
            f"Deploy checks run in production and staging; {ENVIRONMENT_VARIABLE} unset "
            f"means production. For local work set "
            f"{ENVIRONMENT_VARIABLE}={Environment.DEVELOPMENT.value}. To accept a "
            f"finding on purpose, list its id under {SILENCED_KEY}."
        )


class _ChecksConfig(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    silenced: tuple[str, ...] = ()
    modules: tuple[str, ...] = ()


def run_deploy_checks(
    config: ConfigContext,
    *,
    scope: CheckScope = UNSCOPED,
    application: CheckSet = DEPLOY_CHECKS,
) -> DeployReport:
    """Import ``checks.modules``, then run the checks of every path *scope* uses.

    Args:
        config: Loaded configuration the checks read.
        scope: The paths the process requires or leaves out; a path neither
            required nor excluded is used when the configuration declares it.
        application: The application's own set, which always runs; loom's by default.

    Returns:
        The findings, the silenced ids, and the paths checked and skipped.

    Raises:
        ConfigError: When the ``checks`` section holds a placeholder, an
            unknown key or a value of the wrong type; when ``checks.silenced``
            holds an entry outside the id scheme; when a module of
            ``checks.modules`` fails at import; or when a used set needs a
            third-party module that is not installed.
        ModuleNotFoundError: When a used set misses a module of loom itself.
        ValueError: When two sets register one check id.
    """
    section = _checks_section(config)
    _import_modules(section.modules)
    silenced = _silenced(section.silenced)
    selection = _Selection.choose(config, scope, application)
    results = tuple(
        CheckResult(id=check_id, message=finding.message, hint=finding.hint)
        for check_id, check in selection.checks.items()
        if check_id not in silenced
        if (finding := check(config)) is not None
    )
    return DeployReport(
        results=results,
        silenced=silenced,
        checked=selection.checked,
        skipped=selection.skipped,
    )


def silenced_checks(config: ConfigContext) -> tuple[str, ...]:
    """Return the ids ``checks.silenced`` lists, as the runner reads them.

    Raises:
        ConfigError: As :func:`run_deploy_checks` for the ``checks`` section.
    """
    return _silenced(_checks_section(config).silenced)


def enforce_deploy_checks(
    config: ConfigContext,
    environment: Environment,
    *,
    scope: CheckScope = UNSCOPED,
    application: CheckSet = DEPLOY_CHECKS,
) -> DeployReport | None:
    """Run the deploy checks at startup when *environment* is deployed.

    Logs the silenced ids and every warning, then fails on the errors.

    Args:
        config: Loaded configuration the checks read.
        environment: Resolved environment; only production and staging run checks.
        scope: See :func:`run_deploy_checks`.
        application: See :func:`run_deploy_checks`.

    Returns:
        The report, or ``None`` when the environment runs no check.

    Raises:
        DeployCheckError: When a check of level error has a finding.
        ConfigError: When the ``checks`` section holds a placeholder, an
            unknown key or a value of the wrong type; when ``checks.silenced``
            holds an entry outside the id scheme; or when a module of
            ``checks.modules`` fails at import; or when a used set needs a
            third-party module that is not installed.
    """
    if not environment.is_deployed:
        _logger.warning(
            "deploy checks skipped: the environment is %r (%s); they run in production and staging",
            str(environment),
            ENVIRONMENT_VARIABLE,
        )
        return None
    report = run_deploy_checks(config, scope=scope, application=application)
    if report.silenced:
        _logger.warning(
            "deploy checks silenced by %s in the %r environment: %s",
            SILENCED_KEY,
            str(environment),
            ", ".join(report.silenced),
        )
    for result in report.warnings:
        _logger.warning("deploy check %s", result.describe())
    if report.errors:
        raise DeployCheckError(environment, report.errors)
    return report


def _require_id(check_id: str) -> None:
    if CHECK_ID.fullmatch(check_id) is None:
        raise ValueError(
            f"check id {check_id!r} must look like '<area>.<E|W><nnn>', e.g. 'loom.rest.E001'"
        )


def _checks_section(config: ConfigContext) -> _ChecksConfig:
    """Decode ``checks`` as written: silences and modules are literals, never placeholders."""
    raw = config.raw("checks", {})
    if INTERPOLATION in repr(raw):
        raise ConfigError(
            "the checks section holds a placeholder; silenced ids and check modules "
            "are written literally, where they are reviewed"
        )
    try:
        return msgspec.convert(raw, _ChecksConfig)
    except msgspec.ValidationError as exc:
        raise ConfigError(f"the checks section is invalid: {exc}") from exc


def _import_modules(modules: tuple[str, ...]) -> None:
    for module in modules:
        try:
            importlib.import_module(module)
        except Exception as exc:
            raise ConfigError(f"{MODULES_KEY} entry {module!r} cannot be imported: {exc}") from exc


def _silenced(silenced: tuple[str, ...]) -> tuple[str, ...]:
    for entry in silenced:
        if CHECK_ID.fullmatch(entry) is None:
            raise ConfigError(
                f"{SILENCED_KEY} entry {entry!r} is not a check id like 'loom.rest.E001'"
            )
    return silenced


import loom.core.config.checks as _config_checks  # noqa: E402

_ = _config_checks


__all__ = [
    "CHECK_ID",
    "DEPLOY_CHECKS",
    "MODULES_KEY",
    "SILENCED_KEY",
    "silenced_checks",
    "CheckLevel",
    "CheckResult",
    "CheckScope",
    "CheckSet",
    "UNSCOPED",
    "DeployCheck",
    "DeployCheckError",
    "DeployReport",
    "Finding",
    "InstalledChecks",
    "SkipReason",
    "SkippedCheck",
    "deploy_check",
    "enforce_deploy_checks",
    "run_deploy_checks",
]
