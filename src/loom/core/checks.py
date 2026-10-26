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

A check registers when the module defining it is imported. loom's own checks
live in packages the entry points import anyway (``loom.core.logger``,
``loom.core.repository.abc`` and ``loom.rest``); an application lists the
modules holding its checks under ``checks.modules``, which every run imports
first, so the same checks run at startup and in ``loom check --deploy``.

The application factories call :func:`enforce_deploy_checks`, which runs the
checks only in production and staging. ``loom check --deploy`` calls
:func:`run_deploy_checks`, which runs them whatever the environment, for CI.
An id listed under ``checks.silenced`` in the configuration does not run, and
enforcing logs a warning naming every silenced id. This module's logger has
its own WARNING level, so a quieter parent does not hide these warnings.
"""

from __future__ import annotations

import importlib
import logging
import re
from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

import msgspec

from loom.core.config.errors import ConfigError
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

_logger = logging.getLogger(__name__)
# Its own level, so a parent at CRITICAL (the top-level level, or a named
# level on ``loom``) cannot hide a check's warning or the silenced ids; a
# named level on this logger itself is the explicit way to quiet it.
_logger.setLevel(logging.WARNING)

CHECK_ID: Final = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.[EW]\d{3}")
"""Shape of a check id: a dotted area, then ``E`` or ``W`` and three digits."""

_INTERPOLATION: Final = "${"

SILENCED_KEY: Final = "checks.silenced"
"""Configuration key listing the check ids accepted on purpose."""

MODULES_KEY: Final = "checks.modules"
"""Configuration key listing the modules that register the application's checks."""

REST_CHECKS: Final = "loom.rest"
"""Area of loom's REST checks; a process serving no HTTP skips it."""


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


class DeployCheckRegistry:
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


DEPLOY_CHECKS: Final = DeployCheckRegistry()
"""Registry loom's checks and the application's register into."""

deploy_check: Final = DEPLOY_CHECKS.register
"""Decorator registering a check in :data:`DEPLOY_CHECKS` under an id."""


@dataclass(frozen=True)
class DeployReport:
    """Outcome of one run of the deploy checks.

    Attributes:
        results: Every finding, in registration order of its check.
        silenced: Ids listed under ``checks.silenced``, in configuration order.
    """

    results: tuple[CheckResult, ...]
    silenced: tuple[str, ...]

    @property
    def errors(self) -> tuple[CheckResult, ...]:
        """The findings that fail startup."""
        return tuple(r for r in self.results if r.level is CheckLevel.ERROR)

    @property
    def warnings(self) -> tuple[CheckResult, ...]:
        """The findings that are logged."""
        return tuple(r for r in self.results if r.level is CheckLevel.WARNING)


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
    skip: Collection[str] = (),
    registry: DeployCheckRegistry = DEPLOY_CHECKS,
) -> DeployReport:
    """Import ``checks.modules``, then run every check not silenced nor skipped.

    Args:
        config: Loaded configuration the checks read.
        skip: Ids, or dotted areas such as ``"loom.rest"``, the caller knows
            not to apply: a process serving no HTTP skips the REST checks.
        registry: Registry to run; loom's by default.

    Returns:
        The findings and the silenced ids.

    Raises:
        ConfigError: When the ``checks`` section holds a placeholder, an
            unknown key or a value of the wrong type; when ``checks.silenced``
            holds an entry outside the id scheme; or when a module of
            ``checks.modules`` fails at import.
    """
    section = _checks_section(config)
    _import_modules(section.modules)
    silenced = _silenced(section.silenced)
    results = tuple(
        CheckResult(id=check_id, message=finding.message, hint=finding.hint)
        for check_id, check in registry.items()
        if check_id not in silenced and not _skipped(check_id, skip)
        if (finding := check(config)) is not None
    )
    return DeployReport(results=results, silenced=silenced)


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
    skip: Collection[str] = (),
    registry: DeployCheckRegistry = DEPLOY_CHECKS,
) -> DeployReport | None:
    """Run the deploy checks at startup when *environment* is deployed.

    Logs the silenced ids and every warning, then fails on the errors.

    Args:
        config: Loaded configuration the checks read.
        environment: Resolved environment; only production and staging run checks.
        skip: See :func:`run_deploy_checks`.
        registry: Registry to run; loom's by default.

    Returns:
        The report, or ``None`` when the environment runs no check.

    Raises:
        DeployCheckError: When a check of level error has a finding.
        ConfigError: When the ``checks`` section holds a placeholder, an
            unknown key or a value of the wrong type; when ``checks.silenced``
            holds an entry outside the id scheme; or when a module of
            ``checks.modules`` fails at import.
    """
    if not environment.is_deployed:
        _logger.warning(
            "deploy checks skipped: the environment is %r (%s); they run in production and staging",
            str(environment),
            ENVIRONMENT_VARIABLE,
        )
        return None
    report = run_deploy_checks(config, skip=skip, registry=registry)
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
    if _INTERPOLATION in repr(raw):
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
            # Any failure at import, not only ImportError, means the checks of
            # the module cannot run: it stops the run as a configuration error.
            raise ConfigError(f"{MODULES_KEY} entry {module!r} cannot be imported: {exc}") from exc


def _silenced(silenced: tuple[str, ...]) -> tuple[str, ...]:
    for entry in silenced:
        if CHECK_ID.fullmatch(entry) is None:
            raise ConfigError(
                f"{SILENCED_KEY} entry {entry!r} is not a check id like 'loom.rest.E001'"
            )
    return silenced


def _skipped(check_id: str, skip: Collection[str]) -> bool:
    return any(check_id == entry or check_id.startswith(f"{entry}.") for entry in skip)


# The configuration package is loaded above, while this module is still
# starting, so its check cannot be imported from that package's ``__init__``.
import loom.core.config.checks as _config_checks  # noqa: E402

_ = _config_checks


__all__ = [
    "CHECK_ID",
    "DEPLOY_CHECKS",
    "MODULES_KEY",
    "REST_CHECKS",
    "SILENCED_KEY",
    "silenced_checks",
    "CheckLevel",
    "CheckResult",
    "DeployCheck",
    "DeployCheckError",
    "DeployCheckRegistry",
    "DeployReport",
    "Finding",
    "deploy_check",
    "enforce_deploy_checks",
    "run_deploy_checks",
]
