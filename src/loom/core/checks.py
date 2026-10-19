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

A check registers when the module defining it is imported; loom's own checks
live in packages the application factories import anyway (``loom.core.logger``
and ``loom.rest``).

The application factories call :func:`enforce_deploy_checks`, which runs the
checks only in production and staging. ``loom check --deploy`` calls
:func:`run_deploy_checks`, which runs them whatever the environment, for CI.
An id listed under ``checks.silenced`` in the configuration does not run, and
enforcing logs a warning naming every silenced id.
"""

from __future__ import annotations

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

CHECK_ID: Final = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.[EW]\d{3}")
"""Shape of a check id: a dotted area, then ``E`` or ``W`` and three digits."""

SILENCED_KEY: Final = "checks.silenced"
"""Configuration key listing the check ids accepted on purpose."""


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

    def ids(self) -> tuple[str, ...]:
        """Return the registered ids, in registration order."""
        return tuple(self._checks)

    def items(self) -> tuple[tuple[str, DeployCheck], ...]:
        """Return the registered ids with their checks, in registration order."""
        return tuple(self._checks.items())


DEPLOY_CHECKS: Final = DeployCheckRegistry()
"""Registry loom's checks and the application's register into."""


def deploy_check(check_id: str) -> Callable[[DeployCheck], DeployCheck]:
    """Return a decorator registering a check in :data:`DEPLOY_CHECKS` under *check_id*.

    Raises:
        ValueError: When *check_id* is outside the id scheme or taken.
    """
    return DEPLOY_CHECKS.register(check_id)


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


class _ChecksConfig(msgspec.Struct, frozen=True, kw_only=True):
    silenced: tuple[str, ...] = ()


def run_deploy_checks(
    config: ConfigContext,
    *,
    skip: Collection[str] = (),
    registry: DeployCheckRegistry = DEPLOY_CHECKS,
) -> DeployReport:
    """Run every registered check not silenced by *config* nor skipped.

    Args:
        config: Loaded configuration the checks read.
        skip: Ids, or dotted areas such as ``"loom.rest"``, the caller knows
            not to apply: a worker skips the REST checks, an application
            authenticating in code skips the configuration check for it.
        registry: Registry to run; loom's by default.

    Returns:
        The findings and the silenced ids.

    Raises:
        ConfigError: When ``checks.silenced`` holds an entry outside the id scheme.
    """
    silenced = _silenced(config)
    results = tuple(
        CheckResult(id=check_id, message=finding.message, hint=finding.hint)
        for check_id, check in registry.items()
        if check_id not in silenced and not _skipped(check_id, skip)
        if (finding := check(config)) is not None
    )
    return DeployReport(results=results, silenced=silenced)


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
        ConfigError: When ``checks.silenced`` holds an entry outside the id scheme.
    """
    if not environment.is_deployed:
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


def _silenced(config: ConfigContext) -> tuple[str, ...]:
    silenced = config.section_or_default("checks", _ChecksConfig, _ChecksConfig()).silenced
    for entry in silenced:
        if CHECK_ID.fullmatch(entry) is None:
            raise ConfigError(
                f"{SILENCED_KEY} entry {entry!r} is not a check id like 'loom.rest.E001'"
            )
    return silenced


def _skipped(check_id: str, skip: Collection[str]) -> bool:
    return any(check_id == entry or check_id.startswith(f"{entry}.") for entry in skip)


__all__ = [
    "CHECK_ID",
    "DEPLOY_CHECKS",
    "SILENCED_KEY",
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
