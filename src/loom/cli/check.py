"""``loom check --deploy``: run the deploy checks without starting the application.

Meant for CI: it loads the configuration the application would load, runs
every deploy check whatever the environment, prints each finding with its id
and hint, and exits ``1`` when an error remains. loom's checks read keys as
written and resolve no placeholder; the configuration loads with loom's
default resolvers, so a ``${secrets:...}`` value no check reads is left alone.

The command cannot tell which kind of process a configuration is for: a REST
application may declare ``celery`` for its job service and a worker may share
``app.rest``. ``--skip loom.rest`` (repeatable, any id or area) leaves out
what the process does not run, as the worker and the runners do at startup.

``app.code_path`` (``src`` by default, relative to the first file) joins the
import path as ``create_app`` adds it, so the modules of ``checks.modules``
import here as they do at startup.
"""

from __future__ import annotations

import importlib
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from loom.core.checks import DeployReport, run_deploy_checks
from loom.core.config import ConfigContext, with_default_resolvers
from loom.core.config.errors import ConfigError
from loom.core.config.overrides import load_application_config
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.serverless.aws.lambda_.checks import check_triggers


def check(
    config: Annotated[
        list[Path],
        typer.Argument(help="A config directory (base.yaml plus overlays) or YAML files."),
    ],
    deploy: Annotated[bool, typer.Option("--deploy", help="Run the deploy checks.")] = False,
    env: Annotated[
        str | None,
        typer.Option("--env", help="Environment picking the overlay; defaults to LOOM_ENV."),
    ] = None,
    skip: Annotated[
        list[str] | None,
        typer.Option("--skip", help="A check id or area to leave out, e.g. loom.rest; repeatable."),
    ] = None,
) -> None:
    """Run the deploy checks against the configuration, for CI."""
    if not deploy:
        typer.echo("only deploy checks exist: run 'loom check --deploy <config>'.", err=True)
        raise typer.Exit(2)
    try:
        checked = load_checked(config, env, tuple(skip or ()))
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    print_report(checked, err=False)
    if checked.report.errors:
        raise typer.Exit(1)


@dataclass(frozen=True)
class CheckedConfig:
    """A configuration loaded as the application would load it, and its deploy checks."""

    context: ConfigContext
    environment: Environment
    report: DeployReport
    skipped: tuple[str, ...]
    config_root: Path
    code_path: Path


def load_checked(config: list[Path], env: str | None, skip: tuple[str, ...]) -> CheckedConfig:
    """Load *config* for *env*, put its code path on the import path and run the deploy checks.

    Raises:
        ConfigError: When the configuration does not load, or ``lambda`` is
            one startup would refuse.
    """
    _load_check_packages()
    environment = resolve_environment(env)
    paths = _config_paths(config, environment)
    context = load_application_config(
        *paths, resolvers=with_default_resolvers(()), environment=environment
    )
    config_root = Path(paths[0]).resolve().parent
    code_path = _add_code_path(context, config_root)
    check_triggers(context)
    report = run_deploy_checks(context, skip=skip)
    return CheckedConfig(context, environment, report, skip, config_root, code_path)


def _load_check_packages() -> None:
    """Import the packages registering loom's checks; the REST ones need the rest extra."""
    importlib.import_module("loom.core.logger")
    importlib.import_module("loom.core.repository.abc")
    importlib.import_module("loom.core.persistence.audit_checks")
    if importlib.util.find_spec("fastapi") is not None:
        importlib.import_module("loom.rest")


def _add_code_path(context: ConfigContext, config_dir: Path) -> Path:
    raw = context.raw("app.code_path", DEFAULT_CODE_PATH)
    if not isinstance(raw, str) or "${" in raw:
        raise ConfigError(
            "app.code_path must be a literal path for loom check: a placeholder or a "
            "non-string value cannot name the directory its check modules import from"
        )
    return add_code_path(raw, config_dir)


def _config_paths(config: list[Path], environment: Environment) -> tuple[str, ...]:
    directory = config[0] if len(config) == 1 and config[0].is_dir() else None
    if directory is None and any(path.is_dir() for path in config):
        raise ConfigError("pass one config directory or YAML files, not both")
    files = () if directory is not None else tuple(str(path) for path in config)
    return select_config_paths(files, directory, environment, caller="loom check")


def print_report(checked: CheckedConfig, *, err: bool) -> None:
    """Print the findings of *checked*, to stderr when *err*."""
    report = checked.report
    typer.echo(f"environment: {checked.environment}", err=err)
    if checked.skipped:
        typer.echo(f"skipped: {', '.join(checked.skipped)}", err=err)
    if report.silenced:
        typer.echo(f"silenced: {', '.join(report.silenced)}", err=err)
    for result in report.results:
        typer.echo(f"{result.level.upper()} {result.describe()}", err=err)
    typer.echo(f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)", err=err)
