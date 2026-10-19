"""``loom check --deploy``: run the deploy checks without starting the application.

Meant for CI: it loads the configuration the application would load, runs
every deploy check whatever the environment, prints each finding with its id
and hint, and exits ``1`` when an error remains. No placeholder is resolved
unless a check reads it, so secrets need not be reachable.

``app.code_path`` (``src`` by default, relative to the first file) joins the
import path as ``create_app`` adds it, so the modules of ``checks.modules``
import here as they do at startup.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Annotated

import typer

from loom.core.checks import DeployReport, run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.environment import Environment, environment_config_paths, resolve_environment

# create_app's default for app.code_path.
_DEFAULT_CODE_PATH = "src"

# The packages registering loom's checks, each with the dependency it needs.
# Imported by the command, not at module top: without the 'rest' extra the
# application serves no HTTP, so the REST checks do not apply.
_CHECK_PACKAGES: tuple[tuple[str, str | None], ...] = (
    ("loom.core.logger", None),
    ("loom.rest", "fastapi"),
)


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
) -> None:
    """Run the deploy checks against the configuration, for CI."""
    if not deploy:
        typer.echo("only deploy checks exist: run 'loom check --deploy <config>'.", err=True)
        raise typer.Exit(2)
    _load_check_packages()
    try:
        environment = resolve_environment(env)
        paths = _config_paths(config, environment)
        context = ConfigContext.from_yaml(*paths)
        _add_code_path(context, Path(paths[0]))
        report = run_deploy_checks(context)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    _print(report, environment)
    if report.errors:
        raise typer.Exit(1)


def _load_check_packages() -> None:
    """Import the packages registering loom's deploy checks, when installed."""
    for package, dependency in _CHECK_PACKAGES:
        if dependency is None or _installed(dependency):
            importlib.import_module(package)


def _installed(module: str) -> bool:
    try:
        importlib.import_module(module)
    except ModuleNotFoundError:
        return False
    return True


def _add_code_path(context: ConfigContext, first_file: Path) -> None:
    code_path = Path(context.section_or_default("app.code_path", str, _DEFAULT_CODE_PATH))
    if not code_path.is_absolute():
        code_path = first_file.resolve().parent / code_path
    entry = str(code_path.resolve())
    if code_path.is_dir() and entry not in sys.path:
        sys.path.insert(0, entry)


def _config_paths(config: list[Path], environment: Environment) -> tuple[str, ...]:
    if len(config) == 1 and config[0].is_dir():
        return environment_config_paths(config[0], environment)
    directories = [str(path) for path in config if path.is_dir()]
    if directories:
        raise ConfigError(
            f"pass one config directory or YAML files, not both: {', '.join(directories)}"
        )
    return tuple(str(path) for path in config)


def _print(report: DeployReport, environment: Environment) -> None:
    typer.echo(f"environment: {environment}")
    if report.silenced:
        typer.echo(f"silenced: {', '.join(report.silenced)}")
    for result in report.results:
        typer.echo(f"{result.level.upper()} {result.describe()}")
    typer.echo(f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)")
