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
from pathlib import Path
from typing import Annotated

import typer

from loom.core.checks import DeployReport, run_deploy_checks
from loom.core.config import ConfigContext, with_default_resolvers
from loom.core.config.errors import ConfigError
from loom.core.config.overrides import load_application_config
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path


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
    _load_check_packages()
    try:
        environment = resolve_environment(env)
        paths = _config_paths(config, environment)
        context = load_application_config(*paths, resolvers=with_default_resolvers(()))
        _add_code_path(context, Path(paths[0]).resolve().parent)
        report = run_deploy_checks(context, skip=tuple(skip or ()))
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    _print(report, environment, tuple(skip or ()))
    if report.errors:
        raise typer.Exit(1)


def _load_check_packages() -> None:
    """Import the packages registering loom's checks; the REST ones need the rest extra."""
    importlib.import_module("loom.core.logger")
    if importlib.util.find_spec("fastapi") is not None:
        importlib.import_module("loom.rest")


def _add_code_path(context: ConfigContext, config_dir: Path) -> None:
    raw = context.raw("app.code_path", DEFAULT_CODE_PATH)
    add_code_path(raw if isinstance(raw, str) else DEFAULT_CODE_PATH, config_dir)


def _config_paths(config: list[Path], environment: Environment) -> tuple[str, ...]:
    directory = config[0] if len(config) == 1 and config[0].is_dir() else None
    if directory is None and any(path.is_dir() for path in config):
        raise ConfigError("pass one config directory or YAML files, not both")
    files = () if directory is not None else tuple(str(path) for path in config)
    return select_config_paths(files, directory, environment, caller="loom check")


def _print(report: DeployReport, environment: Environment, skipped: tuple[str, ...]) -> None:
    typer.echo(f"environment: {environment}")
    if skipped:
        typer.echo(f"skipped: {', '.join(skipped)}")
    if report.silenced:
        typer.echo(f"silenced: {', '.join(report.silenced)}")
    for result in report.results:
        typer.echo(f"{result.level.upper()} {result.describe()}")
    typer.echo(f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)")
