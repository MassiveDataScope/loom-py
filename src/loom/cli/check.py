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

from pathlib import Path
from typing import Annotated

import typer

from loom.composition.project import CheckedProject, Project
from loom.core.config.errors import ConfigError
from loom.serverless.aws.lambda_.trigger_jobs import check_triggers


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
        checked = check_config(config, env, tuple(skip or ()))
        print_report(checked, err=False)
        check_triggers(checked.config)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    if checked.report.errors:
        raise typer.Exit(1)


def check_config(config: list[Path], env: str | None, skip: tuple[str, ...]) -> CheckedProject:
    """Run the deploy checks on *config*: one directory, or YAML files.

    Raises:
        ConfigError: When the configuration does not load, or directories and
            files are mixed.
    """
    if len(config) == 1 and config[0].is_dir():
        return Project.check(config_dir=str(config[0]), environment=env, skip=skip)
    if any(path.is_dir() for path in config):
        raise ConfigError("pass one config directory or YAML files, not both")
    return Project.check(*(str(path) for path in config), environment=env, skip=skip)


def print_report(checked: CheckedProject, *, err: bool) -> None:
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
