"""``loom deploy manifest``: what each Lambda needs deployed, as JSON for the deployment tool.

The configuration loads as ``loom check --deploy`` loads it and must pass the
same deploy checks, whose findings go to stderr; a failing one prints no
manifest. The manifest alone goes to stdout.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import msgspec
import typer

from loom.cli.check import load_checked, print_report
from loom.core.config.errors import ConfigError
from loom.serverless.aws.lambda_.manifest import build_manifest

deploy_app = typer.Typer(no_args_is_help=True, help="Describe what a deployment needs.")


@deploy_app.command("manifest")
def manifest(
    config: Annotated[
        list[Path],
        typer.Argument(help="A config directory (base.yaml plus overlays) or YAML files."),
    ],
    function: Annotated[
        str | None,
        typer.Option("--function", help="Only this function of lambda.functions."),
    ] = None,
    env: Annotated[
        str | None,
        typer.Option("--env", help="Environment picking the overlay; defaults to LOOM_ENV."),
    ] = None,
    skip: Annotated[
        list[str] | None,
        typer.Option("--skip", help="A check id or area to leave out, e.g. loom.rest; repeatable."),
    ] = None,
) -> None:
    """Print the deploy manifest of lambda.functions once the deploy checks pass."""
    try:
        checked = load_checked(config, env, tuple(skip or ()))
        print_report(checked, err=True)
        if checked.report.errors:
            raise typer.Exit(1)
        document = build_manifest(
            checked.context,
            checked.environment,
            function,
            config_root=checked.config_root,
            code_path=checked.code_path,
        )
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    # Sorted keys: the same configuration prints the same bytes, so CI can diff them.
    encoded = msgspec.json.encode(document, order="sorted")
    typer.echo(msgspec.json.format(encoded, indent=2).decode())
