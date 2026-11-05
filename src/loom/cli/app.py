"""The ``loom`` command tree."""

from __future__ import annotations

import typer

from loom.cli.check import check
from loom.cli.deploy import deploy_app
from loom.cli.schema import schema_app

app = typer.Typer(no_args_is_help=True, help="Operate a loom application.")
app.add_typer(schema_app, name="schema")
app.add_typer(deploy_app, name="deploy")
app.command("check")(check)
