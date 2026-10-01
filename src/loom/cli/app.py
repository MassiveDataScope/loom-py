"""The ``loom`` command tree."""

from __future__ import annotations

import typer

from loom.cli.schema import schema_app

app = typer.Typer(no_args_is_help=True, help="Operate the database side of a loom application.")
app.add_typer(schema_app, name="schema")
