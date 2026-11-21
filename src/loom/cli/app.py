"""The ``loom`` command tree: every command is registered here, through the loom boundary."""

from __future__ import annotations

from typing import Final

import typer

from loom.cli import deploy, schema
from loom.cli.check import check
from loom.core.errors.boundary import PassThrough, boundary

TYPER_PASS_THROUGH: Final[PassThrough] = (typer.Exit, typer.Abort, typer.exceptions.TyperException)
"""How a command ends on purpose: ``typer.Exit``, an abort, or a usage error the command wrote."""

app = typer.Typer(no_args_is_help=True, help="Operate a loom application.")
app.add_typer(schema.schema_app, name="schema")
app.add_typer(deploy.deploy_app, name="deploy")
app.command("check")(boundary(check, pass_through=TYPER_PASS_THROUGH))
schema.schema_app.command("init")(boundary(schema.init, pass_through=TYPER_PASS_THROUGH))
schema.schema_app.command("bootstrap")(boundary(schema.bootstrap, pass_through=TYPER_PASS_THROUGH))
schema.schema_app.command("verify")(boundary(schema.verify_schema, pass_through=TYPER_PASS_THROUGH))
schema.audit_app.command("drop-expired")(
    boundary(schema.drop_expired, pass_through=TYPER_PASS_THROUGH)
)
schema.audit_app.command("ensure-partitions")(
    boundary(schema.ensure_partitions, pass_through=TYPER_PASS_THROUGH)
)
deploy.deploy_app.command("manifest")(boundary(deploy.manifest, pass_through=TYPER_PASS_THROUGH))
