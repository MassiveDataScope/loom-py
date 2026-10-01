"""``loom schema``: the commands that prepare a row-scoped schema."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer
import yaml

if TYPE_CHECKING:
    from loom.core.repository.sqlalchemy.rls.config import SchemaNames

schema_app = typer.Typer(no_args_is_help=True, help="Prepare a row-scoped schema.")


@schema_app.command("init")
def init(
    name: Annotated[str, typer.Argument(help="The application schema.")],
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Merge the names into this configuration file."),
    ] = None,
) -> None:
    """Derive the names around a schema once and write them into the configuration.

    Without ``--config`` the block is printed. With it the names are merged
    into ``database.schema``; a name the product already changed is never
    overwritten.
    """
    from loom.core.repository.sqlalchemy.rls.config import SchemaNames

    try:
        block = _block(name, SchemaNames.derived(name))
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    if config is None:
        typer.echo(yaml.safe_dump({"database": {"schema": block}}, sort_keys=False), nl=False)
        return
    document: dict[str, Any] = (
        yaml.safe_load(config.read_text()) if config.exists() else None
    ) or {}
    schema = document.setdefault("database", {}).setdefault("schema", {})
    conflicts = _merge(schema, block, "database.schema")
    if conflicts:
        typer.echo(
            "these names differ from the derived ones and were kept: " + ", ".join(conflicts),
            err=True,
        )
        raise typer.Exit(1)
    config.write_text(yaml.safe_dump(document, sort_keys=False))


def _load(config: Path) -> dict[str, Any]:
    if not config.exists():
        return {}
    loaded = yaml.safe_load(config.read_text())
    return loaded if isinstance(loaded, dict) else {}


def _block(name: str, names: SchemaNames) -> dict[str, Any]:
    return {
        "name": name,
        "guard": names.guard,
        "groups": {"readers": names.readers, "writers": names.writers},
        "version_tables": {"structure": names.version_table, "data": names.data_version_table},
    }


def _merge(target: dict[str, Any], values: dict[str, Any], path: str) -> list[str]:
    conflicts: list[str] = []
    for key, value in values.items():
        current = target.get(key)
        if isinstance(value, dict):
            nested = current if isinstance(current, dict) else target.setdefault(key, {})
            conflicts += _merge(nested, value, f"{path}.{key}")
        elif current is None:
            target[key] = value
        elif current != value:
            conflicts.append(f"{path}.{key}")
    return conflicts
