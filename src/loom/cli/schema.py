"""``loom schema``: the commands that prepare a row-scoped schema."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer
import yaml

from loom.core.config.keys import ConfigKey
from loom.core.persistence.stores import BACKEND_KEY, DEFAULT_STORE, SQLALCHEMY_BACKEND
from loom.core.schema_names import SchemaNames

_STORES = str(ConfigKey.STORES)  # a plain str: yaml.safe_dump refuses a StrEnum key
_STORE_PATH = f"{_STORES}.{DEFAULT_STORE}"

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
    into ``stores.default.schema``, declaring the store with ``backend:
    sqlalchemy`` when the file does not; a name the product already changed
    is never overwritten.
    """
    try:
        block = _block(name, SchemaNames.derived(name))
        if config is None:
            document = {
                _STORES: {DEFAULT_STORE: {BACKEND_KEY: SQLALCHEMY_BACKEND, "schema": block}}
            }
            typer.echo(yaml.safe_dump(document, sort_keys=False), nl=False)
            return
        _write(config, block)
    except (ValueError, yaml.YAMLError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


def _write(config: Path, block: dict[str, Any]) -> None:
    document = _load(config)
    store = _section(_section(document, _STORES, _STORES), DEFAULT_STORE, _STORE_PATH)
    backend = store.setdefault(BACKEND_KEY, SQLALCHEMY_BACKEND)
    if backend != SQLALCHEMY_BACKEND:
        raise ValueError(
            f"{_STORE_PATH}.{BACKEND_KEY} is {backend!r}; "
            f"a row-scoped schema needs {SQLALCHEMY_BACKEND}"
        )
    schema = _section(store, "schema", f"{_STORE_PATH}.schema")
    conflicts = _merge(schema, block, f"{_STORE_PATH}.schema")
    if conflicts:
        raise ValueError(
            "these names differ from the derived ones and were kept: " + ", ".join(conflicts)
        )
    config.write_text(yaml.safe_dump(document, sort_keys=False))


def _load(config: Path) -> dict[str, Any]:
    if not config.exists():
        return {}
    loaded = yaml.safe_load(config.read_text())
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{config} must hold a mapping at its top level")
    return loaded


def _section(parent: dict[str, Any], key: str, path: str) -> dict[str, Any]:
    current = parent.get(key)
    if current is None:
        current = parent[key] = {}
    if not isinstance(current, dict):
        raise ValueError(f"{path} must be a mapping")
    return current


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
        if current is None:
            target[key] = value
        elif isinstance(value, dict) and isinstance(current, dict):
            conflicts += _merge(current, value, f"{path}.{key}")
        elif current != value:
            conflicts.append(f"{path}.{key}")
    return conflicts
