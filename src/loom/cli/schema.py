"""``loom schema``: the commands that prepare a row-scoped schema."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, NoReturn, TypeVar

import typer
import yaml

from loom.core.config.errors import ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.environment import (
    DB_PASSWORD_PREFIX,
    MIGRATOR_URL_VARIABLE,
    SUPERUSER_URL_VARIABLE,
    VERIFY_URL_VARIABLE,
    loom_variable,
)
from loom.core.schema_names import SchemaNames
from loom.core.stores.catalog import BACKEND_KEY, DEFAULT_STORE, SQLALCHEMY_BACKEND

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection

    from loom.stores.sqlalchemy.rls import BootstrapConfig
    from loom.stores.sqlalchemy.rls.verify import Report


_T = TypeVar("_T")
_STORES = str(ConfigKey.STORES)
_STORE_PATH = f"{_STORES}.{DEFAULT_STORE}"


schema_app = typer.Typer(no_args_is_help=True, help="Prepare a row-scoped schema.")
audit_app = typer.Typer(no_args_is_help=True, help="Operate the store's audit log.")
schema_app.add_typer(audit_app, name="audit")

_ConfigOption = Annotated[
    Path | None,
    typer.Option(
        "--config",
        help="A config directory or file; defaults to LOOM_CONFIG_DIR, else ./config.",
    ),
]


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


def bootstrap(config: _ConfigOption = None) -> None:
    """Install or upgrade the guard and configure roles, as the superuser; idempotent.

    The superuser URL comes from ``LOOM_SUPERUSER_URL`` and the password of the
    migrator and of each database user with a login from ``LOOM_DB_PASSWORD_<USER>``.
    """
    from loom.stores.sqlalchemy.rls import apply_bootstrap

    try:
        settings = _bootstrap_config(config)
        logins = [settings.roles.migrator] + [
            name for name, user in settings.database_users.items() if user.login
        ]
        passwords = {name: _env(DB_PASSWORD_PREFIX + name.upper()) for name in logins}
        asyncio.run(apply_bootstrap(_env(SUPERUSER_URL_VARIABLE), settings, passwords))
    except ConfigError as exc:
        _fail(exc)
    typer.echo("guard is installed and configured")


def verify_schema(config: _ConfigOption = None) -> None:
    """Compare the database with the application; exit 1 on any drift.

    The URL comes from ``LOOM_VERIFY_URL``, else ``LOOM_SUPERUSER_URL``: the
    guard's configuration row needs a privileged reader. Audit partitions past
    the retention are listed, not counted as drift.
    """
    from loom.stores.sqlalchemy.alembic import load_store_schema
    from loom.stores.sqlalchemy.rls import verify

    try:
        schema = load_store_schema(None if config is None else str(config))
        url = loom_variable(VERIFY_URL_VARIABLE) or _env(SUPERUSER_URL_VARIABLE)
        report = asyncio.run(verify(url, schema))
    except ConfigError as exc:
        _fail(exc)
    _print_report(report)
    if not report.ok:
        raise typer.Exit(1)


def _print_report(report: Report) -> None:
    """One line per finding, then one per audit partition past the retention."""
    for finding in report.findings:
        subject = f"{finding.table} " if finding.table else ""
        typer.echo(f"{subject}{finding.check}: expected {finding.expected}, found {finding.actual}")
    for partition in report.expired_audit_partitions:
        typer.echo(f"expired audit partition: {partition}")


def drop_expired() -> None:
    """Drop the oldest audit partition past the retention, as the migrator; one per run."""
    from loom.stores.sqlalchemy.rls.audit import drop_expired_audit_partition

    try:
        dropped = asyncio.run(_as_migrator(drop_expired_audit_partition))
    except ConfigError as exc:
        _fail(exc)
    typer.echo(f"dropped {dropped}" if dropped else "no audit partition is past the retention")


def ensure_partitions() -> None:
    """Create the audit partitions missing in the next twelve months, as the migrator.

    Idempotent; meant for a scheduler between deploys (monthly is enough).
    """
    from loom.stores.sqlalchemy.rls.audit import ensure_audit_partitions

    try:
        created = asyncio.run(_as_migrator(ensure_audit_partitions))
    except ConfigError as exc:
        _fail(exc)
    typer.echo(f"created {created} audit partition(s)")


async def _as_migrator(action: Callable[[AsyncConnection], Awaitable[_T]]) -> _T:
    """Run *action* in one transaction as the migrator (``LOOM_MIGRATOR_URL``)."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(_env(MIGRATOR_URL_VARIABLE), poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            return await action(connection)
    finally:
        await engine.dispose()


def _bootstrap_config(config: Path | None) -> BootstrapConfig:
    from loom.stores.sqlalchemy.alembic import load_store_schema

    settings = load_store_schema(None if config is None else str(config)).bootstrap
    if settings is None:
        raise ConfigError("the configuration declares no row-scoped schema to bootstrap")
    return settings


def _env(name: str) -> str:
    value = loom_variable(name)
    if not value:
        raise ConfigError(f"set {name}")
    return value


def _fail(exc: Exception) -> NoReturn:
    typer.echo(str(exc), err=True)
    raise typer.Exit(1) from exc
