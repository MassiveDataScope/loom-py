"""Minimal discoverable project used by the ``create_app`` config tests.

``create_app`` needs a real importable package to discover use cases and REST
interfaces, plus a YAML file. Writing both is boilerplate every config-driven
test needs, so it lives here once.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

_MODULE_PREFIX = "loom_config_fixture_app"
_DEFAULT_DATABASE: dict[str, Any] = {"url": "sqlite+aiosqlite:///"}
_NON_IDENTIFIER = re.compile(r"\W", re.ASCII)
_AUTOINCREMENT_ID_FIELD = "id: int = ColumnField(primary_key=True, autoincrement=True)"
UUID4_ID_FIELD = "id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)"
"""Primary key a backend without sequences (Mongo) accepts."""
CACHED_RECORDS_PREFIX = "/cached-records"
"""Prefix of the auto-CRUD routes of the ``@cached`` fixture model."""


class ReadCounter:
    """Counts the reads the ``@cached`` fixture repository serves itself."""

    def __init__(self) -> None:
        self.calls = 0

    def record(self) -> None:
        self.calls += 1

    def reset(self) -> None:
        self.calls = 0


CACHED_READS = ReadCounter()
"""Shared by every generated fixture module; reset it before each test."""

_APP_SOURCE = '''\
"""Minimal discoverable app used by the create_app configuration tests."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.model.enums import ServerDefault
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class ConfigRecord(BaseModel):
    __tablename__ = "config_records_fixture"

    {id_field}
    name: str = ColumnField(length=50)


class ConfigPingUseCase(UseCase[ConfigRecord, str]):
    """About ``ConfigRecord`` (so discovery finds the model) but needs no repository."""

    def __init__(self) -> None:
        super().__init__()

    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class ConfigPingInterface(RestInterface[str]):
    prefix = "{prefix}"
    routes = (RestRoute(use_case=ConfigPingUseCase, method="GET", path="{route_path}"),)
'''

_CACHED_MODEL_SOURCE = '''

from loom.core.cache import cached
from loom.core.repository import RepositoryBuildContext, repository_for
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy, SessionManager
from tests.unit.rest._fixture_app import CACHED_READS


class CachedRecord(BaseModel):
    __tablename__ = "cached_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


def _build_cached_record_repository(context: RepositoryBuildContext) -> Any:
    assert context.container is not None
    return CachedRecordRepository(
        context.container.resolve(SessionManager),
        CachedRecord,
        cursor_codec=context.container.resolve(CursorCodec),
    )


@cached
@repository_for(CachedRecord, builder=_build_cached_record_repository)
class CachedRecordRepository(RepositorySQLAlchemy[CachedRecord, int]):
    """Counts every read that reaches the database."""

    async def get_by_id(self, obj_id: int, profile: str = "default") -> CachedRecord | None:
        CACHED_READS.record()
        return await super().get_by_id(obj_id, profile)


class CachedRecordInterface(RestInterface[CachedRecord]):
    prefix = "{cached_prefix}"
    auto = True
'''


def write_project(
    tmp_path: Path,
    *,
    rest: dict[str, Any] | None = None,
    sql: dict[str, Any] | None = None,
    observability: dict[str, Any] | None = None,
    persistence: dict[str, Any] | None = None,
    database: dict[str, Any] | None = _DEFAULT_DATABASE,
    cache: dict[str, Any] | None = None,
    prefix: str = "/ping",
    route_path: str = "/",
    id_field: str = _AUTOINCREMENT_ID_FIELD,
    with_cached_model: bool = False,
) -> str:
    """Write the fixture module plus a YAML config and return the config path.

    Args:
        tmp_path: Directory owning the generated project.
        rest: Contents of the ``app.rest`` section.
        sql: Contents of the ``sql`` section.
        observability: Contents of the ``observability`` section.
        persistence: Contents of the ``persistence`` section, on top of an
            explicit ``cursors.signing: ephemeral``.
        database: Contents of the ``database`` section; ``None`` omits it.
        cache: Contents of the ``cache`` section; ``None`` omits it.
        prefix: Prefix of the generated REST interface.
        route_path: Path of its single route, relative to *prefix*.
        id_field: Source line declaring the fixture model's primary key.
        with_cached_model: Add a second model served by a ``@cached``
            SQLAlchemy repository that counts its reads in ``CACHED_READS``,
            with auto-CRUD routes under ``CACHED_RECORDS_PREFIX``.

    Returns:
        Path of the written YAML config file.
    """
    # One module per test: the interpreter caches imports by name, so a shared
    # name would serve the first test's routes to every later one.
    module = f"{_MODULE_PREFIX}_{_NON_IDENTIFIER.sub('_', tmp_path.name)}"
    source = _APP_SOURCE.format(prefix=prefix, route_path=route_path, id_field=id_field)
    if with_cached_model:
        source += _CACHED_MODEL_SOURCE.format(cached_prefix=CACHED_RECORDS_PREFIX)
    (tmp_path / f"{module}.py").write_text(source, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "config-demo",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [module], "warn_recommended": False},
            },
        },
    }
    if database is not None:
        config["database"] = dict(database)
    if cache is not None:
        config["cache"] = dict(cache)
    if rest is not None:
        config["app"]["rest"] = rest
    if sql is not None:
        config["sql"] = sql
    if observability is not None:
        config["observability"] = observability
    # Every fixture app opts out of cursor signing keys explicitly.
    config["persistence"] = {"cursors": {"signing": "ephemeral"}, **(persistence or {})}
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)
