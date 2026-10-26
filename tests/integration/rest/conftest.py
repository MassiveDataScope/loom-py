"""Shared builders for the ``app.rest.interfaces`` integration suite.

Every test drives :func:`loom.rest.fastapi.auto.create_app` end to end: real
discovery, a real :class:`~loom.core.engine.compiler.UseCaseCompiler`, a real
:class:`~loom.rest.compiler.RestInterfaceCompiler`, a real ``FastAPI`` app
over an in-memory SQLite database. There is no edge to fake here the way the
AI pillar fakes the model and the network — route mounting and CRUD
generation are exactly what this suite exists to prove.

One project is written per test (unique module name derived from
``tmp_path``): the interpreter caches imports by name, so a shared module
name would serve the first test's classes to every later one.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

_MODULE_PREFIX = "loom_yaml_rest_fixture"
_NON_IDENTIFIER = re.compile(r"\W", re.ASCII)

# ``Ticket`` is exposed by a Python RestInterface at "/tickets-py" — the
# fixed side every parity and collision test compares configuration against.
# ``Gadget`` carries no Python interface: it exists purely for
# app.rest.interfaces tests (explicit routes and auto-CRUD) to reference.
_APP_SOURCE = '''\
"""Discoverable fixture app for the app.rest.interfaces integration suite."""

from __future__ import annotations

from typing import Any

from loom.core.errors import NotFound
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class Ticket(BaseModel):
    __tablename__ = "{table_prefix}_tickets"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    title: str = ColumnField(length=80)


class CreateTicketUseCase(UseCase[Ticket, Ticket]):
    async def execute(self, title: str, **kwargs: Any) -> Ticket:
        return await self.main_repo.create(Ticket(title=title))


class GetTicketUseCase(UseCase[Ticket, Ticket]):
    async def execute(self, ticket_id: int, **kwargs: Any) -> Ticket:
        found = await self.main_repo.get_by_id(ticket_id)
        if found is None:
            raise NotFound(f"Ticket {{ticket_id}} not found")
        return found


class TicketRestInterface(RestInterface[Ticket]):
    prefix = "/tickets-py"
    tags = ("Tickets",)
    routes = (
        RestRoute(use_case=CreateTicketUseCase, method="POST", path="/", status_code=201),
        RestRoute(use_case=GetTicketUseCase, method="GET", path="/{{ticket_id}}"),
    )


class Gadget(BaseModel):
    __tablename__ = "{table_prefix}_gadgets"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=60)
'''


def module_name(tmp_path: Path) -> str:
    """Return the fixture module's importable name for *tmp_path*.

    Deterministic from *tmp_path* alone, so a test can build ``module:Symbol``
    references before calling :func:`write_project`.
    """
    return f"{_MODULE_PREFIX}_{_NON_IDENTIFIER.sub('_', tmp_path.name)}"


def write_project(
    tmp_path: Path,
    *,
    rest: dict[str, Any] | None = None,
    extra_source: str = "",
) -> tuple[str, str]:
    """Write the fixture module plus a YAML config; return (config_path, module).

    Args:
        tmp_path: Directory owning the generated project.
        rest: Contents of the ``app.rest`` section (``interfaces``,
            ``disable_routes``, ...). Build its ``module:Symbol`` references
            with :func:`module_name` ahead of this call.
        extra_source: Extra Python source appended after the base fixture
            module, for a test that needs one more class (e.g. a second
            Python ``RestInterface`` to compare against a YAML one).

    Returns:
        The written config path, and the fixture module's importable name.
    """
    module = module_name(tmp_path)
    table_prefix = _NON_IDENTIFIER.sub("_", tmp_path.name)
    source = _APP_SOURCE.format(table_prefix=table_prefix) + extra_source
    (tmp_path / f"{module}.py").write_text(source, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "yaml-rest-demo",
            "code_path": ".",
            "discovery": {"mode": "modules", "modules": {"include": [module]}},
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    if rest is not None:
        config["app"]["rest"] = rest
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path), module
