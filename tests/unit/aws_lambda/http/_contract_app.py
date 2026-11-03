"""A discoverable application exercising every HTTP outcome the adapters must agree on."""

from __future__ import annotations

import asyncio
import base64
import importlib
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import jwt
import yaml

SECRET = "contract-suite-secret-of-at-least-32-bytes"
AUDIENCE = "loom-api"
PUBLIC_PATH = "/public/whoami"
ORIGIN = "https://app.example"
MAX_BODY_BYTES = 256

_APP_SOURCE = '''\
"""Routes for the HTTP contract suite."""

from __future__ import annotations

import asyncio
from typing import Any

from loom.core.command import Command
from loom.core.errors import NotFound
from loom.core.identity import current_identity
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.markers import Input
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute

SEEN: list[tuple[asyncio.AbstractEventLoop, object]] = []


class Item(BaseModel):
    __tablename__ = "contract_items"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class NewItem(Command, frozen=True):
    name: str


class GetItem(UseCase[Item, dict[str, Any]]):
    async def execute(self, item_id: int) -> dict[str, Any]:
        if item_id == 404:
            raise NotFound("Item", id=item_id)
        return {"id": item_id, "type": type(item_id).__name__}


class CreateItem(UseCase[Item, dict[str, Any]]):
    async def execute(self, cmd: NewItem = Input()) -> dict[str, Any]:
        return {"name": cmd.name}


class Explode(UseCase[Item, str]):
    async def execute(self) -> str:
        raise RuntimeError("the database is on fire")


class AdminOnly(UseCase[Item, str]):
    async def execute(self) -> str:
        return "granted"


class AdminLevel(UseCase[Item, int]):
    async def execute(self, level: int) -> int:
        return level


class WhoAmI(UseCase[Item, dict[str, Any]]):
    async def execute(self) -> dict[str, Any]:
        SEEN.append((asyncio.get_running_loop(), self.main_repo.session_manager.active_session))
        return {"subject": current_identity().subject}


class ItemInterface(RestInterface[Item]):
    prefix = "/items"
    routes = (
        RestRoute(use_case=GetItem, method="GET", path="/{item_id}"),
        RestRoute(use_case=CreateItem, method="POST", path="/"),
    )


class OpsInterface(RestInterface[Item]):
    prefix = "/ops"
    routes = (
        RestRoute(use_case=Explode, method="GET", path="/explode"),
        RestRoute(use_case=AdminOnly, method="GET", path="/admin", requires_roles=("admin",)),
        RestRoute(
            use_case=AdminLevel, method="GET", path="/admin/{level}", requires_roles=("admin",)
        ),
        RestRoute(use_case=WhoAmI, method="POST", path="/whoami"),
    )


class PublicInterface(RestInterface[Item]):
    prefix = "/public"
    routes = (RestRoute(use_case=WhoAmI, method="POST", path="/whoami"),)
'''

_HEALTH_ROUTE_SOURCE = """

class HealthLikeInterface(RestInterface[Item]):
    prefix = "/health"
    routes = (RestRoute(use_case=WhoAmI, method="POST", path=""),)
"""


def _module_of(directory: Path) -> str:
    # One module per project: the interpreter caches imports by name.
    return f"loom_contract_app_{directory.name}"


def _discovery(directory: Path, module: str, agents: tuple[str, ...]) -> dict[str, Any]:
    """Discover *module*'s interfaces, or a manifest of them declaring *agents*."""
    if not agents:
        return {
            "mode": "interfaces",
            "interfaces": {"modules": [module], "warn_recommended": False},
        }
    manifest = f"{module}_manifest"
    (directory / f"{manifest}.py").write_text(
        f"from {module} import CreateItem, GetItem, Item, ItemInterface\n\n"
        "MODELS = [Item]\n"
        "USE_CASES = [GetItem, CreateItem]\n"
        "INTERFACES = [ItemInterface]\n"
        f"AGENTS = {list(agents)!r}\n",
        encoding="utf-8",
    )
    return {"mode": "manifest", "manifest": {"module": manifest}}


def write_contract_project(
    directory: Path,
    *,
    agents: tuple[str, ...] = (),
    exclude_paths: tuple[str, ...] | None = (PUBLIC_PATH,),
    health_route: bool = False,
    **sections: Any,
) -> str:
    """Write the contract module and its YAML config into *directory*; return the config path.

    Args:
        directory: Where the project lives.
        exclude_paths: Paths served without a token; ``None`` leaves the
            default exclusions.
        health_route: Also serve a business route at the literal ``/health``.
        agents: Agent artifacts a manifest declares; without them, the
            interfaces are discovered from the module.
        **sections: Extra top-level configuration sections.
    """
    module = _module_of(directory)
    source = _APP_SOURCE + (_HEALTH_ROUTE_SOURCE if health_route else "")
    (directory / f"{module}.py").write_text(source, encoding="utf-8")
    secret_path = directory / "jwt.key"
    secret_path.write_text(SECRET, encoding="utf-8")
    exclusions = {} if exclude_paths is None else {"exclude_paths": list(exclude_paths)}
    config: dict[str, Any] = {
        "app": {
            "name": "contract",
            "code_path": ".",
            "discovery": _discovery(directory, module, agents),
            "rest": {
                "docs_url": None,
                "redoc_url": None,
                "openapi_url": None,
                "max_body_bytes": MAX_BODY_BYTES,
                "auth": {
                    "jwt": {
                        "secret_path": str(secret_path),
                        "algorithms": ["HS256"],
                        "audience": AUDIENCE,
                        "roles_claim": "roles",
                        **exclusions,
                    }
                },
                "cors": {
                    "allow_origins": [ORIGIN],
                    "allow_methods": ["GET", "POST"],
                    "allow_headers": ["authorization", "content-type"],
                    "expose_headers": ["x-request-id"],
                    "max_age": 600,
                },
            },
        },
        "stores": {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}},
        "persistence": {"cursors": {"signing": "ephemeral"}},
        **sections,
    }
    config_path = directory / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def seen_by(config_path: str) -> list[tuple[asyncio.AbstractEventLoop, object]]:
    """The loop and active session of every ``/ops/whoami`` the project at *config_path* served."""
    module = importlib.import_module(_module_of(Path(config_path).parent))
    return list(module.SEEN)


def bearer(subject: str, *roles: str) -> dict[str, str]:
    """The ``Authorization`` header of a valid token for *subject* holding *roles*."""
    claims = {"sub": subject, "aud": AUDIENCE, "roles": list(roles), "exp": int(time.time()) + 600}
    return {"authorization": f"Bearer {jwt.encode(claims, SECRET, algorithm='HS256')}"}


def http_api_event(
    method: str, target: str, headers: Mapping[str, str], body: bytes = b""
) -> dict[str, Any]:
    """An API Gateway HTTP API (payload 2.0) event for *target*, a path and query string.

    API Gateway forwards the ``content-length`` of a request with a body.
    """
    length = {"content-length": str(len(body))} if body else {}
    path, _, query = target.partition("?")
    return {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": path,
        "rawQueryString": query,
        "headers": {"host": "api.example", **length, **headers},
        "requestContext": {
            "accountId": "123456789012",
            "apiId": "contract",
            "domainName": "api.example",
            "http": {
                "method": method,
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": "203.0.113.7",
                "userAgent": "pytest",
            },
            "requestId": "contract-request",
            "routeKey": "$default",
            "stage": "$default",
        },
        "body": base64.b64encode(body).decode("ascii"),
        "isBase64Encoded": True,
    }
