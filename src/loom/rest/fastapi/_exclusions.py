"""Startup validation of the authentication exclusion list.

Authentication excludes paths by exact string, but Starlette routes by
*template*: a route declared as ``/{tenant}`` matches ``/metrics`` just as
happily as ``/acme``.  An operator excluding ``/metrics`` would therefore be
serving a business route with no credentials at all, and nothing at request
time would say so.

This module closes that gap where it can still be fixed: at startup, by asking
the router itself which routes each exclusion reaches.

Internal module: consumed by :func:`loom.rest.fastapi.auto.create_app`.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from starlette.applications import Starlette
from starlette.routing import Match

from loom.core.config.errors import ConfigError

_PROBE_METHOD = "GET"


def verify_exclusion_paths(app: Starlette, exclude_paths: Sequence[str]) -> None:
    """Refuse to start when an excluded path reaches a templated route.

    An exclusion is safe only when every route it matches declares that exact
    literal path — the documentation, schema or metrics endpoints it was meant
    for.  A match through a path parameter means the exclusion opens a business
    route, so startup aborts instead of shipping an unauthenticated hole.

    Args:
        app: Application whose routes are already registered.
        exclude_paths: Paths served without authentication.

    Raises:
        ConfigError: When an exclusion matches a route declared with a
            different path template.
    """
    for path in exclude_paths:
        captured = _templated_matches(app, path)
        if not captured:
            continue
        raise ConfigError(
            f"Authentication excludes {path!r}, but that path is served by the "
            f"route{'s' if len(captured) > 1 else ''} {', '.join(repr(r) for r in captured)}: "
            "the exclusion would answer a business route without any credentials. "
            "Remove it from 'exclude_paths', or declare the route under a prefix that "
            "cannot capture it."
        )


def _templated_matches(app: Starlette, path: str) -> list[str]:
    """Return the path templates matching *path* that are not *path* itself."""
    scope = _probe_scope(path)
    matched: list[str] = []
    for route in app.router.routes:
        template = getattr(route, "path", None)
        if template is None or template == path:
            continue
        if route.matches(scope)[0] != Match.NONE:
            matched.append(template)
    return matched


def _probe_scope(path: str) -> dict[str, Any]:
    """Build the minimal ASGI scope ``Route.matches`` needs."""
    return {
        "type": "http",
        "method": _PROBE_METHOD,
        "path": path,
        "root_path": "",
        "headers": [],
    }
