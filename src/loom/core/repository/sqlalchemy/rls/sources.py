"""Where a scope's value comes from on each request.

A binding is ``identity.<attribute>`` (``subject`` or a verified attribute of
the caller) or ``request.<name>`` (a callable the product registered before
startup). loom never names a scope or a source itself.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping

from loom.core.config import ConfigError
from loom.core.identity import current_identity

ScopeSource = Callable[[], object | None]

_BINDING = re.compile(r"^(identity|request)\.([A-Za-z_]\w*)$")
_sources: dict[str, ScopeSource] = {}


def register_scope_source(name: str, source: ScopeSource) -> None:
    """Publish ``request.<name>``; registering a name twice is an error."""
    if name in _sources:
        raise ValueError(f"scope source {name!r} is already registered")
    _sources[name] = source


def clear_scope_sources() -> None:
    """Forget every registered source; for tests."""
    _sources.clear()


def validate_bindings(scope_sources: Mapping[str, str]) -> None:
    """Fail at startup when a binding is malformed or names an unregistered source.

    Raises:
        ConfigError: Naming the scope and the binding.
    """
    for scope, binding in scope_sources.items():
        kind, name = _parse(scope, binding)
        if kind == "request" and name not in _sources:
            raise ConfigError(
                f"database.schema.scopes.{scope} binds {binding!r} but no scope source "
                f"named {name!r} was registered"
            )


def resolve_binding(binding: str) -> object | None:
    """Return the current value of ``binding`` or ``None`` when there is none."""
    kind, name = _parse("", binding)
    if kind == "identity":
        identity = current_identity()
        if name == "subject":
            return identity.subject or None
        return identity.attribute(name)
    source = _sources.get(name)
    return source() if source is not None else None


def _parse(scope: str, binding: str) -> tuple[str, str]:
    match = _BINDING.fullmatch(binding)
    if match is None:
        raise ConfigError(
            f"database.schema.scopes.{scope} {binding!r} must be identity.<attribute> "
            "or request.<name>"
        )
    return match.group(1), match.group(2)
