"""Where a scope's value comes from on each request.

A binding is ``identity.<attribute>`` (``subject`` or a verified attribute of
the caller) or ``request.<name>`` (a callable the product registered before
startup). Each application binds its sources when it is composed, so the
registry is only read then. loom never names a scope or a source itself.
"""

from __future__ import annotations

import functools
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


def bind_sources(scope_sources: Mapping[str, str], *, path: str) -> dict[str, ScopeSource]:
    """Return what gives each scope its value; called once, when the application is composed.

    A ``request.<name>`` binding takes the source registered under that name
    now: an application keeps the sources it was composed with, whatever is
    registered or cleared afterwards for another one.

    Args:
        scope_sources: The store's ``schema.scopes``, binding per scope.
        path: Dotted path of the store's section, which the messages name.

    Raises:
        ConfigError: When a binding is malformed or names an unregistered
            source, naming the scope and the binding.
    """
    return {scope: _source(scope, binding, path=path) for scope, binding in scope_sources.items()}


def _source(scope: str, binding: str, *, path: str) -> ScopeSource:
    match = _BINDING.fullmatch(binding)
    if match is None:
        raise ConfigError(
            f"{path}.schema.scopes.{scope} {binding!r} must be identity.<attribute> "
            "or request.<name>"
        )
    kind, name = match.groups()
    if kind == "identity":
        return _subject if name == "subject" else functools.partial(_attribute, name)
    source = _sources.get(name)
    if source is None:
        raise ConfigError(
            f"{path}.schema.scopes.{scope} binds {binding!r} but no scope source "
            f"named {name!r} was registered"
        )
    return source


def _subject() -> str | None:
    return current_identity().subject or None


def _attribute(name: str) -> object | None:
    return current_identity().attribute(name)
