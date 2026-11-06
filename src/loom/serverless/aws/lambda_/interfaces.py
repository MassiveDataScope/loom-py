"""The interfaces and authentication of an ``http`` function of ``lambda.functions``."""

from __future__ import annotations

import dataclasses
import functools
import warnings
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from loom.core.config import ConfigContext, ConfigError
from loom.serverless.aws.lambda_.functions import (
    HttpTrigger,
    LambdaFunction,
    declared_lambda,
    named_function,
    source_of,
)

if TYPE_CHECKING:
    from loom.rest.fastapi.adapter import InterfaceSelector
    from loom.rest.model import RestInterface

_SECTION = "lambda.functions"


@dataclasses.dataclass(frozen=True)
class HttpFunction:
    """What an HTTP function changes in the REST composition; nothing by default."""

    auth_section: str | None = None
    select: InterfaceSelector | None = None


def http_function(
    ctx: ConfigContext, name: str | None, functions: Mapping[str, LambdaFunction] | None = None
) -> HttpFunction:
    """The authentication and interfaces of the HTTP function *name*, of *functions* if decoded.

    Every interface, with ``app.rest.auth``, when *name* is ``None``.

    Raises:
        ConfigError: When no HTTP function is called *name*, or
            ``lambda.functions`` is misdeclared.
    """
    if name is None:
        return HttpFunction()
    functions = declared_lambda(ctx).functions if functions is None else functions
    trigger = named_function(functions, name).trigger
    if not isinstance(trigger, HttpTrigger):
        raise ConfigError(f"{_SECTION}.{name} is a {source_of(trigger)} function, not an http one")
    return HttpFunction(
        auth_section=None if trigger.auth is None else f"{_SECTION}.{name}.trigger.auth",
        select=functools.partial(_served_by, functions, name),
    )


def _served_by(
    functions: Mapping[str, LambdaFunction],
    name: str,
    named: Mapping[str, tuple[type[RestInterface[Any]], ...]],
) -> tuple[type[RestInterface[Any]], ...]:
    listed = {
        owner: function.trigger.interfaces
        for owner, function in functions.items()
        if isinstance(function.trigger, HttpTrigger)
    }
    for owner, interfaces in listed.items():
        for interface in interfaces:
            where = f"{_SECTION}.{owner}.trigger.interfaces"
            _check_reference(where, interface, named.get(interface, ()))
    unserved = sorted(set(named) - {interface for found in listed.values() for interface in found})
    if unserved:
        warnings.warn(
            f"No function of {_SECTION} serves {', '.join(unserved)}", UserWarning, stacklevel=2
        )
    return tuple(named[interface][0] for interface in listed[name])


def _check_reference(
    where: str, interface: str, found: tuple[type[RestInterface[Any]], ...]
) -> None:
    if not found:
        raise ConfigError(
            f"{where} names {interface!r}: no discovered RestInterface "
            "and no app.rest.interfaces key is called so"
        )
    if len(found) > 1:
        origins = ", ".join(sorted(f"{cls.__module__}.{cls.__qualname__}" for cls in found))
        raise ConfigError(f"{where} names {interface!r}, which is ambiguous: {origins}")
