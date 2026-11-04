"""``lambda.functions``: several Lambdas served from one configuration.

``app.entry`` (or ``LOOM__APP__ENTRY``) names the function a deployment
serves. An HTTP function serves the interfaces it lists, a Python one by its
class name and a YAML one by its ``app.rest.interfaces`` key; an SQS function
serves the jobs.
"""

from __future__ import annotations

import dataclasses
import functools
import warnings
from collections.abc import Mapping
from typing import TYPE_CHECKING, Annotated, Any

import msgspec

from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.config.loader import convert_section

if TYPE_CHECKING:
    from loom.rest.fastapi.adapter import InterfaceSelector
    from loom.rest.model import RestInterface

_SECTION = "lambda.functions"


class LambdaFunction(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One function: the interfaces it serves over HTTP, or ``sqs: true``.

    ``auth`` has the ``app.rest.auth`` schema and replaces it for the function.
    An sqs function declares its event source mapping for the deployment:
    ``batch_size`` (at most 10 on a FIFO queue), ``batching_window_s`` and
    ``max_concurrency``; loom applies only ``report_batch_item_failures``,
    which ``false`` turns into a failed invocation instead of a partial batch.
    """

    http: tuple[str, ...] = ()
    sqs: bool = False
    auth: dict[str, Any] | None = None
    batch_size: Annotated[int, msgspec.Meta(ge=1, le=10000)] | None = None
    batching_window_s: Annotated[int, msgspec.Meta(ge=0, le=300)] | None = None
    max_concurrency: Annotated[int, msgspec.Meta(ge=2, le=1000)] | None = None
    report_batch_item_failures: bool | None = None


class _LambdaConfig(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    functions: dict[str, LambdaFunction] = msgspec.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class HttpFunction:
    """What an HTTP function changes in the REST composition; nothing by default."""

    auth_section: str | None = None
    select: InterfaceSelector | None = None


def declared_functions(ctx: ConfigContext) -> dict[str, LambdaFunction]:
    """Decode ``lambda.functions`` and refuse a function or an interface declared twice over.

    Raises:
        ConfigError: When a function declares both or neither of ``http`` and
            ``sqs: true``, ``auth`` on an sqs function, a batching setting on
            an http one, a ``batch_size`` over 10 without a ``batching_window_s``,
            or an interface another function already serves.
    """
    functions = _unresolved(ctx).functions
    owners: dict[str, str] = {}
    for name, function in functions.items():
        if bool(function.http) == function.sqs:
            raise ConfigError(f"{_SECTION}.{name} declares exactly one of 'http' or 'sqs: true'")
        if function.auth is not None and function.sqs:
            raise ConfigError(f"{_SECTION}.{name}.auth applies to an http function only")
        _check_batching(name, function)
        for interface in function.http:
            owner = owners.setdefault(interface, name)
            if owner != name:
                raise ConfigError(
                    f"{interface!r} is served by both {_SECTION}.{owner} and {_SECTION}.{name}"
                )
    return functions


_BATCHING = ("batch_size", "batching_window_s", "max_concurrency", "report_batch_item_failures")


def _check_batching(name: str, function: LambdaFunction) -> None:
    if not function.sqs:
        for key in _BATCHING:
            if getattr(function, key) is not None:
                raise ConfigError(f"{_SECTION}.{name}.{key} applies to an sqs function only")
    # AWS: a batch size over 10 needs MaximumBatchingWindowInSeconds of at least 1.
    if (function.batch_size or 0) > 10 and not function.batching_window_s:
        raise ConfigError(f"{_SECTION}.{name}.batch_size over 10 needs batching_window_s >= 1")


def _unresolved(ctx: ConfigContext) -> _LambdaConfig:
    """``lambda`` as written: another function's ``auth`` may name secrets this one cannot read.

    Only the function served resolves its own ``auth``, through its section path.
    """
    raw = ctx.raw(ConfigKey.LAMBDA)
    return _LambdaConfig() if raw is None else convert_section(raw, ConfigKey.LAMBDA, _LambdaConfig)


def declared_function(ctx: ConfigContext, name: str) -> LambdaFunction:
    """The function *name* of ``lambda.functions``.

    Raises:
        ConfigError: When no function is called *name*, or ``lambda.functions``
            is misdeclared.
    """
    return _function(declared_functions(ctx), name)


def http_function(ctx: ConfigContext, name: str) -> HttpFunction:
    """The authentication and interfaces of the HTTP function *name*.

    Raises:
        ConfigError: When no HTTP function is called *name*, or
            ``lambda.functions`` is misdeclared.
    """
    functions = declared_functions(ctx)
    function = _function(functions, name)
    if function.sqs:
        raise ConfigError(f"{_SECTION}.{name} is an sqs function, not an http one")
    return HttpFunction(
        auth_section=None if function.auth is None else f"{_SECTION}.{name}.auth",
        select=functools.partial(_served_by, functions, name),
    )


def entry_function(ctx: ConfigContext, entry: str | None) -> HttpFunction:
    """The HTTP function ``app.entry`` names, or nothing when it names none.

    Raises:
        ConfigError: When *entry* names an sqs function, or
            ``lambda.functions`` is misdeclared.
    """
    if entry is None or entry not in declared_functions(ctx):
        return HttpFunction()
    return http_function(ctx, entry)


def reports_batch_item_failures(ctx: ConfigContext, entry: str | None) -> bool:
    """Whether the function ``app.entry`` names reports batch item failures; true by default.

    Raises:
        ConfigError: When ``lambda.functions`` is misdeclared.
    """
    function = None if entry is None else declared_functions(ctx).get(entry)
    return function is None or function.report_batch_item_failures is not False


def _function(functions: Mapping[str, LambdaFunction], name: str) -> LambdaFunction:
    function = functions.get(name)
    if function is None:
        declared = ", ".join(functions) or "none"
        raise ConfigError(f"{_SECTION} declares no function {name!r} (declared: {declared})")
    return function


def _served_by(
    functions: Mapping[str, LambdaFunction],
    name: str,
    named: Mapping[str, tuple[type[RestInterface[Any]], ...]],
) -> tuple[type[RestInterface[Any]], ...]:
    for owner, function in functions.items():
        for interface in function.http:
            _check_reference(f"{_SECTION}.{owner}.http", interface, named.get(interface, ()))
    listed = {interface for function in functions.values() for interface in function.http}
    unserved = sorted(set(named) - listed)
    if unserved:
        warnings.warn(
            f"No function of {_SECTION} serves {', '.join(unserved)}", UserWarning, stacklevel=2
        )
    return tuple(named[interface][0] for interface in functions[name].http)


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
