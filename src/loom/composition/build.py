"""The fixed recipe that composes an application from configuration."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Final, TypeVar, overload

from loom.composition.application import Application
from loom.composition.phases import compose_application
from loom.composition.source import CompositionSource
from loom.core.config import ConfigError, ConfigResolver
from loom.core.environment import Environment

if TYPE_CHECKING:
    from loom.composition.recipe import Adapter
    from loom.core.di.container import LoomContainer

T = TypeVar("T")


@overload
def create_app(
    *config_paths: str,
    adapter: Adapter[T],
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> T: ...


@overload
def create_app(
    *config_paths: str,
    adapter: None = None,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> object: ...


def create_app(
    *config_paths: str,
    adapter: Adapter[object] | None = None,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> object:
    """Compose an application from configuration and mount *adapter*'s transport on it.

    Args:
        *config_paths: YAML files merged left to right; exclusive with *config_dir*.
        adapter: The transport, and how it wants the application composed;
            ``app.entry`` picks it when omitted: ``fastapi``, ``lambda`` or a
            function of ``lambda.functions``.
        config_dir: Directory with ``base.yaml`` and the per-environment overlays.
        environment: Environment of the application; overrides ``LOOM_ENV``.
        code_path: Overrides ``app.code_path``, relative to the first config file.
        resolvers: Resolvers for ``${name:key}`` placeholders, ahead of the
            built-in ``secrets`` and ``ssm``.
        modules: DI modules run before repositories are registered. They may
            bind ``JobService`` and repositories, which the framework then
            leaves alone, but none of the keys it owns (see
            :class:`~loom.composition.errors.ReservedBindingError`).

    Returns:
        What *adapter* binds: an ASGI app, a Celery app, a handler.

    Raises:
        ConfigError: When the configuration is incomplete or invalid.
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
        ReservedBindingError: When a module binds a framework-owned key.
    """
    source = CompositionSource.load(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        code_path=code_path,
        resolvers=resolvers,
        caller="create_app",
    )
    resolved = adapter if adapter is not None else _adapter_from_entry(source)
    application = compose_application(source, resolved, modules=modules)
    return resolved.bind(application)


def _fastapi() -> Adapter[object]:
    from loom.rest.fastapi.adapter import FastAPIAdapter

    return FastAPIAdapter()


def _lambda(function: str | None = None) -> Adapter[object]:
    from loom.serverless.aws.lambda_.handler import LambdaAdapter

    return LambdaAdapter(function=function)


_BUILTIN_ENTRIES: Final[Mapping[str, Callable[[], Adapter[object]]]] = {
    "fastapi": _fastapi,
    "lambda": _lambda,
}


def _adapter_from_entry(source: CompositionSource) -> Adapter[object]:
    entry = source.app.entry
    if entry is None:
        raise ConfigError("create_app needs adapter= or app.entry")
    builtin = _BUILTIN_ENTRIES.get(entry)
    return builtin() if builtin is not None else _lambda(entry)


def build_application(
    *config_paths: str,
    adapter: Adapter[object],
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> Application:
    """Compose the application *adapter* asks for, without mounting a transport.

    Opens nothing: building a backend creates its engine or client, not a
    connection. :meth:`Application.check` and :meth:`Application.running`
    are what reach the outside.

    Args:
        *config_paths: See :func:`create_app`.
        adapter: Whose recipe to compose.
        config_dir: See :func:`create_app`.
        environment: See :func:`create_app`.
        code_path: See :func:`create_app`.
        resolvers: See :func:`create_app`.
        modules: See :func:`create_app`.

    Returns:
        The composed, closed application.

    Raises:
        ConfigError: When the configuration is incomplete or invalid.
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
        ReservedBindingError: When a module binds a framework-owned key.
    """
    source = CompositionSource.load(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        code_path=code_path,
        resolvers=resolvers,
        caller="build_application",
    )
    return compose_application(source, adapter, modules=modules)


def load_source(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    caller: str = "load_source",
) -> CompositionSource:
    """Load the source an application composes from; see :meth:`CompositionSource.load`."""
    return CompositionSource.load(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        code_path=code_path,
        resolvers=resolvers,
        caller=caller,
    )
