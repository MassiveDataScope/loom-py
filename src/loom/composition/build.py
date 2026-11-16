"""The fixed recipe that composes an application from configuration."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Final, TypeVar, overload

from loom.composition.project import Project
from loom.core.config import ConfigError, ConfigResolver
from loom.core.environment import Environment

if TYPE_CHECKING:
    from loom.composition.recipe import Adapter

T = TypeVar("T")


@overload
def create_app(
    *config_paths: str,
    adapter: Adapter[T],
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> T: ...


@overload
def create_app(
    *config_paths: str,
    adapter: None = None,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> object: ...


def create_app(
    *config_paths: str,
    adapter: Adapter[object] | None = None,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> object:
    """Compose an application from configuration and mount *adapter*'s transport on it.

    Args:
        *config_paths: YAML files merged left to right; exclusive with *config_dir*.
        adapter: The transport, and how it wants the application composed;
            ``app.entry`` picks it when omitted: ``fastapi``, ``lambda`` or a
            function of ``lambda.functions``.
        config_dir: Directory with ``base.yaml`` and the per-environment overlays;
            with no *config_paths* and no *config_dir*, ``LOOM_CONFIG_DIR`` or ``./config``.
        environment: Environment of the application; overrides ``LOOM_ENV``.
        code_path: Overrides ``app.code_path``, relative to the first config file.
        resolvers: Resolvers for ``${name:key}`` placeholders, ahead of the
            built-in ``secrets`` and ``ssm``.

    Returns:
        What *adapter* binds: an ASGI app, a Celery app, a handler.

    Raises:
        ConfigError: When the configuration is incomplete or invalid.
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
        ReservedBindingError: When an installer binds a framework-owned key.
    """
    project = Project.load(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        code_path=code_path,
        resolvers=resolvers,
        caller="create_app",
    )
    return project.serve(adapter if adapter is not None else _adapter_from_entry(project))


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


def _adapter_from_entry(project: Project) -> Adapter[object]:
    entry = project.app.entry
    if entry is None:
        raise ConfigError("create_app needs adapter= or app.entry")
    builtin = _BUILTIN_ENTRIES.get(entry)
    return builtin() if builtin is not None else _lambda(entry)
