from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.repository import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
)
from loom.core.repository import (
    build_repository_registration_module as build_main_repository_module,
)
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.repository.registry import (
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
    repository_for,
)

__all__ = [
    "DynamoDBDefaultRepositoryBuilder",
    "RepositoryRegistration",
    "RepositoryToken",
    "build_dynamodb_repository_registration_module",
    "get_repository_registration",
    "repository_for",
]


@dataclass(frozen=True)
class DynamoDBDefaultRepositoryBuilder:
    """Default repository builder for DynamoDB-backed models.

    A frozen dataclass injected with the shared boto3 ``dynamodb`` client and
    the configured table name. Every model is bound to the same table — the
    simple, config-driven mapping the DynamoDB backend uses. Register it via the
    DI module rather than constructing it directly so the client is shared.

    Args:
        dynamo_client: Shared boto3 low-level ``dynamodb`` client.
        table_name: Name of the DynamoDB table backing every model.
    """

    dynamo_client: Any
    table_name: str
    executor: Executor | None = None

    def __call__(self, context: RepositoryBuildContext) -> Any:
        if not issubclass(context.model, BaseModel):
            raise RuntimeError(
                f"DynamoDBDefaultRepositoryBuilder cannot build a repository "
                f"for non-persistible type {context.model.__qualname__}"
            )
        return RepositoryDynamoDB(
            client=self.dynamo_client,
            table_name=self.table_name,
            model=context.model,
            executor=self.executor,
        )


def build_dynamodb_repository_registration_module(
    dynamo_client: Any,
    table_name: str,
    models: Sequence[type[BaseModel]],
    *,
    logical_models: Sequence[type[Any]] = (),
    executor: Executor | None = None,
) -> Callable[[LoomContainer], None]:
    """Build a DI module that registers DynamoDB repositories and capability bindings.

    Mirrors
    :func:`~loom.core.repository.sqlalchemy.registry.build_sqlalchemy_repository_registration_module`:
    the module self-declares its infrastructure by registering a
    ``DefaultRepositoryBuilder`` bound to the shared boto3 client, so the
    bootstrap needs no knowledge of DynamoDB internals.

    To swap the default builder, register your own ``DefaultRepositoryBuilder``
    in the container *before* loading this module — the module will not
    overwrite an existing registration.

    Args:
        dynamo_client: Shared boto3 low-level ``dynamodb`` client.
        table_name: Name of the DynamoDB table backing every model.
        models: Persistible models to register repositories for.
        logical_models: Extra models with explicit ``repository_for`` registrations.
        executor: Threads the repositories run their blocking client calls on;
            ``None`` uses the event loop's default executor.
    """

    def _registered_builder(
        context: RepositoryBuildContext,
        registration: RepositoryRegistration,
    ) -> Any:
        if registration.builder is not None:
            return registration.builder(context)
        repository_type = registration.repository_type
        if not isinstance(repository_type, type):
            raise RuntimeError(
                "Repository registration for "
                f"{registration.model.__qualname__} must use a class type."
            )
        if issubclass(repository_type, RepositoryDynamoDB):
            return repository_type(
                client=dynamo_client,
                table_name=table_name,
                model=context.model,
                executor=executor,
            )
        return repository_type()

    register = build_main_repository_module(
        models=models,
        explicit_models=logical_models,
        build_registered_repository=_registered_builder,
        default_repository_type=RepositoryDynamoDB,
    )

    def _register_with_client(container: LoomContainer) -> None:
        if not container.is_registered(DefaultRepositoryBuilder):
            container.register_instance(
                DefaultRepositoryBuilder,
                DynamoDBDefaultRepositoryBuilder(
                    dynamo_client=dynamo_client,
                    table_name=table_name,
                    executor=executor,
                ),
            )
        register(container)

    return _register_with_client
