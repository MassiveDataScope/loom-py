"""Concrete REST transport adapter."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import LoomError
from loom.core.identity import current_identity
from loom.core.transport.adapter import AdapterRequest
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.outcome import Created
from loom.core.use_case.use_case import UseCase
from loom.rest.errors import HttpErrorMapper
from loom.rest.fastapi.response import created_response


class LoomRestAdapter:
    """Concrete REST transport adapter for FastAPI routes.

    Bridges HTTP requests to the ``RuntimeExecutor`` pipeline: each call
    builds the use case class inside its execution through ``factory`` (so
    its constructor ports, ``Caller()`` and per-execution adapters are
    resolved there), as the request's caller. Serializes ``msgspec.Struct``
    results to plain Python primitives and maps ``LoomError`` subclasses to
    ``HTTPException`` instances.

    Args:
        executor: Configured ``RuntimeExecutor`` shared across all routes.
        factory: Builds each use case with its dependencies from the container.
        error_mapper: Optional custom error mapper. Defaults to
            ``HttpErrorMapper`` with standard status code mapping.

    Example::

        adapter = LoomRestAdapter(kernel.executor, factory=kernel.factory)

        @router.post("/users")
        async def create_user(body: CreateUserBody) -> dict:
            return await adapter.handle(
                CreateUserUseCase,
                AdapterRequest(params={}, payload=body.model_dump()),
            )
    """

    def __init__(
        self,
        executor: RuntimeExecutor,
        *,
        factory: UseCaseFactory,
        error_mapper: HttpErrorMapper | None = None,
    ) -> None:
        self._executor = executor
        self._factory = factory
        self._error_mapper = error_mapper or HttpErrorMapper()

    async def handle(
        self,
        use_case: type[UseCase[Any, Any]],
        request: AdapterRequest,
    ) -> Any:
        """Build and execute the UseCase and return a serializable result.

        Runs as the request's caller: the ambient
        :func:`~loom.core.identity.current_identity` the transport installed,
        read once and passed to the executor.

        Converts ``msgspec.Struct`` results to plain dicts/lists via
        ``msgspec.to_builtins``. For a :class:`~loom.core.use_case.Created`
        result it returns a :class:`~loom.rest.fastapi.response.MsgspecJSONResponse`:
        ``201``, the value as the body, and ``Location`` when it gives one.
        Other results are returned as-is.

        Args:
            use_case: UseCase class to build and run.
            request: Normalized request carrying params and payload.

        Returns:
            Serializable result (plain Python primitives for Struct results),
            or a ``MsgspecJSONResponse`` for a ``Created`` result.

        Raises:
            HTTPException: When the UseCase raises a ``LoomError``.
        """
        try:
            result: Any = await self._executor.run(
                use_case,
                factory=self._factory,
                params=request.params,
                payload=request.payload,
                identity=current_identity(),
            )
        except LoomError as exc:
            raise self._error_mapper.to_http(exc) from exc

        if isinstance(result, Created):
            return created_response(result)
        if isinstance(result, msgspec.Struct):
            return msgspec.to_builtins(result)
        return result
