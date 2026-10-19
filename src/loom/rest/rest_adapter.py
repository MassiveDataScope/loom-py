"""Concrete REST transport adapter."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import LoomError
from loom.core.transport.adapter import AdapterRequest
from loom.core.use_case.outcome import Created
from loom.core.use_case.use_case import UseCase
from loom.rest.errors import HttpErrorMapper
from loom.rest.fastapi.response import created_response


class LoomRestAdapter:
    """Concrete REST transport adapter for FastAPI routes.

    Bridges HTTP requests to the ``RuntimeExecutor`` pipeline. Serializes
    ``msgspec.Struct`` results to plain Python primitives and maps
    ``LoomError`` subclasses to ``HTTPException`` instances.

    Does not use ``FastAPI Depends()`` internally — dependencies are resolved
    by the caller (DI container) and passed via ``AdapterRequest.dependencies``.

    Args:
        executor: Configured ``RuntimeExecutor`` shared across all routes.
        error_mapper: Optional custom error mapper. Defaults to
            ``HttpErrorMapper`` with standard status code mapping.

    Example::

        adapter = LoomRestAdapter(executor)

        @router.post("/users")
        async def create_user(body: CreateUserBody) -> dict:
            return await adapter.handle(
                CreateUserUseCase(repo=user_repo),
                AdapterRequest(payload=body.model_dump()),
            )
    """

    def __init__(
        self,
        executor: RuntimeExecutor,
        error_mapper: HttpErrorMapper | None = None,
    ) -> None:
        self._executor = executor
        self._error_mapper = error_mapper or HttpErrorMapper()

    async def handle(
        self,
        use_case: UseCase[Any, Any],
        request: AdapterRequest,
    ) -> Any:
        """Execute the UseCase and return a serializable result.

        Converts ``msgspec.Struct`` results to plain dicts/lists via
        ``msgspec.to_builtins``. For a :class:`~loom.core.use_case.Created`
        result it returns a :class:`~loom.rest.fastapi.response.MsgspecJSONResponse`:
        ``201``, the value as the body, and ``Location`` when it gives one.
        Other results are returned as-is.

        Args:
            use_case: Constructed UseCase instance.
            request: Normalized request carrying params, payload, and
                pre-resolved dependencies.

        Returns:
            Serializable result (plain Python primitives for Struct results),
            or a ``MsgspecJSONResponse`` for a ``Created`` result.

        Raises:
            HTTPException: When the UseCase raises a ``LoomError``.
        """
        try:
            result: Any = await self._executor.execute(
                use_case,
                params=request.params,
                payload=request.payload,
                dependencies=request.dependencies if request.dependencies else None,
            )
        except LoomError as exc:
            raise self._error_mapper.to_http(exc) from exc

        if isinstance(result, Created):
            return created_response(result)
        if isinstance(result, msgspec.Struct):
            return msgspec.to_builtins(result)
        return result
