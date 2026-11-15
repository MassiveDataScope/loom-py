from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from loom.core.use_case.use_case import UseCase


@dataclass(frozen=True)
class AdapterRequest:
    """Normalized, transport-agnostic request for UseCase execution.

    Carries the parsed inputs that a transport layer (REST, CLI, gRPC, etc.)
    extracts from its native request format before handing off to a
    ``LoomAdapter`` implementation. What the use case is built with comes
    from the container, never from the request.

    Args:
        params: Primitive parameter values keyed by name (e.g. path/query vars).
        payload: Raw dict for command construction via ``Input()``. ``None``
            when the UseCase declares no ``Input()`` marker.

    Example::

        request = AdapterRequest(
            params={"user_id": 1},
            payload={"email": "new@corp.com"},
        )
    """

    params: dict[str, Any]
    payload: dict[str, Any] | None = None


@runtime_checkable
class LoomAdapter(Protocol):
    """Port for transport adapters.

    Any transport layer (REST, CLI, event bus) that drives UseCase execution
    must implement this protocol. This keeps the engine decoupled from
    transport concerns.

    The adapter is responsible for:
    - Constructing an ``AdapterRequest`` from the native request format.
    - Running the use case class inside one execution, as the request's
      caller, so loom builds it there with its dependencies.
    - Serializing the result into the native response format.
    - Mapping domain errors to transport-specific error responses.

    Example::

        class MyCliAdapter:
            async def handle(
                self,
                use_case: type[UseCase[Any, Any]],
                request: AdapterRequest,
            ) -> Any:
                ...
    """

    async def handle(
        self,
        use_case: type[UseCase[Any, Any]],
        request: AdapterRequest,
    ) -> Any:
        """Build and execute the UseCase and return a transport-ready result.

        Args:
            use_case: UseCase class to build and run.
            request: Normalized request data.

        Returns:
            Transport-ready result (serializable form).
        """
        ...
