"""Process and StreamFlow definitions for the streaming DSL."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, Generic, TypeAlias, TypeVar

from loom.core.config import ConfigBinding
from loom.core.errors import InvalidType, InvalidValue
from loom.core.model import LoomFrozenStruct
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._boundary import FromMultiTypeTopic, FromTopic, IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork
from loom.streaming.nodes._mongo import FromMongoCDC
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach
from loom.streaming.nodes._sink import IntoSink
from loom.streaming.nodes._step import Step
from loom.streaming.nodes._with import With, WithAsync

ProcessNode: TypeAlias = (
    ConfigBinding
    | Step[Any, Any]
    | type[Step[Any, Any]]
    | With[Any, Any]
    | WithAsync[Any, Any]
    | CollectBatch
    | ForEach
    | Drain
    | IntoTopic[Any]
    | IntoSink[Any]
    | Fork[Any]
    | Router[Any, Any]
    | Broadcast[Any]
    | ExpandRoutes[Any]
)

InT = TypeVar("InT", bound=StreamPayload, contravariant=True)
OutT = TypeVar("OutT", bound=StreamPayload, covariant=True)


class Process(Generic[InT, OutT]):
    """Ordered collection of streaming nodes.

    Args:
        *nodes: One or more nodes to execute in order.
    """

    __slots__ = ("_nodes",)

    def __init__(self, *nodes: ProcessNode) -> None:
        if not nodes:
            raise InvalidValue("Process must contain at least one node.")
        self._nodes = nodes

    def __iter__(self) -> Iterator[ProcessNode]:
        return iter(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    @property
    def nodes(self) -> tuple[ProcessNode, ...]:
        """Immutable sequence of process nodes."""
        return self._nodes


class ErrorRoute(LoomFrozenStruct, frozen=True):
    """Route a group of error kinds to one output topic.

    Args:
        kinds: Error kinds handled by this route.
        output: Kafka boundary that receives the routed envelopes.
    """

    kinds: tuple[ErrorKind, ...]
    output: IntoTopic[StreamPayload]


ErrorRoutes: TypeAlias = (
    IntoTopic[StreamPayload]
    | ErrorRoute
    | Sequence[ErrorRoute]
    | Mapping[ErrorKind, IntoTopic[StreamPayload]]
    | None
)


class StreamFlow(Generic[InT, OutT]):
    """Complete streaming flow declaration.

    Args:
        name: Unique flow identifier.
        source: Input topic declaration.
        process: Ordered sequence of transformation nodes.
        output: Optional terminal output topic.
        errors: Optional error-route declaration.  Pass one ``IntoTopic`` to
            route every error kind to the same topic, or use ``ErrorRoute``
            groups / explicit mappings for more specific routing.
    """

    __slots__ = ("_errors", "_name", "_output", "_process", "_source")

    def __init__(
        self,
        name: str,
        source: FromTopic[InT] | FromMultiTypeTopic[InT] | FromMongoCDC[InT],
        process: Process[InT, OutT],
        output: IntoTopic[OutT] | None = None,
        errors: ErrorRoutes = None,
    ) -> None:
        if not name:
            raise InvalidValue("StreamFlow.name must not be empty.")
        self._name = name
        self._source = source
        self._process = process
        self._output = output
        self._errors = _normalize_error_routes(errors)

    @property
    def errors(self) -> Mapping[ErrorKind, IntoTopic[StreamPayload]]:
        """Explicit error routes keyed by error kind."""
        return MappingProxyType(self._errors)

    @property
    def name(self) -> str:
        """Flow identifier."""
        return self._name

    @property
    def output(self) -> IntoTopic[OutT] | None:
        """Terminal output topic, if any."""
        return self._output

    @property
    def process(self) -> Process[InT, OutT]:
        """Ordered node sequence."""
        return self._process

    @property
    def source(self) -> FromTopic[InT] | FromMultiTypeTopic[InT] | FromMongoCDC[InT]:
        """Input topic declaration."""
        return self._source


def _normalize_error_routes(
    errors: ErrorRoutes,
) -> dict[ErrorKind, IntoTopic[StreamPayload]]:
    if errors is None:
        return {}
    if isinstance(errors, IntoTopic):
        return dict.fromkeys(ErrorKind, errors)
    if isinstance(errors, ErrorRoute):
        return _routes_from_error_route(errors)
    if isinstance(errors, Sequence):
        if isinstance(errors, (str, bytes, bytearray)):
            raise InvalidType("StreamFlow.errors must be an error route declaration, not text.")
        if not errors:
            raise InvalidValue("StreamFlow.errors must not be an empty sequence.")
        normalized: dict[ErrorKind, IntoTopic[StreamPayload]] = {}
        for route in errors:
            _merge_error_route(normalized, route)
        return normalized
    return dict(errors)


def _routes_from_error_route(route: ErrorRoute) -> dict[ErrorKind, IntoTopic[StreamPayload]]:
    normalized: dict[ErrorKind, IntoTopic[StreamPayload]] = {}
    _merge_error_route(normalized, route)
    return normalized


def _merge_error_route(
    normalized: dict[ErrorKind, IntoTopic[StreamPayload]],
    route: ErrorRoute,
) -> None:
    if not route.kinds:
        raise InvalidValue("ErrorRoute.kinds must not be empty.")
    for kind in route.kinds:
        if kind in normalized and normalized[kind] is not route.output:
            raise InvalidValue(f"error kind {kind.value} is declared more than once")
        normalized[kind] = route.output
