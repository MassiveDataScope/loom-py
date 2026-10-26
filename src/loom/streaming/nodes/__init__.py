"""Streaming DSL node primitives.

These are the building blocks used to declare a
:class:`~loom.streaming.graph.Process`.

Families:
    * Boundary nodes: ``FromTopic``, ``FromMongoCDC`` and ``IntoTopic``.
    * Shape adapters: ``CollectBatch``, ``ForEach`` and ``Drain``.
    * Record steps: ``Step`` subclasses declared declaratively.
    * Wrappers: ``With`` and ``WithAsync``.
    * Routing nodes: ``Router`` and ``Fork``.
    * Fan-out nodes: ``Broadcast``.
"""

from loom.streaming.nodes._boundary import (
    FromTopic,
    IntoTopic,
    PartitionGuarantee,
    PartitionPolicy,
    PartitionStrategy,
)
from loom.streaming.nodes._decompose import Explode, PayloadExpander
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork, ForkRoute
from loom.streaming.nodes._mongo import FromMongoCDC
from loom.streaming.nodes._protocols import Predicate, Selector
from loom.streaming.nodes._router import Route, Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape, WindowStrategy
from loom.streaming.nodes._sink import IntoSink, SinkPartition
from loom.streaming.nodes._step import (
    BatchExpandStep,
    BatchStep,
    ExpandStep,
    RecordStep,
    ResourceFactory,
    Step,
    StepContext,
)
from loom.streaming.nodes._table import (
    Backend,
    DeltaSinkConfig,
    IntoTable,
    SqlAlchemySinkConfig,
)
from loom.streaming.nodes._with import (
    AsyncContextDependency,
    ContextDependency,
    ContextFactory,
    ResourceScope,
    SyncContextDependency,
    With,
    WithAsync,
)
from loom.streaming.nodes.refs import msg as msg
from loom.streaming.nodes.refs import payload as payload

__all__ = [
    "AsyncContextDependency",
    "Backend",
    "BatchExpandStep",
    "BatchStep",
    "CollectBatch",
    "ContextDependency",
    "ContextFactory",
    "DeltaSinkConfig",
    "ExpandRoutes",
    "Explode",
    "Drain",
    "PayloadExpander",
    "ExpandStep",
    "ForEach",
    "Fork",
    "ForkRoute",
    "FromMongoCDC",
    "FromTopic",
    "IntoSink",
    "IntoTable",
    "IntoTopic",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
    "Predicate",
    "RecordStep",
    "ResourceFactory",
    "ResourceScope",
    "Route",
    "Router",
    "Selector",
    "SinkPartition",
    "StreamShape",
    "SyncContextDependency",
    "Step",
    "StepContext",
    "SqlAlchemySinkConfig",
    "WindowStrategy",
    "With",
    "WithAsync",
    "evaluate_predicate",
    "msg",
    "payload",
    "select_value",
]
