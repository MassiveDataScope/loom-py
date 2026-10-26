"""Loom Streaming — topic-oriented declarations and transport adapters.

Public authoring API
--------------------

Use :mod:`loom.streaming` for user-facing flow declarations::

    from loom.streaming import (
        CollectBatch,
        ErrorKind,
        FromTopic,
        IntoTopic,
        Message,
        StreamShape,
    )

Compilation types (plan, structured errors) live under
:mod:`loom.streaming.compiler`::

    from loom.streaming.compiler import (
        CompilationError,
        CompilationIssue,
        CompiledPlan,
        StreamingErrorCode,
        compile_flow,
    )

Kafka-specific codecs, clients, and transport settings live under
:mod:`loom.streaming.kafka`.
"""

from loom.core.model import JsonStr
from loom.streaming.compiler import compile_flow
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind, ErrorMessage, ErrorMessageMeta
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.graph._flow import ErrorRoute, Process, ProcessNode, StreamFlow
from loom.streaming.mongo import (
    MongoBsonTimestamp,
    MongoCDCEvent,
    MongoCDCNamespace,
    MongoConfig,
    MongoDBRef,
    MongoObjectId,
    MongoSourceConfig,
    build_mongo_cdc_event,
    build_mongo_cdc_message,
    normalize_bson_value,
)
from loom.streaming.nodes._boundary import (
    FromMultiTypeTopic,
    FromTopic,
    IntoTopic,
    PartitionGuarantee,
    PartitionPolicy,
    PartitionStrategy,
)
from loom.streaming.nodes._broadcast import Broadcast, BroadcastRoute
from loom.streaming.nodes._decompose import Explode, PayloadExpander
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork, ForkRoute
from loom.streaming.nodes._mongo import FromMongoCDC
from loom.streaming.nodes._protocols import Predicate, Selector
from loom.streaming.nodes._router import Route, Router
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
    "Broadcast",
    "BroadcastRoute",
    "CollectBatch",
    "ContextFactory",
    "DeltaSinkConfig",
    "ExpandRoutes",
    "Explode",
    "Drain",
    "PayloadExpander",
    "ErrorEnvelope",
    "ErrorKind",
    "ErrorMessage",
    "ErrorMessageMeta",
    "ErrorRoute",
    "ExpandStep",
    "ForEach",
    "Fork",
    "ForkRoute",
    "FromMongoCDC",
    "FromMultiTypeTopic",
    "FromTopic",
    "IntoSink",
    "IntoTable",
    "JsonStr",
    "IntoTopic",
    "Message",
    "MessageMeta",
    "MongoConfig",
    "MongoBsonTimestamp",
    "MongoCDCEvent",
    "MongoCDCNamespace",
    "MongoDBRef",
    "MongoObjectId",
    "MongoSourceConfig",
    "build_mongo_cdc_event",
    "build_mongo_cdc_message",
    "normalize_bson_value",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
    "Predicate",
    "Process",
    "ProcessNode",
    "RecordStep",
    "ResourceFactory",
    "ResourceScope",
    "Route",
    "Router",
    "Selector",
    "SinkPartition",
    "StreamFlow",
    "StreamShape",
    "Step",
    "StepContext",
    "SyncContextDependency",
    "SqlAlchemySinkConfig",
    "WindowStrategy",
    "With",
    "WithAsync",
    "compile_flow",
    "msg",
    "payload",
]
