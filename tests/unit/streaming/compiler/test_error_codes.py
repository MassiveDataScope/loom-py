"""Structured compilation issues: codes, components, and aggregation contract."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any, ClassVar, cast

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    Backend,
    BatchStep,
    Broadcast,
    BroadcastRoute,
    CollectBatch,
    ContextFactory,
    Drain,
    ErrorEnvelope,
    ExpandRoutes,
    ExpandStep,
    Explode,
    Fork,
    ForkRoute,
    FromMongoCDC,
    FromMultiTypeTopic,
    FromTopic,
    IntoTable,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    ResourceScope,
    Route,
    Router,
    StreamFlow,
    WindowStrategy,
    With,
    msg,
)
from loom.streaming.compiler import (
    CompilationError,
    StreamingErrorCode,
    compile_flow,
)
from loom.streaming.compiler.phases.build_plan import (
    _build_dispatch_table,
    _build_storage_sink,
)
from loom.streaming.nodes._fork import ForkKind
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


class TestCompilationErrorContract:
    def test_issues_carry_code_component_and_message(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(CollectBatch(2, 10), FakeStep(), IntoTopic("out", payload=Result)),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        codes = [issue.code for issue in exc_info.value.issues]
        assert StreamingErrorCode.SHAPE_MISMATCH in codes
        mismatch = next(
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.SHAPE_MISMATCH
        )
        assert mismatch.component == "FakeStep"
        assert "shape mismatch" in mismatch.message

    def test_errors_property_mirrors_issue_messages(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(FakeStep()),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        exc = exc_info.value
        assert exc.errors == [issue.message for issue in exc.issues]
        assert str(exc).startswith(f"Compilation failed with {len(exc.issues)} error(s):")

    def test_missing_terminal_output_code_and_field(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(FakeStep()),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        issue = next(
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.MISSING_TERMINAL_OUTPUT
        )
        assert issue.field == "output"

    def test_kafka_config_invalid_code(self) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(FakeStep(), IntoTopic("out", payload=Result)),
        )

        empty_config = OmegaConf.create({})

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=empty_config)

        codes = {issue.code for issue in exc_info.value.issues}
        assert StreamingErrorCode.KAFKA_CONFIG_INVALID in codes

    def test_constructor_normalizes_bare_strings(self) -> None:
        exc = CompilationError(["legacy message"])

        assert exc.errors == ["legacy message"]
        assert exc.issues[0].code is StreamingErrorCode.UNSPECIFIED
        assert exc.issues[0].message == "legacy message"


class TestBranchPrefixing:
    def test_fork_branch_issue_keeps_code_and_prefixes_message(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Fork.when(
                    routes=[
                        ForkRoute(
                            when=msg.payload.amount >= 0,
                            process=Process(FakeStep()),
                        )
                    ],
                )
            ),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        branch_issue = next(
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.FORK_BRANCH_NO_TERMINAL
        )
        assert branch_issue.message.startswith("fork branch ")


class TestBuildPhaseCodes:
    def test_unsupported_storage_sink_raises_coded_error(self) -> None:
        class _NotATable:
            name = "fake"

        not_a_table = _NotATable()
        empty_config = OmegaConf.create({})

        with pytest.raises(CompilationError) as exc_info:
            _build_storage_sink(not_a_table, empty_config)

        issue = exc_info.value.issues[0]
        assert issue.code is StreamingErrorCode.STORAGE_SINK_UNSUPPORTED
        assert "Unsupported storage sink: _NotATable" in issue.message

    def test_payload_without_message_type_raises_coded_error(self) -> None:
        class _PlainPayload:
            pass

        with pytest.raises(CompilationError) as exc_info:
            _build_dispatch_table((_PlainPayload,))

        issue = exc_info.value.issues[0]
        assert issue.code is StreamingErrorCode.PAYLOAD_TYPE_INVALID
        assert issue.field == "payloads"


class _NeedsPrefix(RecordStep[Order, Result]):
    """A step declared as a class whose constructor cannot run without arguments."""

    def __init__(self) -> None:
        raise TypeError("prefix is required")

    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=message.payload.order_id)


class _Tokens(ContextFactory):
    """A configured context factory: a binding that resolves to no process node."""

    def __init__(self, *, token: str) -> None:
        if not token:
            raise ValueError("token must not be empty")
        super().__init__(self._create)
        self.token = token

    def _create(self) -> AbstractContextManager[str]:
        return nullcontext(self.token)


class _Fanout(ExpandStep[Order, Result]):
    def execute(self, message: Message[Order], **kwargs: object) -> list[Message[Result]]:
        del message, kwargs
        return []


class _SameBatch(BatchStep[Order, Order]):
    def execute(self, messages: list[Message[Order]], **kwargs: object) -> list[Order]:
        del kwargs
        return [message.payload for message in messages]


class _Expander:
    outputs: ClassVar[tuple[type, ...]] = (Result,)

    def expand(self, event: Order) -> dict[type, list[Any]]:
        del event
        return {}


def _out() -> IntoTopic[Result]:
    return IntoTopic("out", payload=Result)


def _fork() -> Fork[Order]:
    return Fork.when([ForkRoute(when=msg.payload.order_id == "a", process=Process(_out()))])


def _broadcast(*nodes: Any) -> Broadcast[Order]:
    return Broadcast(BroadcastRoute(process=Process(*nodes), output=_out()))


def _expand_routes(*nodes: Any) -> ExpandRoutes[Order]:
    return ExpandRoutes(expander=_Expander, routes={Result: Process(*nodes)})  # type: ignore[arg-type]


def _terminal_router() -> Router[Order, Any]:
    """A Router with a default whose every branch ends the path of its messages."""
    return Router.by(msg.payload.order_id, routes={"a": Process(_out())}, default=Process(_out()))


def _router(*nodes: Any) -> Router[Order, Any]:
    return Router.when([Route(when=msg.payload.order_id == "a", process=Process(*nodes))])


def _flow(
    *nodes: Any,
    source: Any = None,
    output: IntoTopic[Any] | None = None,
) -> StreamFlow[Any, Any]:
    return StreamFlow(
        name="issue_codes",
        source=source or FromTopic("in", payload=Order),
        process=Process(*nodes),
        output=output,
    )


_TOKENS_CONFIG: dict[str, object] = {"streaming": {"bindings": {"tokens": {"token": ""}}}}

_FlowCase = tuple[
    Callable[[], StreamFlow[Any, Any]], dict[str, object], StreamingErrorCode, str, str, str | None
]

# (flow, extra config, code, start of the message, component, field). Where the
# issue carries an exception, only the catalogued text before it is pinned.
_COMPILE_PATH_CASES: dict[str, _FlowCase] = {
    "step class that cannot be built": (
        lambda: _flow(_NeedsPrefix, _out()),
        {},
        StreamingErrorCode.STEP_INSTANTIATION_FAILED,
        "step _NeedsPrefix: prefix is required",
        "_NeedsPrefix",
        None,
    ),
    "binding whose constructor refuses its config": (
        lambda: _flow(_Tokens.from_config("streaming.bindings.tokens"), _out()),
        _TOKENS_CONFIG,
        StreamingErrorCode.BINDING_RESOLUTION_FAILED,
        "binding streaming.bindings.tokens: token must not be empty",
        "streaming.bindings.tokens",
        None,
    ),
    "binding that resolves to a context factory": (
        lambda: _flow(_Tokens.from_config("streaming.bindings.tokens", token="t"), _out()),
        _TOKENS_CONFIG,
        StreamingErrorCode.BINDING_NOT_PROCESS_NODE,
        "binding streaming.bindings.tokens: resolved object is not a process node",
        "streaming.bindings.tokens",
        None,
    ),
    "keyed fork without selector": (
        lambda: _flow(Fork(kind=ForkKind.KEYED, routes={"a": Process(_out())})),
        {},
        StreamingErrorCode.FORK_SELECTOR_MISSING,
        "fork selector missing for keyed fork",
        "fork",
        "selector",
    ),
    "storage sink without name": (
        lambda: _flow(FakeStep(), IntoTable(payload=Result, table="t", backend=Backend.DELTA)),
        {},
        StreamingErrorCode.SINK_MISSING_NAME,
        "storage sink 'IntoTable': missing name",
        "IntoTable",
        "name",
    ),
    "storage sink without config section": (
        lambda: _flow(
            FakeStep(),
            IntoTable(payload=Result, table="t", backend=Backend.CLICKHOUSE, name="results"),
        ),
        {},
        StreamingErrorCode.SINK_CONFIG_INVALID,
        "storage sink 'results': ",
        "results",
        "streaming.sinks.results",
    ),
    "mongo source without mongo config": (
        lambda: _flow(Drain(), source=FromMongoCDC("domain_events")),
        {},
        StreamingErrorCode.MONGO_CONFIG_INVALID,
        "mongo source 'domain_events': ",
        "mongo source 'domain_events'",
        "mongo",
    ),
    "batch-scoped With holding a context manager": (
        lambda: _flow(
            With(ResourceScope.BATCH, process=Process(FakeStep(), _out()), db=nullcontext())
        ),
        {},
        StreamingErrorCode.BATCH_SCOPE_DIRECT_CONTEXT,
        "With with scope=BATCH cannot use direct context manager instances: db. "
        "Use ContextFactory for batch-scoped resources.",
        "With",
        None,
    ),
    "router that ends every path followed by a step": (
        lambda: _flow(_terminal_router(), FakeStep(), _out()),
        {},
        StreamingErrorCode.TERMINAL_NOT_LAST,
        "Router must be the last node in a process",
        "Router",
        None,
    ),
    "router that ends every path followed by an IntoTopic": (
        lambda: _flow(_terminal_router(), _out()),
        {},
        StreamingErrorCode.TERMINAL_NOT_LAST,
        "Router must be the last node in a process",
        "Router",
        None,
    ),
    "router that ends every path inside a broadcast route, followed by a step": (
        lambda: _flow(
            Broadcast(
                BroadcastRoute(process=Process(_terminal_router(), FakeStep()), output=_out())
            )
        ),
        {},
        StreamingErrorCode.TERMINAL_NOT_LAST,
        "broadcast branch 0: Router must be the last node in a process",
        "broadcast branch 0 > Router",
        None,
    ),
    "terminal followed by a step": (
        lambda: _flow(_out(), FakeStep()),
        {},
        StreamingErrorCode.TERMINAL_NOT_LAST,
        "IntoTopic must be the last node in a process",
        "IntoTopic",
        None,
    ),
    "fork followed by a step": (
        lambda: _flow(_fork(), FakeStep()),
        {},
        StreamingErrorCode.FORK_NOT_LAST,
        "fork must be the last node in a process",
        "fork",
        None,
    ),
    "broadcast followed by a step": (
        lambda: _flow(_broadcast(FakeStep()), FakeStep()),
        {},
        StreamingErrorCode.BROADCAST_NOT_LAST,
        "broadcast must be the last node in a process",
        "broadcast",
        None,
    ),
    "broadcast branch issue scoped under its branch": (
        lambda: _flow(_broadcast(CollectBatch(10, 500), FakeStep())),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "broadcast branch 0: shape mismatch: expected record but got batch before FakeStep",
        "broadcast branch 0 > FakeStep",
        None,
    ),
    "broadcast not last inside an expand route": (
        lambda: _flow(_expand_routes(_broadcast(FakeStep()), FakeStep())),
        {},
        StreamingErrorCode.BROADCAST_NOT_LAST,
        "expand_routes branch Result: broadcast must be the last node in a process",
        "expand_routes branch Result > broadcast",
        None,
    ),
    "broadcast after an expand routes": (
        lambda: _flow(_expand_routes(_out()), _broadcast(FakeStep())),
        {},
        StreamingErrorCode.TERMINAL_NOT_LAST,
        "ExpandRoutes must be the last node in a process",
        "ExpandRoutes",
        None,
    ),
    "broadcast not last inside a router branch": (
        lambda: _flow(_router(_broadcast(FakeStep()), FakeStep())),
        {},
        StreamingErrorCode.BROADCAST_NOT_LAST,
        "router branch predicate[0]: broadcast must be the last node in a process",
        "router branch predicate[0] > broadcast",
        None,
    ),
    "scoped process followed by a step": (
        lambda: _flow(With(process=Process(FakeStep(), _out())), FakeStep()),
        {},
        StreamingErrorCode.SCOPED_PROCESS_NOT_LAST,
        "With(process=...) must be the last node in a process",
        "With",
        None,
    ),
    "explode followed by a step": (
        lambda: _flow(Explode(_Expander), FakeStep(), _out()),
        {},
        StreamingErrorCode.EXPLODE_WITHOUT_ROUTER,
        "Explode must be immediately followed by a Router; got FakeStep",
        "Explode",
        None,
    ),
    "explode at the end of a process": (
        lambda: _flow(Explode(_Expander)),
        {},
        StreamingErrorCode.EXPLODE_WITHOUT_ROUTER,
        "Explode must be immediately followed by a Router; got nothing",
        "Explode",
        None,
    ),
    "router branch holding a fork": (
        lambda: _flow(_router(_fork())),
        {},
        StreamingErrorCode.ROUTER_BRANCH_UNSAFE_NODE,
        "router branch predicate[0]: node Fork is not router-branch safe",
        "router branch predicate[0]",
        None,
    ),
    "router branch holding a fan-out step": (
        lambda: _flow(_router(_Fanout(), _out())),
        {},
        StreamingErrorCode.ROUTER_BRANCH_FANOUT_UNSUPPORTED,
        "router branch predicate[0]: _Fanout is not supported in Router branches — Router is "
        "1-to-1; use Fork for fan-out.",
        "router branch predicate[0]",
        None,
    ),
    "router branches of different shapes": (
        lambda: _flow(
            Router.when(
                [Route(when=msg.payload.order_id == "a", process=Process(CollectBatch(10, 1000)))],
                default=Process(FakeStep()),
            ),
            _out(),
        ),
        {},
        StreamingErrorCode.ROUTER_BRANCH_SHAPE_DIVERGENCE,
        "router branches produce different shapes: batch, record",
        "router",
        None,
    ),
    "router branch issue scoped under its branch": (
        lambda: _flow(_router(CollectBatch(2, 10), FakeStep(), _out())),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "router branch predicate[0]: shape mismatch: expected record but got batch before FakeStep",
        "router branch predicate[0] > FakeStep",
        None,
    ),
    "flow output next to a fork": (
        lambda: _flow(_fork(), output=_out()),
        {},
        StreamingErrorCode.OUTPUT_WITH_FORK,
        "flow.output cannot be combined with Fork: branches must be terminal",
        "flow.output",
        "output",
    ),
    "flow output next to a broadcast": (
        lambda: _flow(_broadcast(FakeStep()), output=_out()),
        {},
        StreamingErrorCode.OUTPUT_WITH_BROADCAST,
        "flow.output cannot be combined with Broadcast: branches must be terminal",
        "flow.output",
        "output",
    ),
    "flow output after a process that ends in IntoTopic": (
        lambda: _flow(FakeStep(), _out(), output=_out()),
        {},
        StreamingErrorCode.OUTPUT_AFTER_PATH_END,
        "flow.output is never written: the process ends in IntoTopic, which ends the path "
        "of every message it receives. Remove the output or the IntoTopic.",
        "flow.output",
        "output",
    ),
    "flow output after an ExpandRoutes": (
        lambda: _flow(_expand_routes(_out()), output=_out()),
        {},
        StreamingErrorCode.OUTPUT_AFTER_PATH_END,
        "flow.output is never written: the process ends in ExpandRoutes, which ends the path "
        "of every message it receives. Remove the output or the ExpandRoutes.",
        "flow.output",
        "output",
    ),
    "flow output after a process that ends in IntoSink": (
        lambda: _flow(
            FakeStep(),
            IntoTable(payload=Result, table="t", backend=Backend.DELTA, name="results"),
            output=_out(),
        ),
        {"streaming": {"sinks": {"results": {"uri": "memory://results"}}}},
        StreamingErrorCode.OUTPUT_AFTER_PATH_END,
        "flow.output is never written: the process ends in IntoTable, which ends the path "
        "of every message it receives. Remove the output or the IntoTable.",
        "flow.output",
        "output",
    ),
    "flow output after a process that ends in Drain": (
        lambda: _flow(FakeStep(), Drain(), output=_out()),
        {},
        StreamingErrorCode.OUTPUT_AFTER_PATH_END,
        "flow.output is never written: the process ends in Drain, which ends the path "
        "of every message it receives. Remove the output or the Drain.",
        "flow.output",
        "output",
    ),
    "broadcast route output after an ExpandRoutes": (
        lambda: _flow(
            Broadcast(BroadcastRoute(process=Process(_expand_routes(_out())), output=_out()))
        ),
        {},
        StreamingErrorCode.BROADCAST_OUTPUT_AFTER_PATH_END,
        "broadcast branch 0: its output is never written: the route's process ends in "
        "ExpandRoutes, which ends the path of every message it receives. Remove the output "
        "or the ExpandRoutes.",
        "broadcast branch 0",
        "output",
    ),
    "broadcast route output after a Fork with a default": (
        lambda: _flow(
            Broadcast(
                BroadcastRoute(
                    process=Process(
                        Fork.when(
                            [ForkRoute(when=msg.payload.order_id == "a", process=Process(_out()))],
                            default=Process(_out()),
                        )
                    ),
                    output=_out(),
                )
            )
        ),
        {},
        StreamingErrorCode.BROADCAST_OUTPUT_AFTER_PATH_END,
        "broadcast branch 0: its output is never written: the route's process ends in "
        "Fork, which ends the path of every message it receives. Remove the output "
        "or the Fork.",
        "broadcast branch 0",
        "output",
    ),
    "flow output after a Router whose branches all end": (
        lambda: _flow(
            Router.by(msg.payload.order_id, routes={"a": Process(_out())}, default=Process(_out())),
            output=_out(),
        ),
        {},
        StreamingErrorCode.OUTPUT_AFTER_PATH_END,
        "flow.output is never written: the process ends in Router, which ends the path "
        "of every message it receives. Remove the output or the Router.",
        "flow.output",
        "output",
    ),
    "broadcast route output after a Router whose branches all end": (
        lambda: _flow(
            Broadcast(
                BroadcastRoute(
                    process=Process(
                        Router.by(
                            msg.payload.order_id,
                            routes={"a": Process(_out())},
                            default=Process(_out()),
                        )
                    ),
                    output=_out(),
                )
            )
        ),
        {},
        StreamingErrorCode.BROADCAST_OUTPUT_AFTER_PATH_END,
        "broadcast branch 0: its output is never written: the route's process ends in "
        "Router, which ends the path of every message it receives. Remove the output "
        "or the Router.",
        "broadcast branch 0",
        "output",
    ),
    "broadcast route output after a terminal": (
        lambda: _flow(_broadcast(_out())),
        {},
        StreamingErrorCode.BROADCAST_OUTPUT_AFTER_PATH_END,
        "broadcast branch 0: its output is never written: the route's process ends in "
        "IntoTopic, which ends the path of every message it receives. Remove the output "
        "or the IntoTopic.",
        "broadcast branch 0",
        "output",
    ),
    "bare ErrorEnvelope among the payloads": (
        lambda: _flow(Drain(), source=FromMultiTypeTopic("in", payloads=(Order, ErrorEnvelope))),
        {},
        StreamingErrorCode.ERROR_ENVELOPE_UNPARAMETERIZED,
        "ErrorEnvelope in FromMultiTypeTopic must be parameterized, "
        "e.g. ErrorEnvelope[OrderEvent]. Got: ",
        "FromMultiTypeTopic",
        "payloads",
    ),
    "unsupported window strategy": (
        lambda: _flow(CollectBatch(10, 500, window=WindowStrategy.TUMBLING), Drain()),
        {},
        StreamingErrorCode.WINDOW_STRATEGY_UNSUPPORTED,
        "CollectBatch.window=tumbling is not yet supported by the Bytewax adapter. "
        "Only WindowStrategy.COLLECT is available in this adapter version.",
        "CollectBatch",
        "window",
    ),
    "batch into a topic": (
        lambda: _flow(CollectBatch(3, 50), _SameBatch(), _out()),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "shape mismatch: expected record but got batch before IntoTopic",
        "IntoTopic",
        None,
    ),
    "batch into the flow output": (
        lambda: _flow(CollectBatch(3, 50), _SameBatch(), output=_out()),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "shape mismatch: expected record but got batch before flow.output",
        "flow.output",
        "output",
    ),
    "batch into a broadcast route output": (
        lambda: _flow(_broadcast(CollectBatch(3, 50), _SameBatch())),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "broadcast branch 0: shape mismatch: expected record but got batch before output",
        "broadcast branch 0 > output",
        "output",
    ),
    "batch into a table": (
        lambda: _flow(
            CollectBatch(3, 50),
            _SameBatch(),
            IntoTable(payload=Order, table="t", backend=Backend.DELTA, name="orders"),
        ),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "shape mismatch: expected record but got batch before IntoTable",
        "IntoTable",
        None,
    ),
    "batch into a fork": (
        lambda: _flow(CollectBatch(3, 50), _SameBatch(), _fork()),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "shape mismatch: expected record but got batch before Fork",
        "Fork",
        None,
    ),
    "batch into an expand routes": (
        lambda: _flow(CollectBatch(3, 50), _SameBatch(), _expand_routes(_out())),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "shape mismatch: expected record but got batch before ExpandRoutes",
        "ExpandRoutes",
        None,
    ),
    "batch into a drain": (
        lambda: _flow(CollectBatch(3, 50), _SameBatch(), Drain()),
        {},
        StreamingErrorCode.SHAPE_MISMATCH,
        "shape mismatch: expected record but got batch before Drain",
        "Drain",
        None,
    ),
    "scoped IntoTopic followed by a step": (
        lambda: _flow(With(process=Process(_out(), FakeStep()))),
        {},
        StreamingErrorCode.SCOPED_INTO_TOPIC_NOT_LAST,
        "With(process=...) requires IntoTopic to be last; found FakeStep after it.",
        "With",
        None,
    ),
    "scoped process holding a batch node": (
        lambda: _flow(With(process=Process(CollectBatch(1, 1), _out()))),
        {},
        StreamingErrorCode.SCOPED_PROCESS_UNSUPPORTED_NODE,
        "With(process=...) only supports RecordStep nodes and an optional terminal IntoTopic; "
        "found CollectBatch.",
        "With",
        None,
    ),
}


@pytest.mark.parametrize(
    ("build", "extra_config", "code", "message", "component", "field"),
    list(_COMPILE_PATH_CASES.values()),
    ids=list(_COMPILE_PATH_CASES),
)
def test_compiling_the_flow_reports_the_catalogued_issue(
    streaming_kafka_config: DictConfig,
    build: Callable[[], StreamFlow[Any, Any]],
    extra_config: dict[str, object],
    code: StreamingErrorCode,
    message: str,
    component: str,
    field: str | None,
) -> None:
    """The compiler builds each issue on its real path, so a wrong parameter fails here."""
    flow = build()
    config = cast(
        DictConfig, OmegaConf.merge(streaming_kafka_config, OmegaConf.create(extra_config))
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, config=config)

    issue = next(issue for issue in exc_info.value.issues if issue.code is code)
    assert issue.message.startswith(message)
    assert (issue.component, issue.field) == (component, field)
