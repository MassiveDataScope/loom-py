"""Branch indexing: one authority for the index every branching node's path carries.

Three components index the branches of a branching node: the compiler keys
``plan.terminal_sinks`` by path, the validator labels branches for error
messages, and the Bytewax adapter resolves the sink for the path it wires.
The parity tests here fail whenever any two of them stop agreeing.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any, ClassVar

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from bytewax.testing import TestingSink, TestingSource  # noqa: E402
from omegaconf import DictConfig  # noqa: E402

from loom.core.model import LoomStruct  # noqa: E402
from loom.streaming import (  # noqa: E402
    Broadcast,
    BroadcastRoute,
    Fork,
    ForkRoute,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    Route,
    Router,
    StreamFlow,
)
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown  # noqa: E402
from loom.streaming.compiler import (  # noqa: E402
    CompilationError,
    CompiledPlan,
    StreamingErrorCode,
    compile_flow,
    walk_process_nodes,
)
from loom.streaming.nodes._branches import iter_branches  # noqa: E402
from loom.streaming.nodes._expand_routes import ExpandRoutes  # noqa: E402
from loom.streaming.nodes._fork import ForkKind  # noqa: E402

_RAW_TOPIC = "orders.raw"
_TOPIC_A = "orders.a"
_TOPIC_B = "orders.b"
_TOPIC_DEFAULT = "orders.default"
_TOPIC_ANALYTICS = "events.analytics"
_TOPIC_FULFILLMENT = "orders.fulfillment"


class _Order(LoomStruct):
    channel: str
    order_id: str


class _Routed(LoomStruct):
    order_id: str


class _StoreRow(LoomStruct):
    order_id: str


class _AuditRow(LoomStruct):
    order_id: str


class _MarkRouted(RecordStep[_Order, _Routed]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _Routed:
        del kwargs
        return _Routed(order_id=message.payload.order_id)


class _ChannelSelector:
    def select(self, message: Message[_Order]) -> object:
        return message.payload.channel


class _IsChannelA:
    def matches(self, message: Message[_Order]) -> bool:
        return message.payload.channel == "a"


class _IsChannelB:
    def matches(self, message: Message[_Order]) -> bool:
        return message.payload.channel == "b"


class _OrderExpander:
    outputs: ClassVar[tuple[type, ...]] = (_StoreRow, _AuditRow)

    @classmethod
    def expand(cls, event: _Order) -> dict[type, list[Any]]:
        return {
            _StoreRow: [_StoreRow(order_id=event.order_id)],
            _AuditRow: [_AuditRow(order_id=event.order_id)],
        }


def _flow(process: Process[Any, Any]) -> StreamFlow[_Order, Any]:
    return StreamFlow(
        name="branch_indexing",
        source=FromTopic(_RAW_TOPIC, payload=_Order),
        process=process,
    )


def _fork_by_node() -> Fork[_Order]:
    return Fork.by(
        _ChannelSelector(),
        {
            "a": Process(IntoTopic(_TOPIC_A, payload=_Routed)),
            "b": Process(IntoTopic(_TOPIC_B, payload=_Routed)),
        },
        default=Process(IntoTopic(_TOPIC_DEFAULT, payload=_Routed)),
    )


def _fork_when_node() -> Fork[_Order]:
    return Fork.when(
        (
            ForkRoute(when=_IsChannelA(), process=Process(IntoTopic(_TOPIC_A, payload=_Routed))),
            ForkRoute(when=_IsChannelB(), process=Process(IntoTopic(_TOPIC_B, payload=_Routed))),
        ),
        default=Process(IntoTopic(_TOPIC_DEFAULT, payload=_Routed)),
    )


def _broadcast_node() -> Broadcast[_Order]:
    return Broadcast(
        BroadcastRoute(
            process=Process(_MarkRouted()),
            output=IntoTopic(_TOPIC_ANALYTICS, payload=_Routed),
        ),
        BroadcastRoute(
            process=Process(_MarkRouted()),
            output=IntoTopic(_TOPIC_FULFILLMENT, payload=_Routed),
        ),
    )


def _expand_routes_node() -> ExpandRoutes[_Order]:
    return ExpandRoutes(
        expander=_OrderExpander,
        routes={_StoreRow: Process(IntoTopic(_TOPIC_A, payload=_StoreRow))},
        default=Process(IntoTopic(_TOPIC_DEFAULT, payload=_AuditRow)),
    )


def _router_when_node() -> Router[_Order, _Routed]:
    return Router.when(
        (
            Route(
                when=_IsChannelA(),
                process=Process(_MarkRouted(), IntoTopic(_TOPIC_A, payload=_Routed)),
            ),
            Route(
                when=_IsChannelB(),
                process=Process(_MarkRouted(), IntoTopic(_TOPIC_B, payload=_Routed)),
            ),
        ),
        default=Process(_MarkRouted(), IntoTopic(_TOPIC_DEFAULT, payload=_Routed)),
    )


def _router_keyed_and_predicate_node() -> Router[_Order, _Routed]:
    return Router(
        selector=_ChannelSelector(),
        routes={"a": Process(_MarkRouted(), IntoTopic(_TOPIC_A, payload=_Routed))},
        predicate_routes=(
            Route(
                when=_IsChannelB(),
                process=Process(_MarkRouted(), IntoTopic(_TOPIC_B, payload=_Routed)),
            ),
        ),
        default=Process(_MarkRouted(), IntoTopic(_TOPIC_DEFAULT, payload=_Routed)),
    )


class _RecordingTerminalSinks(dict[tuple[int, ...], Any]):
    """Terminal-sink mapping that remembers every path the adapter resolved."""

    def __init__(self, paths: Iterable[tuple[int, ...]]) -> None:
        super().__init__({path: TestingSink([]) for path in paths})
        self.lookups: list[tuple[int, ...]] = []

    def get(self, path: tuple[int, ...], default: Any = None) -> Any:
        self.lookups.append(path)
        return super().get(path, default)


def _adapter_resolved_paths(plan: CompiledPlan) -> set[tuple[int, ...]]:
    """Build the Bytewax dataflow for *plan* and return the paths it resolved."""
    sinks = _RecordingTerminalSinks(plan.terminal_sinks)
    built = build_dataflow_with_shutdown(
        plan,
        source=TestingSource([]),
        sink=None,
        terminal_sinks=sinks,
    )
    built.shutdown()
    return set(sinks.lookups)


def _sink_topics(plan: CompiledPlan) -> list[str]:
    return [sink.topic for sink in plan.terminal_sinks.values()]


def _declared_branch_terminal_paths(node: object, prefix: tuple[int, ...]) -> set[tuple[int, ...]]:
    """Return the path every terminal topic of *node*'s branches must be keyed by."""
    paths: set[tuple[int, ...]] = set()
    for branch in iter_branches(node):
        branch_path = prefix + (branch.index,)
        if branch.output is not None:
            paths.add(branch_path)
        for position, inner in enumerate(branch.nodes):
            if isinstance(inner, IntoTopic):
                paths.add(branch_path + (position,))
    return paths


class TestBranchEnumeration:
    """``iter_branches`` is the single authority for branch indices and labels."""

    def test_fork_by_indexes_keyed_branches_then_the_default(self) -> None:
        branches = list(iter_branches(_fork_by_node()))

        assert [(b.index, b.label) for b in branches] == [
            (0, "'a'"),
            (1, "'b'"),
            (2, "default"),
        ]
        assert [b.key for b in branches] == ["a", "b", None]
        assert [b.is_default for b in branches] == [False, False, True]

    def test_fork_when_indexes_predicate_branches_then_the_default(self) -> None:
        branches = list(iter_branches(_fork_when_node()))

        assert [(b.index, b.label) for b in branches] == [
            (0, "predicate[0]"),
            (1, "predicate[1]"),
            (2, "default"),
        ]
        assert [b.when is None for b in branches] == [False, False, True]

    def test_router_indexes_keyed_branches_then_predicates_then_the_default(self) -> None:
        branches = list(iter_branches(_router_keyed_and_predicate_node()))

        assert [(b.index, b.label) for b in branches] == [
            (0, "'a'"),
            (1, "predicate[0]"),
            (2, "default"),
        ]

    def test_broadcast_indexes_every_route_with_its_output(self) -> None:
        branches = list(iter_branches(_broadcast_node()))

        assert [(b.index, b.label) for b in branches] == [(0, "0"), (1, "1")]
        assert [b.output.name if b.output is not None else None for b in branches] == [
            _TOPIC_ANALYTICS,
            _TOPIC_FULFILLMENT,
        ]

    def test_expand_routes_indexes_declared_types_then_the_default(self) -> None:
        branches = list(iter_branches(_expand_routes_node()))

        assert [(b.index, b.label) for b in branches] == [(0, "_StoreRow"), (1, "default")]
        assert [b.key for b in branches] == [_StoreRow, None]

    def test_every_branch_index_is_unique_and_contiguous(self) -> None:
        for node in (
            _fork_by_node(),
            _fork_when_node(),
            _broadcast_node(),
            _expand_routes_node(),
            _router_when_node(),
            _router_keyed_and_predicate_node(),
        ):
            indices = [branch.index for branch in iter_branches(node)]

            assert indices == list(range(len(indices))), type(node).__name__

    def test_a_node_that_does_not_branch_has_no_branches(self) -> None:
        assert list(iter_branches(_MarkRouted())) == []


class TestCompiledBranchPathParity:
    """The compiler keys every branch terminal by its ``iter_branches`` path.

    Parametrized over every branching kind, ``Router`` included: a branch index
    that the compiler derived on its own would collide with a sibling and lose
    a terminal, which is what these expected path sets pin down.
    """

    @pytest.mark.parametrize(
        ("kind", "build_node"),
        [
            ("fork_by", _fork_by_node),
            ("fork_when", _fork_when_node),
            ("broadcast", _broadcast_node),
            ("expand_routes", _expand_routes_node),
            ("router_when", _router_when_node),
            ("router_keyed_and_predicate", _router_keyed_and_predicate_node),
        ],
    )
    def test_compiled_terminal_paths_match_the_branch_paths(
        self,
        kind: str,
        build_node: Callable[[], Any],
        streaming_kafka_config: DictConfig,
    ) -> None:
        node = build_node()

        plan = compile_flow(_flow(Process(node)), config=streaming_kafka_config)

        assert set(plan.terminal_sinks) == _declared_branch_terminal_paths(node, (0,)), kind


class TestAdapterResolvesCompiledPaths:
    """Every path the compiler keys a sink by is a path the adapter resolves."""

    @pytest.mark.parametrize(
        ("kind", "build_node", "expected_sinks"),
        [
            ("fork_by", _fork_by_node, 3),
            ("fork_when", _fork_when_node, 3),
            ("broadcast", _broadcast_node, 2),
            ("expand_routes", _expand_routes_node, 2),
            ("router_when", _router_when_node, 3),
            ("router_keyed_and_predicate", _router_keyed_and_predicate_node, 3),
        ],
    )
    def test_compiled_terminal_paths_are_all_resolved_by_the_adapter(
        self,
        kind: str,
        build_node: Callable[[], Any],
        expected_sinks: int,
        streaming_kafka_config: DictConfig,
    ) -> None:
        plan = compile_flow(_flow(Process(build_node())), config=streaming_kafka_config)

        compiled_paths = set(plan.terminal_sinks)
        assert len(compiled_paths) == expected_sinks, f"{kind}: one sink per declared terminal"

        orphans = compiled_paths - _adapter_resolved_paths(plan)
        assert orphans == set(), f"{kind}: compiled sinks the adapter never resolves"


class TestRouterBranchPaths:
    """A router branch keeps its own path even when a fallback branch exists."""

    def test_router_when_with_a_default_gives_every_branch_its_own_path(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        plan = compile_flow(_flow(Process(_router_when_node())), config=streaming_kafka_config)

        assert sorted(_sink_topics(plan)) == sorted([_TOPIC_A, _TOPIC_B, _TOPIC_DEFAULT])

    def test_router_with_keyed_predicate_and_default_gives_every_branch_its_own_path(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        plan = compile_flow(
            _flow(Process(_router_keyed_and_predicate_node())),
            config=streaming_kafka_config,
        )

        assert sorted(_sink_topics(plan)) == sorted([_TOPIC_A, _TOPIC_B, _TOPIC_DEFAULT])


class TestForkKindConsistency:
    """A fork declares one dispatch family, so only that family's routes exist."""

    def test_keyed_fork_rejects_predicate_routes(self) -> None:
        selector = _ChannelSelector()
        routes: Mapping[object, Process[Any, Any]] = {
            "a": Process(IntoTopic(_TOPIC_A, payload=_Routed))
        }
        predicate_routes = (
            ForkRoute(
                when=_IsChannelB(),
                process=Process(IntoTopic(_TOPIC_B, payload=_Routed)),
            ),
        )

        with pytest.raises(ValueError, match="predicate_routes"):
            Fork(
                kind=ForkKind.KEYED,
                selector=selector,
                routes=routes,
                predicate_routes=predicate_routes,
            )

    def test_predicate_fork_rejects_keyed_routes(self) -> None:
        routes = {"a": Process(IntoTopic(_TOPIC_A, payload=_Routed))}
        predicate_routes = (
            ForkRoute(
                when=_IsChannelB(),
                process=Process(IntoTopic(_TOPIC_B, payload=_Routed)),
            ),
        )

        with pytest.raises(ValueError, match="routes"):
            Fork(
                kind=ForkKind.PREDICATE,
                routes=routes,
                predicate_routes=predicate_routes,
            )


class TestForkDefaultBranchIsValidated:
    """The fork fallback branch is validated like every other branch."""

    def test_a_fork_default_branch_without_a_terminal_is_rejected(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow = _flow(
            Process(
                Fork.by(
                    _ChannelSelector(),
                    {"a": Process(IntoTopic(_TOPIC_A, payload=_Routed))},
                    default=Process(_MarkRouted()),
                )
            )
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        issues = [
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.FORK_BRANCH_NO_TERMINAL
        ]
        assert [issue.component for issue in issues] == ["fork branch default"]

    def test_walking_the_process_tree_reaches_the_fork_default_branch(self) -> None:
        marker = _MarkRouted()
        fork = Fork.by(
            _ChannelSelector(),
            {"a": Process(IntoTopic(_TOPIC_A, payload=_Routed))},
            default=Process(marker, IntoTopic(_TOPIC_DEFAULT, payload=_Routed)),
        )

        assert any(node is marker for node in walk_process_nodes((fork,)))
