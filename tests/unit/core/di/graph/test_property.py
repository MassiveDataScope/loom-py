"""Property: a graph is refused iff a root reaches a cycle of non-call edges; the path is a loop.

Each seed draws a small graph of classes, each bound under itself, whose
constructor parameters are plain keys (``to<j>``) or call edges
(``call<j>``). An independent oracle decides whether the root reaches a cycle
of non-call edges; the analysis must agree, and a refusal must name a path
of real edges that closes on a node after its last call edge.
"""

from __future__ import annotations

import inspect
import random
import re

import pytest

from loom.core.di.graph import DependencyGraphError
from loom.core.di.scope import Scope
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.acme import AcmeCall

SEEDS = range(500)
_STEP = re.compile(r" -> (?P<name>(?:to|call)\d+): (?P<target>\w+)")

Edges = dict[int, list[tuple[int, bool]]]


def _draw(rng: random.Random) -> Edges:
    """Each node's out-edges: ``(target, call_edge)``, at most one per target and kind."""
    size = rng.randint(1, 6)
    return {
        i: [(j, call) for j in range(size) for call in (False, True) if rng.random() < 0.18]
        for i in range(size)
    }


def _classes(seed: int, edges: Edges) -> list[type]:
    """One class per node, whose ``__init__`` declares the node's edges."""
    classes = [type(f"AcmeG{seed}N{i}", (), {}) for i in edges]
    for i, cls in enumerate(classes):
        parameters = [inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        for j, call in edges[i]:
            annotation = AcmeCall[classes[j]] if call else classes[j]  # type: ignore[valid-type]
            name = f"call{j}" if call else f"to{j}"
            parameters.append(
                inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=annotation)
            )

        def init(self: object, **kwargs: object) -> None:
            del self, kwargs

        init.__signature__ = inspect.Signature(parameters)  # type: ignore[attr-defined]
        init.__annotations__ = {p.name: p.annotation for p in parameters[1:]}
        cls.__init__ = init  # type: ignore[misc]
    return classes


def _reaches_construction_cycle(edges: Edges) -> bool:
    """The oracle: from node 0, through any edges, a cycle of non-call edges is reachable."""
    reached, stack = {0}, [0]
    while stack:
        for j, _ in edges[stack.pop()]:
            if j not in reached:
                reached.add(j)
                stack.append(j)
    keys = {j for i in reached for j, call in edges[i] if not call}
    plain = {i: [j for j, call in edges[i] if not call] for i in edges}
    return any(_on_plain_cycle(start, plain) for start in keys)


def _on_plain_cycle(start: int, plain: dict[int, list[int]]) -> bool:
    """Whether a cycle of non-call edges is reachable from *start* through non-call edges."""
    state: dict[int, str] = {}

    def visit(node: int) -> bool:
        state[node] = "open"
        for target in plain[node]:
            if state.get(target) == "open" or (target not in state and visit(target)):
                return True
        state[node] = "closed"
        return False

    return visit(start)


def _assert_real_loop(message: str, seed: int, edges: Edges) -> None:
    """Every step of the reported path is a real edge, and it closes on a node it holds.

    A node is ``(index, built by loom)``: the root and a call target are
    classes loom builds, any other target is the binding of that class.
    """
    assert message.endswith(": dependency cycle"), message
    nodes = [(0, True)]
    start = 0
    for step in _STEP.finditer(message.removesuffix(": dependency cycle")):
        target = int(step["target"].removeprefix(f"AcmeG{seed}N"))
        call = step["name"].startswith("call")
        assert (target, call) in edges[nodes[-1][0]], message
        if call:
            start = len(nodes)
        nodes.append((target, call))
    assert nodes[-1] in nodes[start:-1], message


@pytest.mark.parametrize("seed", SEEDS)
def test_refused_iff_a_root_reaches_a_cycle_of_non_call_edges(seed: int) -> None:
    edges = _draw(random.Random(seed))
    classes = _classes(seed, edges)
    container = acme.container()
    for cls in classes:
        container.register(cls, cls, scope=Scope.REQUEST)

    try:
        acme.analysed(container, classes[0])
    except DependencyGraphError as refused:
        assert _reaches_construction_cycle(edges), str(refused)
        _assert_real_loop(str(refused), seed, edges)
    else:
        assert not _reaches_construction_cycle(edges)
