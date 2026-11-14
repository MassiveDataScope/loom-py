"""Lazy compile: a key first resolved after seal compiles then, refusing a cycle with its path."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.graph import DependencyGraphError
from loom.core.di.scope import Scope


class AcmeFirst:
    def __init__(self, second: AcmeSecond) -> None:
        self.second = second


class AcmeSecond:
    def __init__(self, third: AcmeThird) -> None:
        self.third = third


class AcmeThird:
    def __init__(self, first: AcmeFirst) -> None:
        self.first = first


class AcmeLeaf:
    pass


class AcmeBranch:
    def __init__(self, left: AcmeLeaf, right: AcmeLeaf) -> None:
        self.left, self.right = left, right


def test_a_cycle_compiled_lazily_after_seal_is_refused_raw_with_its_path() -> None:
    container = LoomContainer()
    for key in (AcmeFirst, AcmeSecond, AcmeThird):
        container.register(key, key)
    container.seal()

    with pytest.raises(DependencyGraphError) as caught:
        container.resolve(AcmeSecond)

    assert str(caught.value) == (
        "AcmeSecond -> third: AcmeThird -> first: AcmeFirst -> second: AcmeSecond: dependency cycle"
    )


def test_a_cycle_leaves_nothing_compiled_and_stays_refused() -> None:
    container = LoomContainer()
    for key in (AcmeFirst, AcmeSecond, AcmeThird):
        container.register(key, key)

    for _ in range(2):
        with pytest.raises(DependencyGraphError, match="dependency cycle"):
            container.resolve(AcmeFirst)


def test_a_dependency_reached_twice_is_no_cycle() -> None:
    container = LoomContainer()
    container.register(AcmeLeaf, AcmeLeaf, scope=Scope.APPLICATION)
    container.register(AcmeBranch, AcmeBranch)

    branch = container.resolve(AcmeBranch)

    assert branch.left is branch.right
