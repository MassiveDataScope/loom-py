"""Characterization of the compile-order sort used by ``compile_all``."""

from __future__ import annotations

from loom.core.backend.sqlalchemy import _topological_sort


class _A: ...


class _B: ...


class _C: ...


class _D: ...


class TestTopologicalSort:
    def test_dependencies_come_before_their_dependents(self) -> None:
        deps = {_C: frozenset({_B}), _B: frozenset({_A}), _A: frozenset[type]()}
        assert _topological_sort([_C, _B, _A], deps) == [_A, _B, _C]

    def test_independent_models_keep_their_input_order(self) -> None:
        assert _topological_sort([_C, _A, _B], {}) == [_C, _A, _B]

    def test_ties_are_released_in_input_order(self) -> None:
        deps = {
            _D: frozenset({_B, _C}),
            _C: frozenset({_A}),
            _B: frozenset({_A}),
        }
        assert _topological_sort([_D, _C, _B, _A], deps) == [_A, _C, _B, _D]

    def test_dependencies_outside_the_set_are_ignored(self) -> None:
        deps = {_A: frozenset({_D}), _B: frozenset({_A})}
        assert _topological_sort([_B, _A], deps) == [_A, _B]

    def test_cyclic_models_are_appended_in_input_order(self) -> None:
        deps = {
            _A: frozenset({_B}),
            _B: frozenset({_A}),
            _D: frozenset({_A}),
        }
        assert _topological_sort([_D, _B, _A, _C], deps) == [_C, _D, _B, _A]

    def test_a_self_dependency_is_treated_as_a_cycle(self) -> None:
        deps = {_A: frozenset({_A})}
        assert _topological_sort([_A, _B], deps) == [_B, _A]

    def test_an_empty_input_yields_an_empty_order(self) -> None:
        assert _topological_sort([], {}) == []
