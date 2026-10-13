"""Tests for runner-specific errors."""

from __future__ import annotations

import importlib
import pickle

import pytest

import loom.etl.runner.errors as errors_mod

errors_mod = importlib.reload(errors_mod)
InvalidStageError = errors_mod.InvalidStageError


def test_invalid_stage_error_message_lists_the_unknown_names_sorted() -> None:
    error = InvalidStageError(frozenset({"MissingStep", "MissingProcess"}))

    assert str(error).startswith(
        "Unknown step or process names: ['MissingProcess', 'MissingStep']."
    )
    assert error.unknown == frozenset({"MissingStep", "MissingProcess"})


def test_invalid_stage_error_takes_the_names_as_unknown() -> None:
    error = InvalidStageError(unknown=frozenset({"MissingStep"}))

    assert error.unknown == frozenset({"MissingStep"})
    assert "['MissingStep']" in str(error)


def test_the_include_keyword_still_builds_the_error_with_a_deprecation_warning() -> None:
    with pytest.warns(DeprecationWarning, match="unknown="):
        error = InvalidStageError(include=frozenset({"MissingStep"}))

    assert error.unknown == frozenset({"MissingStep"})
    assert "['MissingStep']" in str(error)


@pytest.mark.parametrize(
    "kwargs",
    [{}, {"unknown": frozenset({"A"}), "include": frozenset({"A"})}],
    ids=["neither", "both"],
)
def test_the_names_are_given_exactly_once(kwargs: dict[str, frozenset[str]]) -> None:
    with pytest.raises(TypeError, match="InvalidStageError"):
        InvalidStageError(**kwargs)


def test_invalid_stage_error_keeps_its_unknown_names_across_pickle() -> None:
    error = InvalidStageError(frozenset({"MissingStep", "MissingProcess"}))
    error.add_note("selected by the run")

    restored = pickle.loads(pickle.dumps(error))

    assert type(restored) is InvalidStageError
    assert restored.unknown == error.unknown
    assert str(restored) == str(error)
    assert restored.__notes__ == ["selected by the run"]
