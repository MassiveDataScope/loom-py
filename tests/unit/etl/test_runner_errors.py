"""Tests for InvalidStageError."""

from __future__ import annotations

import pickle

import pytest

from loom.etl.compiler import InvalidStageError


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


def test_the_include_keyword_is_refused() -> None:
    names = frozenset({"MissingStep"})
    with pytest.raises(TypeError, match="include"):
        InvalidStageError(include=names)  # type: ignore[call-arg]


def test_the_unknown_names_are_required() -> None:
    with pytest.raises(TypeError, match="unknown"):
        InvalidStageError()  # type: ignore[call-arg]


def test_invalid_stage_error_keeps_its_unknown_names_across_pickle() -> None:
    error = InvalidStageError(frozenset({"MissingStep", "MissingProcess"}))
    error.add_note("selected by the run")

    restored = pickle.loads(pickle.dumps(error))

    assert type(restored) is InvalidStageError
    assert restored.unknown == error.unknown
    assert str(restored) == str(error)
    assert restored.__notes__ == ["selected by the run"]
