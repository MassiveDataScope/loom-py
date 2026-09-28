from __future__ import annotations

import pytest

from loom.core.authz import Scope

ROOT = Scope.root()
T = Scope.of("T")
TX = Scope.of("T", "x")
TXY = Scope.of("T", "x", "y")
TZ = Scope.of("T", "z")
U = Scope.of("U")

COVERS = {
    ROOT: {ROOT, T, TX, TXY, TZ, U},
    T: {T, TX, TXY, TZ},
    TX: {TX, TXY},
    TXY: {TXY},
    TZ: {TZ},
    U: {U},
}

PAIRS = [(a, b, b in covered) for a, covered in COVERS.items() for b in COVERS]


@pytest.mark.parametrize(("outer", "inner", "expected"), PAIRS, ids=lambda v: str(v))
def test_coverage_truth_table(outer: Scope, inner: Scope, expected: bool) -> None:
    assert outer.covers(inner) is expected


def test_child_extends_the_path() -> None:
    assert T.child("x", "y") == TXY
    assert ROOT.child("T") == T


@pytest.mark.parametrize("scope", [ROOT, T, TXY])
def test_parse_reads_back_the_rendered_text(scope: Scope) -> None:
    assert Scope.parse(str(scope)) == scope


@pytest.mark.parametrize("text", ["", "T", "/T/", "//", "/T//x"])
def test_parse_rejects_malformed_text(text: str) -> None:
    with pytest.raises(ValueError):
        Scope.parse(text)


def test_any_sequence_of_segments_becomes_a_hashable_path() -> None:
    segments = ["T", "x"]
    scope = Scope(segments)
    segments.append("y")

    assert scope == TX
    assert hash(scope) == hash(TX)


@pytest.mark.parametrize("path", ["acme", ("T", 1)])
def test_rejects_a_bare_string_or_a_non_string_segment(path: object) -> None:
    with pytest.raises(TypeError):
        Scope(path)  # type: ignore[arg-type]


def test_scopes_sort_by_path() -> None:
    assert sorted([U, TX, ROOT, T]) == [ROOT, T, TX, U]


@pytest.mark.parametrize(("scope", "text"), [(ROOT, "/"), (T, "/T"), (TXY, "/T/x/y")])
def test_renders_as_a_path(scope: Scope, text: str) -> None:
    assert str(scope) == text


def test_of_requires_a_segment() -> None:
    with pytest.raises(ValueError, match="root"):
        Scope.of()


@pytest.mark.parametrize("segments", [("",), ("T", ""), ("T", "a/b")])
def test_rejects_invalid_segments(segments: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="segment"):
        Scope.of(*segments)


def test_child_rejects_invalid_segments() -> None:
    with pytest.raises(ValueError, match="segment"):
        T.child("")
