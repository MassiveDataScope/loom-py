"""StrictStruct at run time, and LoomFrozenStruct unchanged."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.model import LoomFrozenStruct, StrictStruct
from loom.core.model.redaction import Secret


class Quote(StrictStruct):
    amount_cents: int
    password: Secret[str] | None = None


class Child(Quote):
    note: str = ""


class Open(StrictStruct, forbid_unknown_fields=False):
    row_id: int


def test_strict_struct_is_frozen_keyword_only_closed_and_redacted() -> None:
    with pytest.raises(msgspec.ValidationError, match="unknown field"):
        msgspec.json.decode(b'{"amount_cents": 1, "extra": 2}', type=Quote)
    with pytest.raises(TypeError):
        Quote(1)  # type: ignore[misc]  # pyright: ignore[reportCallIssue]
    with pytest.raises(TypeError):
        Child(1)  # type: ignore[misc]  # pyright: ignore[reportCallIssue]
    with pytest.raises(AttributeError):
        Quote(amount_cents=1).amount_cents = 2  # type: ignore[misc]  # pyright: ignore[reportAttributeAccessIssue]
    with pytest.raises(TypeError, match="password must be Secret"):
        Quote(amount_cents=1, password="plain")  # type: ignore[arg-type]
    assert msgspec.json.encode(Quote(amount_cents=5)) == b'{"amount_cents":5,"password":null}'


def test_a_subclass_can_reopen_unknown_fields_and_positional_args() -> None:
    assert msgspec.json.decode(b'{"row_id": 1, "x": 2}', type=Open) == Open(row_id=1)

    class Positional(StrictStruct, kw_only=False):
        a: int

    assert Positional(1) == Positional(a=1)


class _Plain(msgspec.Struct, frozen=True):
    name: str


class _Loom(LoomFrozenStruct):
    name: str


def test_loom_frozen_struct_config_is_unchanged() -> None:
    names = [n for n in dir(_Plain.__struct_config__) if not n.startswith("_")]
    loom, plain = _Loom.__struct_config__, _Plain.__struct_config__
    assert [getattr(loom, n) for n in names] == [getattr(plain, n) for n in names]
