"""Assert on what crossed the loom boundary: a clean loom exception, never foreign text."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import pytest

from loom.core.errors import UnexpectedError
from loom.core.errors.failure import Failure


def type_name(kind: type[BaseException]) -> str:
    """The fully qualified name loom writes for *kind*, as ``exception.type`` holds it."""
    if kind.__module__ == "builtins":
        return kind.__qualname__
    return f"{kind.__module__}.{kind.__qualname__}"


@dataclass
class LeftLoom:
    """What left loom, once the block raised."""

    value: BaseException | None = None


@contextmanager
def left_loom(kind: type[BaseException], match: str | None = None) -> Iterator[LeftLoom]:
    """Expect the block to raise what the boundary lets out in place of a *kind*.

    Loom's own exception leaves as itself, matching *match*; a foreign one as
    an :class:`~loom.core.errors.UnexpectedError` naming its type, whose text
    *match* cannot reach. Either way the chain and the notes are gone.
    """
    crossed = LeftLoom()
    with pytest.raises(Exception) as raised:
        yield crossed
    leaving = raised.value
    if Failure(kind.__new__(kind)).is_loom_own:
        assert isinstance(leaving, kind), repr(leaving)
        if match is not None:
            assert re.search(match, str(leaving)), str(leaving)
    else:
        assert isinstance(leaving, UnexpectedError), repr(leaving)
        assert leaving.original_type == type_name(kind), leaving.original_type
    assert (leaving.__cause__, leaving.__context__) == (None, None)
    assert not hasattr(leaving, "__notes__")
    crossed.value = leaving
