"""Assert on the failure a Lambda edge handed the runtime as a reference."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import pytest

from loom.core.logger.failure import FailureReference
from loom.serverless.aws.lambda_ import boundary


@dataclass
class ReplacedFailure:
    """The failure the edge replaced, once the block raised."""

    value: BaseException | None = None


@contextmanager
def lambda_failure(
    kind: type[BaseException], match: str | None = None
) -> Iterator[ReplacedFailure]:
    """Expect the block to raise a reference in place of a *kind* matching *match*.

    The reference holds no original; a spy on the edge keeps the failure it
    replaced, in this process only, so the test can still read it. The
    reference is raised with no ``__context__``, its class named after the
    original type.
    """
    replaced = ReplacedFailure()
    seen: list[BaseException] = []
    original = boundary.reference

    def spy(failure: BaseException) -> FailureReference:
        seen.append(failure)
        return original(failure)

    boundary.reference = spy
    try:
        with pytest.raises(FailureReference) as raised:
            yield replaced
    finally:
        boundary.reference = original
    failure = seen[0]
    assert isinstance(failure, kind), repr(failure)
    if match is not None:
        assert re.search(match, str(failure)), str(failure)
    assert (raised.value.__cause__, raised.value.__context__) == (None, None)
    assert type(raised.value).__name__ == type(failure).__name__
    replaced.value = failure
