"""Assert on the failure a handoff replaced with a reference, in this process only."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from types import ModuleType

import pytest

from loom.core.logger.failure import FailureReference
from loom.serverless.aws.lambda_ import boundary


@dataclass
class ReplacedFailure:
    """The failure the handoff replaced, once the block raised."""

    value: BaseException | None = None


@contextmanager
def handed_off(
    module: ModuleType, kind: type[BaseException], match: str | None = None
) -> Iterator[ReplacedFailure]:
    """Expect the block to raise a reference in place of a *kind* matching *match*.

    The reference holds no original; a spy on *module*'s ``reference`` keeps
    the failure it replaced, in this process only, so a test can still read
    it. The reference is raised with no ``__context__``.
    """
    replaced = ReplacedFailure()
    seen: list[BaseException] = []
    original = module.reference

    def spy(failure: BaseException) -> FailureReference:
        seen.append(failure)
        return original(failure)

    module.reference = spy
    try:
        with pytest.raises(FailureReference) as raised:
            yield replaced
    finally:
        module.reference = original
    failure = next(f for f in seen if not isinstance(f, FailureReference))
    assert isinstance(failure, kind), repr(failure)
    if match is not None:
        assert re.search(match, str(failure)), str(failure)
    assert (raised.value.__cause__, raised.value.__context__) == (None, None)
    replaced.value = failure


@contextmanager
def lambda_failure(
    kind: type[BaseException], match: str | None = None
) -> Iterator[ReplacedFailure]:
    """Expect a Lambda edge to hand off a *kind*, as a reference named after its type."""
    with handed_off(boundary, kind, match) as replaced:
        yield replaced
