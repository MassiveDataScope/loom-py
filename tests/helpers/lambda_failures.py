"""Assert on the failure a Lambda boundary replaced with its message-free error."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import pytest

from loom.serverless.aws.lambda_.errors import LambdaInvocationError


@dataclass
class ReplacedFailure:
    """The failure the boundary replaced, once the block raised."""

    value: BaseException | None = None


@contextmanager
def lambda_failure(
    kind: type[BaseException], match: str | None = None
) -> Iterator[ReplacedFailure]:
    """Expect the block to raise the boundary's error in place of a *kind* matching *match*.

    The error the runtime would report names the failure's type only.
    """
    replaced = ReplacedFailure()
    with pytest.raises(LambdaInvocationError) as raised:
        yield replaced
    failure = raised.value.failure
    assert isinstance(failure, kind), repr(failure)
    if match is not None:
        assert re.search(match, str(failure)), str(failure)
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__
    replaced.value = failure
