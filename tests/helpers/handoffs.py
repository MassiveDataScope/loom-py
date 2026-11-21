"""Assert on the failure a handoff replaced, or an exit printed the public form of, in process."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from types import ModuleType

import pytest

from loom.cli import check as cli_check
from loom.cli import deploy as cli_deploy
from loom.cli import schema as cli_schema
from loom.core.logger.failure import FailureReference, public_form
from loom.prefect.flow import _assemble
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


@contextmanager
def prefect_failure(
    kind: type[BaseException], match: str | None = None
) -> Iterator[ReplacedFailure]:
    """Expect a flow built by loom to hand Prefect a reference in place of a *kind*."""
    with handed_off(_assemble, kind, match) as replaced:
        yield replaced


class EchoedFailures:
    """The failures the ``loom`` command printed the public form of, as the command saw them."""

    def __init__(self) -> None:
        self.failures: list[BaseException] = []

    def rendered(self, exc: BaseException) -> str:
        """Record *exc* and return its public form, as the command prints it."""
        self.failures.append(exc)
        return public_form(exc)

    @property
    def messages(self) -> str:
        """The messages of every failure printed, one per line: what the command kept out."""
        return "\n".join(str(failure) for failure in self.failures)


@contextmanager
def echoed_failures() -> Iterator[EchoedFailures]:
    """Record each failure a ``loom`` command prints, so a test can still match its message."""
    echoed = EchoedFailures()
    modules = (cli_check, cli_deploy, cli_schema)
    originals = [module.public_form for module in modules]
    for module in modules:
        module.public_form = echoed.rendered
    try:
        yield echoed
    finally:
        for module, original in zip(modules, originals, strict=True):
            module.public_form = original
