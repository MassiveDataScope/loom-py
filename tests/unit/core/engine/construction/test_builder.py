"""``run(type, factory=None)`` (L9): a given factory overrides the executor's builder."""

from __future__ import annotations

import pytest

from loom.core.bootstrap.kernel import create_kernel
from tests.unit.core.engine.construction import acme


def _refusing(use_case_type: type[object]) -> object:
    raise AssertionError("the executor's builder was used instead of the given factory")


async def test_without_a_factory_the_builder_builds() -> None:
    executor = acme.executor(builder=acme.factory().build)

    probe = await executor.run(acme.AcmeProbe, identity=acme.ALICE)

    assert probe.session.caller is acme.ALICE


async def test_a_given_factory_overrides_the_builder() -> None:
    executor = acme.executor(builder=_refusing)

    probe = await executor.run(acme.AcmeProbe, factory=acme.factory(), identity=acme.ALICE)

    assert probe.session.caller is acme.ALICE


async def test_with_neither_run_names_both() -> None:
    with pytest.raises(TypeError, match=r"factory=.*builder="):
        await acme.executor().run(acme.AcmeProbe, identity=acme.ALICE)

    assert acme.JOURNAL.entries == []


class _Settings:
    pass


async def test_the_kernel_executor_builds_with_its_factory() -> None:
    kernel = create_kernel(
        config=_Settings(), use_cases=[acme.AcmeProbe], modules=[acme.serves_session]
    )

    probe = await kernel.executor.run(acme.AcmeProbe, identity=acme.ALICE)

    assert probe.session.caller is acme.ALICE
