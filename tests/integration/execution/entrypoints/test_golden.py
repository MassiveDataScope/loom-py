"""The golden harness builds the use case inside the execution it runs."""

from __future__ import annotations

import pytest

from loom.core.errors import Unauthenticated
from loom.testing.golden import GoldenHarness

from .doubles import ALICE, AcmeBuiltAs


class TestGoldenHarness:
    async def test_builds_inside_an_execution_carrying_the_given_identity(self) -> None:
        seen = await GoldenHarness().run(AcmeBuiltAs, identity=ALICE)

        assert seen == {"built_as": ALICE.subject, "same_execution": True}

    async def test_given_no_identity_a_constructor_caller_is_refused_as_unauthenticated(
        self,
    ) -> None:
        with pytest.raises(Unauthenticated, match="identity="):
            await GoldenHarness().run(AcmeBuiltAs)
