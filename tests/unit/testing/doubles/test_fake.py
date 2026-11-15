from __future__ import annotations

import pytest

from loom.core.identity import Identity
from loom.testing.golden import GoldenHarness
from tests.unit.testing.doubles import AgentTriage, FakeTriage, Triage, TriageUseCase


class TestRegisterInstanceOfAFakePort:
    @pytest.mark.parametrize("fake_first", [True, False], ids=["fake-first", "adapter-first"])
    async def test_the_fake_stands_in_and_no_adapter_is_built(
        self, harness: GoldenHarness, caller: Identity, fake_first: bool
    ) -> None:
        fake = FakeTriage()
        if fake_first:
            harness.register_instance(Triage, fake)
            harness.register(Triage, AgentTriage)
        else:
            harness.register(Triage, AgentTriage)
            harness.register_instance(Triage, fake)

        result = await harness.run(TriageUseCase, params={"incident": "INC-1"}, identity=caller)

        assert result == "fake"
        assert fake.incidents == ["INC-1"]
        assert AgentTriage.built == 0

    async def test_a_fake_port_needs_no_double_across_runs(
        self, harness: GoldenHarness, caller: Identity
    ) -> None:
        fake = FakeTriage()
        harness.register_instance(Triage, fake)

        for incident in ("INC-1", "INC-2", "INC-3"):
            await harness.run(TriageUseCase, params={"incident": incident}, identity=caller)

        assert fake.incidents == ["INC-1", "INC-2", "INC-3"]
        assert AgentTriage.built == 0

    async def test_force_error_applies_to_a_registered_instance(
        self, harness: GoldenHarness, caller: Identity
    ) -> None:
        harness.register_instance(Triage, FakeTriage())
        harness.force_error(Triage, "triage", LookupError("down"))

        with pytest.raises(LookupError, match="down"):
            await harness.run(TriageUseCase, params={"incident": "INC-1"}, identity=caller)
