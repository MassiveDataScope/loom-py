"""The kernel's container and executor share one kinds registry (F5)."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.bootstrap import create_kernel
from loom.core.engine.kinds import AGENT
from loom.core.execution import ExecutionRequired, ExecutionScope
from loom.core.identity import Identity
from loom.core.use_case.kinds import CALLER
from loom.core.use_case.markers import Agent, Caller
from tests.unit.core.bootstrap.kernel.components import AcmeConfig, AcmePing

_ACME = Identity(subject="acme-alice", mechanism="test")


class TestSharedKernelKinds:
    def test_the_kernel_executor_uses_the_container_kinds_with_the_caller_bound(self) -> None:
        kernel = create_kernel(config=AcmeConfig(), use_cases=[AcmePing])
        kinds = kernel.container.kinds

        assert kernel.executor._kinds is kinds  # noqa: SLF001 - the shared registry is the contract
        assert kinds.kind_for(Caller(), Identity) is CALLER
        assert kinds.kind_for(Agent("acme-triage"), Any) is AGENT
        with pytest.raises(ExecutionRequired):
            kinds.resolve(CALLER, Caller(), Identity)
        with ExecutionScope(_ACME):
            assert kinds.resolve(CALLER, Caller(), Identity) is _ACME
