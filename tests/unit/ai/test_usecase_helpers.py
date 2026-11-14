"""Contracts of the engine-free use-case helper in ``loom.ai._usecase``.

``require_invoker`` reads the bound invoker structurally from a dependency
bundle and refuses a bundle that carries none. The invoker it returns acts as
its bound caller through the executor; nothing installs an ambient identity
(spec 011, T09a).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from loom.ai._usecase import require_invoker
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.engine.compilable import Compilable
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.use_case.invoker import ApplicationInvoker, EntityInvoker


@dataclass
class _RecordingInvoker:
    """Fake invoker that records what it saw, including the ambient identity."""

    calls: list[dict[str, Any]] = field(default_factory=list)

    async def invoke(
        self,
        use_case: type[Compilable],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append(
            {
                "use_case": use_case,
                "params": params,
                "payload": payload,
                "identity": current_identity(),
            }
        )
        return "result"

    async def invoke_name(
        self,
        key: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        raise NotImplementedError

    def entity(self, model: type[Any]) -> EntityInvoker:
        raise NotImplementedError


@dataclass(frozen=True)
class _Bundle:
    invoker: object


def test_require_invoker_returns_the_invoker_when_the_bundle_exposes_one() -> None:
    invoker = _RecordingInvoker()

    assert require_invoker(_Bundle(invoker=invoker), "tool") is invoker


def test_require_invoker_rejects_a_bundle_without_an_invoker() -> None:
    with pytest.raises(AgentRunError) as excinfo:
        require_invoker({"identity": ANONYMOUS}, "tool 'lookup'")

    assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED
    assert str(excinfo.value).startswith("tool 'lookup' requires ")


def test_require_invoker_uses_the_label_verbatim_for_a_hook_caller() -> None:
    """The label names the caller verbatim: a hook is not reported as a tool."""
    with pytest.raises(AgentRunError) as excinfo:
        require_invoker({"identity": ANONYMOUS}, "on_output hook 'incidents.record_triage'")

    assert str(excinfo.value).startswith("on_output hook 'incidents.record_triage' requires ")


def test_require_invoker_rejects_an_attribute_that_is_not_an_invoker() -> None:
    bundle = _Bundle(invoker=object())

    with pytest.raises(AgentRunError) as excinfo:
        require_invoker(bundle, "tool")

    assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED


def test_the_fake_satisfies_the_invoker_protocol() -> None:
    """Guards the fake itself: ``require_invoker`` checks the protocol structurally."""
    assert isinstance(_RecordingInvoker(), ApplicationInvoker)
