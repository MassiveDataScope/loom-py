"""Every island of the engine fails start-up naming its extra when its SDK is absent.

The SDK is blocked and the island evicted, so the next load re-imports the
island against the blocked SDK: that is the exact path a deployment without
the extra takes, coded issue included.
"""

from __future__ import annotations

import pytest

from loom.ai.engines.pydantic_ai._models import model_class_for, resolve_model
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.model.redaction import Dsn
from tests.helpers.extras import ENGINE_PROVIDERS, without_extra


class TestAProviderWithoutItsSdk:
    @pytest.mark.parametrize(
        ("provider", "sdk", "extra"),
        [
            ("typesafe", "pydantic_ai.models.typesafe", "ai-typesafe"),
            ("anthropic", "pydantic_ai.models.anthropic", "ai-anthropic"),
            ("bedrock", "pydantic_ai.models.bedrock", "ai-bedrock"),
        ],
    )
    def test_fails_start_up_naming_the_extra(
        self, provider: str, sdk: str, extra: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``PROVIDER_NOT_INSTALLED`` names the extra, never the missing module."""
        without_extra(monkeypatch, [sdk], [f"{ENGINE_PROVIDERS}.{provider}"])
        target = InferenceTarget(provider=provider, model="a-model", region="eu-west-1")

        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(target)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.PROVIDER_NOT_INSTALLED
        assert extra in issue.message
        assert provider in issue.message

    def test_the_model_class_question_fails_the_same_way(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Asking which class a binding uses needs the island too, and says so."""
        without_extra(
            monkeypatch, ["pydantic_ai.models.typesafe"], [f"{ENGINE_PROVIDERS}.typesafe"]
        )

        with pytest.raises(AgentCompilationError) as failure:
            model_class_for(InferenceTarget(provider="typesafe", model="jev-latest"))

        assert failure.value.issues[0].code is AgentErrorCode.PROVIDER_NOT_INSTALLED

    def test_the_gateway_names_the_openai_extra(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``gateway`` rides on the OpenAI island, so its missing SDK names ``ai-openai``."""
        without_extra(
            monkeypatch,
            ["pydantic_ai.models.openai"],
            [f"{ENGINE_PROVIDERS}.openai", f"{ENGINE_PROVIDERS}.gateway"],
        )
        target = InferenceTarget(
            provider="gateway", model="m", endpoint=Dsn("https://gw.example.com/v1")
        )

        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(target)

        assert "ai-openai" in failure.value.issues[0].message
