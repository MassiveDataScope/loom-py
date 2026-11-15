"""Doubles shared by the engine unit tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel


class AcmeToolsetFactory:
    """Hands out the toolsets it was given, in order, and counts how many it built."""

    def __init__(self, *toolsets: Any) -> None:
        self._toolsets = list(toolsets)
        self.built = 0

    def __call__(self) -> Any:
        self.built += 1
        return self._toolsets.pop(0)


def damaged_stream_model(payload: bytes) -> FunctionModel:
    """A model whose stream drops the first two characters of every tool call."""
    text = payload.decode()

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, text)])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        name = info.output_tools[0].name
        damaged = DeltaToolCall(name=name, json_args=text[2:], tool_call_id="c")
        yield {0: damaged}

    return FunctionModel(respond, stream_function=stream)
