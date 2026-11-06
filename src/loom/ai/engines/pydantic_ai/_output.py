"""Output validation at the engine boundary for a ``msgspec.Struct`` plan — one decode.

This module serves the ``msgspec.Struct`` output path only. pydantic-ai's
``output_schema`` instructs the model and returns a plain dictionary
**without runtime validation** (research R-004), so loom validates. It does
so with the :class:`~loom.core.model.LoomType` the compiler already built,
over the model's raw JSON bytes: :meth:`~loom.core.model.LoomType.decode_json`
fuses validation and construction into a single pass, which is what
invariant 5 requires for this path. ``msgspec.convert()`` over a mapping is
never used here — walking a second object graph for the same payload is the
double pass the performance rules forbid.

A pydantic output satisfies invariant 5 differently and never reaches this
module (D7, FR-012): the compiled model class is pydantic-ai's own
``output_type``, so pydantic-ai validates the answer, feeds a rejection back
to the model and retries, entirely on its own side — zero loom decodes. The
engine reads that answer straight off ``result.output``
(:class:`~loom.ai.engines.pydantic_ai._engine.PydanticAIEngine`'s
``self._answer``) and projects it to builtins once, at the wire, exactly as
it projects a ``msgspec.Struct`` answer.

**Where the bytes come from.** The engine keeps the provider's own payload in
the final ``ModelResponse``: the output tool call's ``args``, or the text part
for a text answer. For every provider whose wire format is JSON text — OpenAI
and Anthropic among them — ``args`` is still the untouched string the model
produced, and loom decodes exactly that.

pydantic-ai builds its own ``result.output`` for every provider, so it parses
that payload once on its side whatever the vendor. That pass is the engine's
and is outside loom's budget; loom's own budget — the one the contract suite
counts — is the single decode below.

**Declared residual** (invariant 5, "residual"): some provider SDKs parse the
tool arguments before pydantic-ai sees them (the Bedrock Converse API returns
``toolUse.input`` already decoded). For those, the raw string no longer
exists, and the engine's own ``ToolCallPart.args_as_json_str()`` re-serialises
it. That pass belongs to the engine and is not hidden: loom still performs
exactly one decode and no encode, and the residual is confined to the branch
below.
"""

from __future__ import annotations

from typing import Any

from pydantic_ai import AgentRunResult
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart

from loom.ai.compiler import CompiledOutput
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.errors import BoundaryValidationError


class MissingOutputPayload(Exception):
    """The run produced no part carrying an answer payload."""


def _payload_part(response: ModelResponse) -> ToolCallPart | TextPart:
    """Return the part of a response that carries the answer, last one first."""
    for part in reversed(response.parts):
        if isinstance(part, ToolCallPart | TextPart):
            return part
    raise MissingOutputPayload("the final model response carries no answer part")


def raw_output(result: AgentRunResult[Any]) -> str | bytes:
    """Return the model's own JSON payload for the run's answer.

    Args:
        result: Completed engine run.

    Returns:
        The raw JSON text the model produced.

    Raises:
        MissingOutputPayload: When the run ended with no answer part at all.
    """
    for message in reversed(result.all_messages()):
        if isinstance(message, ModelResponse):
            part = _payload_part(message)
            if isinstance(part, TextPart):
                return part.content
            # ``args`` is the provider's untouched string whenever the wire
            # format kept it; ``args_as_json_str()`` is the engine's own
            # re-serialisation for SDKs that pre-parsed it (declared residual).
            return part.args if isinstance(part.args, str) else part.args_as_json_str()
    raise MissingOutputPayload("the run produced no model response")


def decode_output(output: CompiledOutput, result: AgentRunResult[Any]) -> object:
    """Validate and build the answer in one pass over the model's bytes.

    Args:
        output: Compiled output contract carrying the pre-built boundary type.
        result: Completed engine run.

    Returns:
        The validated answer, built by :meth:`~loom.core.model.LoomType.decode_json`.

    Raises:
        AgentRunError: With ``OUTPUT_SCHEMA_VIOLATION`` when the model's
            answer does not satisfy the declared shape — a failure of model
            behaviour, which FR-028 declares non-retriable at this level.
    """
    try:
        raw = raw_output(result)
        return output.loom_type.decode_json(raw)
    except (BoundaryValidationError, MissingOutputPayload) as exc:
        raise AgentRunError(
            AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION,
            f"the model's answer does not satisfy the declared output shape: {exc}",
        ) from exc
