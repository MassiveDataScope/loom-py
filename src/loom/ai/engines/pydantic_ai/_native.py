"""Provider-run tools: the loom name of each, and which binding admits it.

The mapping is by class, never by the engine's own identifier, so a rename in
pydantic-ai fails the import of this module instead of leaving a grant silently
unsupported.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from pydantic_ai.capabilities import NativeTool
from pydantic_ai.native_tools import (
    AbstractNativeTool,
    CodeExecutionTool,
    WebFetchTool,
    WebSearchTool,
)

from loom.ai.compiler._plan import CompiledNativeCapability
from loom.ai.engines.pydantic_ai._models import model_class_for

if TYPE_CHECKING:
    from collections.abc import Mapping

    from loom.ai.inference import InferenceTarget

TOOL_CLASSES: Final[Mapping[str, type[AbstractNativeTool]]] = MappingProxyType(
    {
        "web_search": WebSearchTool,
        "web_fetch": WebFetchTool,
        "code_execution": CodeExecutionTool,
    }
)
"""Provider tool class behind every name an artifact may declare."""


def supported_native_tools(target: InferenceTarget) -> frozenset[str]:
    """Return the tool names the model class bound to *target* admits.

    Answered from the class the provider binds, so no model is instantiated and
    no credential is read. The class is the upper bound: pydantic-ai narrows it
    again per model name when the request is made.

    Raises:
        AgentCompilationError: When the provider is unknown or its SDK missing.
    """
    admitted = model_class_for(target).supported_native_tools()
    return frozenset(name for name, cls in TOOL_CLASSES.items() if cls in admitted)


def native_capability(capability: CompiledNativeCapability) -> NativeTool[object]:
    """Build the engine capability of one granted provider tool.

    Args:
        capability: Compiled grant naming a tool already checked against the model.

    Returns:
        The capability the engine hands to the model provider.
    """
    return NativeTool(TOOL_CLASSES[capability.tool]())
