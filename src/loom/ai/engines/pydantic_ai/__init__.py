"""pydantic-ai engine adapter, behind the ``ai-pydantic`` extra.

Importing this package requires ``pydantic-ai-slim``; nothing in ``loom.ai``
imports it. The runtime reaches :class:`PydanticAIEnginePlugin` through the
``loom.ai.engines`` entry point named ``pydantic-ai``.

The adapter is *native-inside*: an :class:`~loom.ai.compiler.AgentPlan` is
translated into a ``pydantic_ai.AgentSpec`` and handed to
``Agent.from_spec()``. Loom wraps none of the engine's primitives; it owns
only what the engine does not do — binding the model, validating the output
and classifying failures.

:func:`create_a2a_client` is exported beside the provider because it is not
part of the engine contract: it is the
:data:`~loom.ai.execution.A2AClientFactory` a composition root hands to
:class:`~loom.ai.execution.AgentRuntime`, so that an outbound grant is validated
against the live remote at start-up rather than on its first use. The MCP
counterpart is ``PydanticAIEnginePlugin.mcp_client_factory`` rather than a
free function, because the session it opens is the very toolset the run path
uses: one connection per server for the whole worker. :func:`create_mcp_client`
opens a session of its own and is for diagnostics, never for wiring the
runtime.

:func:`native_agent` is the escape hatch out of loom: an async context
manager over the agent this adapter built, handed to calling code so it can
drive pydantic-ai directly for what loom's neutral surface does not serve.
Being *this* package's export is the point — a caller reaching it has
imported an engine on purpose. It rejoins loom's chain and admission bounds
around its body; see :meth:`~loom.ai.abc.AgentHandle.native` for what a
driven run keeps and loses. :func:`as_run_error` is exported alone for a
driver that wants loom's error classification without that context manager.
"""

from __future__ import annotations

from loom.ai.engines.pydantic_ai._a2a import create_a2a_client
from loom.ai.engines.pydantic_ai._errors import as_run_error
from loom.ai.engines.pydantic_ai._escape import NativeAgent, native_agent
from loom.ai.engines.pydantic_ai._mcp import create_mcp_client
from loom.ai.engines.pydantic_ai.plugin import PydanticAIEnginePlugin

__all__ = [
    "NativeAgent",
    "PydanticAIEnginePlugin",
    "as_run_error",
    "create_a2a_client",
    "create_mcp_client",
    "native_agent",
]
