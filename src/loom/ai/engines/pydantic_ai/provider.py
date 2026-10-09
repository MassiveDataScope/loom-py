"""Entry-point target of the pydantic-ai engine (group ``loom.ai.engines``).

The provider is what the ``ai.engine: pydantic-ai`` setting resolves to. It
builds one engine per plan — called exactly once per plan by
:class:`~loom.ai.runtime.AgentRuntime`, in ``__aenter__``, never per request —
and answers which capability kinds this adapter can serve.

The plan's output schema reaches the engine through the spec; a pinned
``output_mode`` on the model binding reaches it as ``output_type=`` on
``Agent.from_spec`` (see :func:`~loom.ai.engines.pydantic_ai._spec.build_output_type`),
and that keyword is absent when no mode is pinned. A pydantic output is the
one case where the keyword is never absent: :func:`build_output_type` hands
``Agent.from_spec`` the model class itself, wrapped or bare, so pydantic-ai
owns validation and retries for that answer (D7, FR-012).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pydantic_ai import Agent, ModelRetry
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.models import Model

from loom.ai.abc import AgentEngine, DepsFactory, OutputCheck
from loom.ai.compiler import AgentPlan, CompiledMcpCapability
from loom.ai.engines.pydantic_ai._a2a import create_a2a_client
from loom.ai.engines.pydantic_ai._capabilities import (
    SUPPORTED_KINDS,
    build_capabilities,
    build_toolsets,
)
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.engines.pydantic_ai._instructions import (
    build_instructions,
    ensure_templating_available,
)
from loom.ai.engines.pydantic_ai._limits import (
    ConfiguredPrice,
    is_priceable,
    usage_limits,
    warn_if_model_not_priceable,
)
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.engines.pydantic_ai._models import ModelResolver, resolve_model
from loom.ai.engines.pydantic_ai._native import supported_native_tools
from loom.ai.engines.pydantic_ai._spec import build_agent_spec, build_output_type
from loom.ai.errors import AgentCompilationError, streaming_required_by_capability
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.model import LoomType

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from loom.ai.runtime import McpSession


class PydanticAIEngineProvider:
    """Builds pydantic-ai engines from compiled plans.

    Args:
        model_resolver: Builds the engine model from the plan's resolved
            binding. Defaults to the provider bindings of
            :mod:`~loom.ai.engines.pydantic_ai._models`; a deployment supplies
            one to inject a preconfigured vendor client, and the shared engine
            contract suite supplies one to exercise this adapter with no
            network and no credentials (FR-048).

    Every MCP toolset this provider builds starts at
    :data:`~loom.ai.engines.pydantic_ai._mcp.DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS`
    for its connection and ``initialize`` handshake (FR-051), because
    :func:`~loom.ai.registry.resolve_engine_provider` constructs this class
    with no arguments. The composition root instead calls
    :meth:`configure_mcp_connect_timeout` once it has read
    ``ai.startup_timeout_ms``.

    One instance serves one runtime lifecycle. It holds the worker's shared
    MCP toolsets, which are never evicted, so re-entering the same provider
    from a second event loop would reuse a connection lock bound to the first.
    ``create_app`` builds one provider per application.

    Attributes:
        LOOM_AI_ENGINE_API: Handshake version, read with ``getattr`` on load.

    Example::

        provider = PydanticAIEngineProvider()
        provider.configure_mcp_connect_timeout(ai_cfg.startup_timeout_ms / 1000)
        engine = provider.create_engine(plan, deps=deps, container=container)
    """

    LOOM_AI_ENGINE_API: ClassVar[int] = 2

    a2a_client_factory = staticmethod(create_a2a_client)
    """Client factory for ``a2a`` grants, read off the provider by the
    composition root so it never imports this engine (FR-016, FR-051)."""

    def __init__(self, *, model_resolver: ModelResolver | None = None) -> None:
        self._resolve_model: ModelResolver = model_resolver or resolve_model
        self._mcp = SharedMcpToolsets()

    def configure_mcp_connect_timeout(self, seconds: float) -> None:
        """Replace the MCP handshake deadline every toolset built from now on waits for.

        Read structurally, with ``getattr``, by the composition root (FR-051),
        so a third-party engine that declares no such method is simply left
        at its own default. Has no effect on a connection whose toolset was
        already built: call this before the runtime opens its clients.

        Args:
            seconds: New handshake deadline, in seconds.
        """
        self._mcp.set_connect_timeout(seconds)

    def mcp_client_factory(
        self, capability: CompiledMcpCapability
    ) -> AbstractAsyncContextManager[McpSession]:
        """Open the worker's shared session for one ``mcp`` grant.

        Read off the provider by the composition root the same way
        ``a2a_client_factory`` is (FR-016, FR-051). It is an instance method,
        not a free function, because the session it opens is the very toolset
        :meth:`create_engine` puts behind the capability boundary: one
        connection per server for the whole worker, not one for start-up plus
        one per agent (FR-026).

        Args:
            capability: Compiled grant naming the server to reach.

        Returns:
            The not-yet-opened client; entering it connects the server.
        """
        return self._mcp.open(capability)

    def create_engine(
        self, plan: object, *, deps: DepsFactory, container: LoomContainer
    ) -> AgentEngine:
        """Build the engine serving one compiled plan.

        ``deps_type=object`` stays: this seam receives ``deps: DepsFactory``,
        whose ``build`` returns ``object`` (``loom.ai.abc``), so the concrete
        bundle type is not visible here to pass instead — and it still feeds
        ``pydantic_ai.agent._validate_spec``'s template validation context,
        which is what a templated block is checked against at compile time.

        Args:
            plan: The compiled :class:`~loom.ai.compiler.AgentPlan`.
            deps: Per-invocation dependency factory.
            container: Application container.

        Returns:
            The engine serving this plan.

        Raises:
            TypeError: When ``plan`` is not an ``AgentPlan``.
            AgentCompilationError: When the vendor SDK the binding needs is not
                installed, a required provider setting is missing, a
                templated instruction block needs the templating extra and
                it is not installed, a templated block fails to compile, or
                the plan sets ``streaming: false`` while a built capability
                overrides ``wrap_run_event_stream``.
        """
        if not isinstance(plan, AgentPlan):
            raise TypeError(f"expected an AgentPlan, got {type(plan).__name__}")
        ensure_templating_available(plan)
        model = self._resolve_model(plan.inference)
        if plan.price is None:
            warn_if_model_not_priceable(
                model.model_name, _provider_name(model), plan.policies, f"agent '{plan.name}'"
            )
        toolsets = build_toolsets(plan, container, mcp=self._mcp)
        capabilities = _with_price(build_capabilities(plan, container), plan)
        if not plan.inference.streaming and any(
            capability.has_wrap_run_event_stream for capability in capabilities
        ):
            raise AgentCompilationError([streaming_required_by_capability(plan.name)])
        output_type = build_output_type(plan)
        # The keyword is absent, not ``None``, when no mode is pinned: the
        # engine's default for ``output_type`` is ``str``, and passing ``None``
        # would override the resolution ``output_schema`` alone triggers.
        pinned: dict[str, Any] = {} if output_type is None else {"output_type": output_type}
        spec = build_agent_spec(plan)
        agent_kwargs: dict[str, Any] = {
            "model": model,
            "deps_type": object,
            "instructions": build_instructions(plan),
            "description": plan.description,
            "toolsets": toolsets or None,
            "capabilities": capabilities or None,
            **pinned,
        }
        agent = Agent.from_spec(spec, **agent_kwargs)
        shaped_agent = agent
        if plan.output_check is not None:
            _register_output_check(agent, plan.output.loom_type, plan.output_check)
            # A run overriding the plan's shape (``AgentHandle.run(expect=...)``,
            # ``run_text``) must not carry the check above: pydantic-ai refuses
            # a custom ``output_type`` on a run whenever the agent holds an
            # output validator (``UserError``), and the check was compiled
            # against the plan's own declared schema, not the override's. A
            # second agent, built from the same spec and kwargs but with no
            # validator registered, serves that path instead
            # (:meth:`PydanticAIEngine.run_stream_shaped`); built here, at
            # start-up, alongside the checked one — never lazily or cached
            # per call.
            shaped_agent = Agent.from_spec(spec, **agent_kwargs)
        return PydanticAIEngine(
            plan=plan,
            agent=agent,
            shaped_agent=shaped_agent,
            deps=deps,
            container=container,
            usage_limits=usage_limits(plan.policies),
        )

    def native_tool_support(self, target: InferenceTarget) -> frozenset[str]:
        """Return the provider tools the model bound to *target* admits.

        Satisfies :data:`~loom.ai.abc.NativeToolSupport`, which the bootstrap
        hands to the compiler so a ``native`` grant fails at compile time rather
        than on the first request.

        Args:
            target: Resolved model binding of the agent's role.

        Returns:
            The tool names this binding admits.

        Raises:
            AgentCompilationError: When the provider is unknown or its SDK is
                not installed.
        """
        return supported_native_tools(target)

    def prices_model(self, target: InferenceTarget) -> bool:
        """Report whether the model bound to *target* is priced without ``ai.prices``.

        Read structurally by :func:`~loom.ai.registry.engine_prices_model`.
        Builds the model as :meth:`create_engine` would, so the answer is the
        one billing itself will give, and sends no request.

        Args:
            target: Resolved model binding of the agent's role.

        Returns:
            Whether genai-prices knows the built model.

        Raises:
            AgentCompilationError: When the provider is unknown or its SDK is
                not installed.
        """
        model = self._resolve_model(target)
        return is_priceable(model.model_name, _provider_name(model))

    def supported_capability_kinds(self) -> frozenset[str]:
        """Capability kinds this adapter can serve.

        Derived from the one table that says how each kind is built, so a kind
        this adapter announces always has a builder behind it.

        Returns:
            The supported ``kind`` identifiers.
        """
        return SUPPORTED_KINDS


def _provider_name(model: Model) -> str | None:
    """Return the name of the provider behind *model*, or ``None`` for a bare test double."""
    return model.provider.name if model.provider is not None else None


def _with_price(
    capabilities: tuple[AbstractCapability[Any], ...], plan: AgentPlan
) -> tuple[AbstractCapability[Any], ...]:
    """Append the plan's configured price, when it declares one, to *capabilities*."""
    if plan.price is None:
        return capabilities
    return (*capabilities, ConfiguredPrice(price=plan.price))


def _register_output_check(agent: Agent[Any, Any], loom_type: LoomType, check: OutputCheck) -> None:
    """Register *check* on *agent* as a pydantic-ai output validator.

    A rejection is a :class:`~pydantic_ai.ModelRetry` carrying the text
    *check* returned, which the engine reads back as the correction the
    model must act on (see :data:`~loom.ai.abc.OutputCheck`). The validator
    receives *check*'s own contract, a ``Mapping``: for a ``msgspec.Struct``
    output the mapping pydantic-ai parsed is passed straight through; for a
    pydantic output the validator receives the validated model instance and
    calls *check* with :meth:`~loom.core.model.LoomType.to_builtins` of it,
    so a check written once serves both libraries. The validator returns its
    argument unchanged on acceptance in both cases: the engine never uses
    that return value to build the answer (a ``msgspec.Struct`` answer is
    decoded independently by
    :func:`~loom.ai.engines.pydantic_ai._output.decode_output`; a pydantic
    answer is pydantic-ai's own validated instance), so a mapping this
    validator built could only ever be discarded.

    Args:
        agent: Agent built by :meth:`PydanticAIEngineProvider.create_engine`.
        loom_type: The plan's compiled output type, read for its library and
            :meth:`~loom.core.model.LoomType.to_builtins`.
        check: Resolved :data:`~loom.ai.abc.OutputCheck` of the plan.
    """
    is_pydantic = loom_type.library == "pydantic"

    @agent.output_validator
    def _validate(output: Any) -> Any:
        payload = loom_type.to_builtins(output) if is_pydantic else output
        rejection = check(payload)
        if rejection is not None:
            raise ModelRetry(rejection)
        return output
