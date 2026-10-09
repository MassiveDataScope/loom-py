"""Assembly of an agent runtime from a deployment's ``ai:`` section.

The one path every transport takes from configuration to a live
:class:`~loom.ai.runtime.AgentRuntime`: resolve the engine, load and compile
the artifacts, and hand both to the runtime, which opens nothing until it is
entered.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from pathlib import Path
from typing import Any

from loom.ai.abc import AgentEngineProvider, DepsFactory
from loom.ai.compiler import AgentCompiler
from loom.ai.config import AiConfig
from loom.ai.declarative import DecodedSpec, load_specs
from loom.ai.registry import (
    configure_engine_mcp_connect_timeout,
    engine_client_factories,
    engine_native_tool_support,
    engine_supported_kinds,
    resolve_engine_provider,
)
from loom.ai.runtime import AgentRuntime, UseCaseMcpGrant
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.sql.config import SqlConfig
from loom.core.use_case.registry import UseCaseRegistry

__all__ = ["build_agent_runtime"]


class _NoDependencies(DepsFactory):
    """Dependency factory of a runtime whose agents reach no application service."""

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        return None


def build_agent_runtime(
    config: AiConfig,
    *,
    root: Path | str,
    names: Collection[str] | None = None,
    specs: Sequence[str] | None = None,
    engine_provider: AgentEngineProvider | None = None,
    registry: UseCaseRegistry | None = None,
    container: LoomContainer | None = None,
    deps: DepsFactory | None = None,
    sql: SqlConfig | None = None,
    use_case_mcp: Sequence[UseCaseMcpGrant] = (),
) -> AgentRuntime:
    """Compile the declared artifacts and build the runtime that serves them.

    Compilation is offline and complete: every issue of every artifact is
    reported at once. No client opens until the returned runtime is entered.

    Args:
        config: Parsed ``ai:`` section.
        root: Directory the artifact globs are resolved against; no glob may
            leave it.
        names: Agents to serve; ``None`` serves every artifact found.
        specs: Artifact globs replacing ``config.specs``.
        engine_provider: Engine provider to build engines with; resolved from
            ``config.engine`` when omitted.
        registry: Use cases ``usecase`` grants and hooks resolve against;
            empty when omitted.
        container: Application container engines resolve services from;
            empty when omitted.
        deps: Per-invocation dependency factory; when omitted every run gets
            no dependency bundle.
        sql: Data-layer configuration ``sql`` grants resolve against.
        use_case_mcp: Compiled ``Mcp()`` marker grants of the application's
            use cases.

    Returns:
        The runtime, not yet entered.

    Raises:
        AgentCompilationError: When the engine cannot be resolved, a glob
            leaves *root*, or an artifact does not compile.
    """
    provider = engine_provider or resolve_engine_provider(config.engine)
    configure_engine_mcp_connect_timeout(provider, config.startup_timeout_ms / 1000)
    mcp_factory, a2a_factory = engine_client_factories(provider)
    compiler = AgentCompiler(
        config=config,
        registry=registry or UseCaseRegistry.build([]),
        supported_kinds=engine_supported_kinds(provider, config.engine),
        sql=sql,
        native_tools=engine_native_tool_support(provider),
    )
    decoded = load_specs(config.specs if specs is None else specs, root=root)
    plans = compiler.compile_all(_named(decoded, names))
    return AgentRuntime(
        plans=plans,
        config=config,
        engine_provider=provider,
        deps=deps or _NoDependencies(),
        container=container or LoomContainer(),
        sql_config=sql,
        mcp_client_factory=mcp_factory,
        a2a_client_factory=a2a_factory,
        use_case_mcp=use_case_mcp,
    )


def _named(
    decoded: Sequence[DecodedSpec], names: Collection[str] | None
) -> tuple[DecodedSpec, ...]:
    if names is None:
        return tuple(decoded)
    return tuple(item for item in decoded if item.spec.name in names)
