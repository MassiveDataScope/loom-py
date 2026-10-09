"""Engine and provider resolution for the AI pillar.

Resolves the configured engine through :mod:`loom.core.plugins.entrypoints`
(group ``loom.ai.engines``, duplicates rejected — FR-021) and offers the two
helpers engines use to fail fast on missing provider SDKs and settings.

This module never imports a concrete engine: the entry point is loaded by
name and the handshake is a ``getattr`` on the loaded object, never an
``isinstance`` check, so the contract stays structural.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:  # import cycle at runtime: runtime.py imports the compiler,
    # which imports abc; the aliases are only needed for annotations.
    from loom.ai.runtime import A2AClientFactory, McpClientFactory

from types import ModuleType

from loom.ai.abc import AgentEngineProvider, NativeToolSupport
from loom.ai.errors import (
    AgentCompilationError,
    engine_api_mismatch,
    engine_duplicate,
    engine_not_found,
    provider_not_installed,
    provider_setting_missing,
)
from loom.ai.inference import InferenceTarget
from loom.core.plugins.entrypoints import (
    ApiVersionMismatchError,
    ApiVersionRequirement,
    DuplicateEntryPointError,
    EntryPointNotFoundError,
    check_api_version,
    load_entry_point,
)
from loom.core.plugins.optional import MissingExtraError, import_optional

_logger = logging.getLogger(__name__)

ENGINE_ENTRY_POINT_GROUP = "loom.ai.engines"
"""Entry-point group every engine distribution registers under."""

ENGINE_API_ATTRIBUTE = "LOOM_AI_ENGINE_API"
"""Attribute a provider declares its handshake version on."""

SUPPORTED_ENGINE_APIS: frozenset[int] = frozenset({2})
"""Handshake versions this release of loom accepts."""

_ENGINE_API_REQUIREMENT = ApiVersionRequirement(
    attribute=ENGINE_API_ATTRIBUTE,
    supported=SUPPORTED_ENGINE_APIS,
)


def resolve_engine_provider(name: str) -> AgentEngineProvider:
    """Resolve the engine provider registered under ``name``.

    Loads the entry point in group ``loom.ai.engines`` with duplicates
    rejected, instantiates it when it targets a class, and verifies the
    :data:`ENGINE_API_ATTRIBUTE` handshake via ``getattr`` on the resulting
    object, so an engine declaring its version in ``__init__`` is accepted.

    Args:
        name: Entry-point name from ``ai.engine``.

    Returns:
        The provider instance, handshake verified.

    Raises:
        AgentCompilationError: With ``ENGINE_NOT_FOUND`` (listing the
            installed engines), ``ENGINE_DUPLICATE`` (listing every claiming
            distribution) or ``ENGINE_API_MISMATCH`` (missing or unsupported
            handshake version).
    """
    try:
        loaded = load_entry_point(ENGINE_ENTRY_POINT_GROUP, name, on_duplicate="error")
    except DuplicateEntryPointError as exc:
        raise AgentCompilationError([engine_duplicate(name, exc.distributions)]) from exc
    except EntryPointNotFoundError as exc:
        raise AgentCompilationError([engine_not_found(name, exc.available)]) from exc
    provider = loaded() if isinstance(loaded, type) else loaded
    _verify_engine_api(name, provider)
    # The handshake above is the structural runtime check; the protocol is not
    # runtime-checkable by design, so the cast records the contract.
    return cast(AgentEngineProvider, provider)


def require_provider_sdk(provider: str, module: str, extra: str) -> ModuleType:
    """Import an island that needs a provider SDK, failing with the extra to install.

    The engine's reading of :func:`loom.core.plugins.optional.import_optional`:
    the same load, with the failure translated to this layer's coded issue.

    Args:
        provider: Provider identifier the error should name.
        module: Importable path of the island — the loom module that imports
            the SDK at its top.
        extra: Loom extra whose installation brings the SDK.

    Returns:
        The imported island.

    Raises:
        AgentCompilationError: With ``PROVIDER_NOT_INSTALLED`` naming the
            extra when the island cannot be imported.
    """
    try:
        return import_optional(module, extra=extra)
    except MissingExtraError as exc:
        raise AgentCompilationError([provider_not_installed(provider, extra)]) from exc


def require_provider_setting(provider: str, setting: str, value: object | None) -> None:
    """Require a provider setting to be present and non-empty.

    ``None`` and the empty string count as missing; any other value —
    including ``0`` and ``False`` — is accepted, since those can be
    legitimate settings.

    Args:
        provider: Provider identifier the error should name.
        setting: Setting name the error should name.
        value: Resolved setting value.

    Raises:
        AgentCompilationError: With ``PROVIDER_SETTING_MISSING`` naming the
            setting when the value is absent.
    """
    if value is None or value == "":
        raise AgentCompilationError([provider_setting_missing(provider, setting)])


def _verify_engine_api(name: str, provider: object) -> None:
    """Check the handshake attribute on the constructed provider, fail-closed."""
    try:
        check_api_version(provider, _ENGINE_API_REQUIREMENT)
    except ApiVersionMismatchError as exc:
        # A version that is not an integer is reported as 0: the code says
        # "cannot speak it", and there is no version to name.
        found = exc.declared if type(exc.declared) is int else 0
        raise AgentCompilationError(
            [engine_api_mismatch(name, found, sorted(SUPPORTED_ENGINE_APIS))]
        ) from exc


def engine_supported_kinds(provider: object, engine: str) -> frozenset[str]:
    """Return the capability kinds an engine can actually be trusted to serve.

    An engine advertising ``native`` without the oracle the compiler needs to
    check a grant against its model would let the grant through unchecked, so
    the kind is dropped and the artifact is refused for the right reason.

    Args:
        provider: Engine provider resolved from the entry point group.
        engine: Engine name, for the warning that names the offender.

    Returns:
        The kinds the compiler may accept.
    """
    kinds = cast("AgentEngineProvider", provider).supported_capability_kinds()
    if "native" in kinds and engine_native_tool_support(provider) is None:
        _logger.warning(
            "engine %r serves 'native' grants but supplies no native_tool_support: "
            "the kind is refused until it does.",
            engine,
        )
        return kinds - {"native"}
    return kinds


def engine_native_tool_support(provider: object) -> NativeToolSupport | None:
    """Return the oracle a provider supplies for provider-run tools.

    Read off the resolved provider with ``getattr``, the same handshake shape as
    the client factories: the compiler learns what a model binding admits
    without importing an engine.

    Args:
        provider: Engine provider resolved from the entry point group.

    Returns:
        The oracle, or ``None`` when the engine serves no ``native`` grant.

    Example::

        support = engine_native_tool_support(resolve_engine_provider("pydantic-ai"))
    """
    return cast("NativeToolSupport | None", getattr(provider, "native_tool_support", None))


def engine_prices_model(provider: object, target: InferenceTarget) -> bool:
    """Report whether an engine prices the model bound to *target* on its own.

    Read off the resolved provider with ``getattr``, like
    :func:`engine_native_tool_support`. An engine that supplies no
    ``prices_model`` oracle is taken to price nothing, so a declared spend cap
    is only trusted when a price is actually known.

    Args:
        provider: Engine provider resolved from the entry point group.
        target: Resolved model binding of the agent's role.

    Returns:
        ``True`` only when the engine says it can price the bound model.
    """
    oracle = cast(
        "Callable[[InferenceTarget], bool] | None", getattr(provider, "prices_model", None)
    )
    return oracle is not None and oracle(target)


def engine_client_factories(
    provider: object,
) -> tuple[McpClientFactory | None, A2AClientFactory | None]:
    """Return the ``(mcp, a2a)`` live-client factories a provider supplies.

    Read off the resolved provider, never imported from an engine package: the
    composition root importing ``loom.ai.engines.<engine>`` directly is what
    would make ``create_app`` fail on a deployment running a third-party engine
    without that one installed, undoing the entry-point seam the pillar exists
    for (FR-016, FR-051).

    Optional by design and read with ``getattr``, the same handshake shape as
    ``LOOM_AI_ENGINE_API``: an engine that serves neither ``mcp`` nor ``a2a``
    grants declares neither factory, and the compiler already refuses those
    grants through ``supported_capability_kinds``.

    Args:
        provider: Engine provider resolved from the entry point group.

    Returns:
        The MCP client factory and the A2A client factory, each ``None`` when
        the engine does not supply it.

    Example::

        mcp, a2a = engine_client_factories(resolve_engine_provider("pydantic-ai"))
    """
    mcp = cast("McpClientFactory | None", getattr(provider, "mcp_client_factory", None))
    a2a = cast("A2AClientFactory | None", getattr(provider, "a2a_client_factory", None))
    return mcp, a2a


def configure_engine_mcp_connect_timeout(provider: object, seconds: float) -> None:
    """Hand the deployment's published MCP handshake budget to the resolved engine.

    Read structurally with ``getattr``, the same optional shape as
    ``engine_client_factories``: an engine that declares no
    ``configure_mcp_connect_timeout`` method is silently left at its own
    default. :func:`resolve_engine_provider` constructs every provider with
    no arguments, so ``ai.startup_timeout_ms`` can only reach the engine
    after resolution, through this call (FR-051).

    Args:
        provider: Engine provider resolved from the entry point group.
        seconds: ``ai.startup_timeout_ms`` converted to seconds.
    """
    configure = getattr(provider, "configure_mcp_connect_timeout", None)
    if configure is not None:
        configure(seconds)
